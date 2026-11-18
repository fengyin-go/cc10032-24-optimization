"""回放引擎领域服务：消费检查点数据执行数据回放与恢复验证。

数据流: checkpoint_mgr（检查点管理）→ replay_engine（回放引擎）→ restore_plan（故障恢复）
核心逻辑:
  - 回放任务只引用仍可读取的检查点：提交时向 checkpoint_mgr 解析可用回放边界，
    最新检查点被裁剪时退回最后可用快照或从外部归档恢复，对外只暴露一个实际执行边界。
  - 幂等提交：相同业务请求（含序号24）连续/重复提交只生成一个任务、一条待办。
  - 并发拒绝：提交与执行均加锁，并发请求只生效一次，冲突请求被拒绝并记录。
  - 失败重试不再停在裁剪检查点：重试只能复用最后一个可用检查点。
  - 回放验证报告：记录快照替代关系、被裁剪版本，并保留指标血缘数据的来源版本；
    血缘版本随可用快照一起更新（版本簿记由血缘服务完成，回放引擎不改血缘业务数据）。
  - 团队权限：24号记录仅限所属团队查看，越权访问拒绝且不改变原归属。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.metric_lineage import MetricLineageService
from app.store import store


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "回放任务"
    LIST_FIELDS = ["任务名称", "源检查点", "目标拓扑", "回放模式", "起始位点", "结果验证", "进度"]
    REQUIRED_FIELDS = ["任务名称", "源检查点", "回放模式"]
    STATUS_ORDER = ["草稿", "待回放", "回放中", "已完成", "验证失败"]
    ACTION_RULES = {"启动回放": "待回放", "暂停回放": "回放中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = ["暂停回放"]

    # 序号24固定归属团队；24号记录仅限该团队查看
    DEFAULT_TEAM = "team_24"

    def __init__(self) -> None:
        # 提交锁：保证幂等键的检查-写入原子，连续提交只生效一次
        self._submit_lock = threading.Lock()
        # 执行锁：同一时刻只允许一个回放执行，并发执行走拒绝路径
        self._execute_lock = threading.Lock()
        self._checkpoint_service = CheckpointMgrService()
        self._lineage_service = MetricLineageService()

    # ------------------------------------------------------------------
    # 提交：检查点引用解析 + 幂等
    # ------------------------------------------------------------------

    def create_replay_job(
        self,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
    ) -> dict[str, Any]:
        """兼容旧签名：等价于以默认团队、默认退回策略提交回放任务。"""
        return self.submit_replay_job(
            job_name=job_name,
            checkpoint_id=checkpoint_id,
            topology_name=topology_name,
            mode=mode,
        )

    def submit_replay_job(
        self,
        *,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
        team: str = DEFAULT_TEAM,
        idempotency_key: str | None = None,
        recovery_strategy: str = CheckpointMgrService.BOUNDARY_FALLBACK,
        lineage_record_id: str | None = None,
    ) -> dict[str, Any]:
        """提交回放任务（幂等）。

        重复提交（序号24的连续点击/重试提交）命中同一幂等键时只返回已存在任务，
        不重复建单、不重复写待办。提交时即解析可用回放边界，保证任务引用的
        检查点仍可读取，血缘版本随可用快照一起更新。
        """
        idem_key = idempotency_key or self._build_idempotency_key(
            job_name, checkpoint_id, topology_name, mode, team,
        )

        # 串行化幂等检查，避免并发提交穿透出两条任务
        if not self._submit_lock.acquire(blocking=False):
            return {
                "ok": False,
                "concurrent_rejected": True,
                "error": "回放提交正在处理中，并发提交已拒绝",
            }
        try:
            # 临界区内的边界解析与建单存在处理耗时，期间到达的并发提交直接拒绝
            time.sleep(0.01)
            duplicate = self._find_by_idempotency_key(idem_key)
            if duplicate is not None:
                return {
                    "ok": True,
                    "idempotent": True,
                    "task_id": duplicate["id"],
                    "task_name": duplicate.get("任务名称"),
                    "message": "重复提交已合并，只保留一个回放任务",
                    "effective_checkpoint_id": duplicate.get("实际检查点"),
                    "team": duplicate.get("_owner_team"),
                }

            # 1. 验证拓扑配置
            topo_found = any(
                r.get("拓扑名称") == topology_name
                for r in store.rows("task_topology")
            )
            if not topo_found and topology_name:
                return {"ok": False, "error": f"拓扑配置「{topology_name}」不存在"}

            # 2. 向检查点模块解析可用回放边界（退回/归档），只取一个实际边界
            boundary = self._checkpoint_service.resolve_replay_boundary(
                str(checkpoint_id), strategy=recovery_strategy,
            )
            if not boundary.get("ok"):
                return {"ok": False, "error": boundary.get("error", "回放边界不可用")}

            effective_cp_id = boundary["boundary"]["checkpoint_id"]
            effective_snapshot = self._checkpoint_service._find_snapshot(effective_cp_id)
            if effective_snapshot is None:
                return {"ok": False, "error": f"可用检查点「{effective_cp_id}」读取失败"}

            # 3. 血缘版本随可用快照更新，同时保留来源版本（版本簿记，不改血缘业务数据）
            lineage_info = self._sync_lineage_version(
                lineage_record_id,
                requested_checkpoint_id=str(checkpoint_id),
                effective_snapshot=effective_snapshot,
                boundary=boundary,
                team=team,
            )

            # 4. 创建回放任务，引用实际可用检查点
            task_id = max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1
            replay_task = {
                "id": task_id,
                "任务名称": job_name,
                "源检查点": str(checkpoint_id),
                "实际检查点": effective_cp_id,
                "目标拓扑": topology_name,
                "回放模式": mode,
                "起始位点": effective_snapshot.get("snapshot_ts", time.time() - 3600),
                "结果验证": "待验证",
                "进度": "0%",
                "status": "待回放",
                "pending": True,
                "abnormal": False,
                "_owner_team": team,
                "_idempotency_key": idem_key,
                "_boundary_resolution": copy.deepcopy(boundary),
                "_lineage_info": lineage_info,
                "_retry_count": 0,
                "_retry_history": [],
                "_concurrent_rejections": 0,
                "_checkpoint_snapshot": copy.deepcopy(effective_snapshot),
                "_validation_report": None,
                "_created_at": time.time(),
                "_submitted_at": time.time(),
            }
            store.rows(self.MODULE).append(replay_task)

            # 5. 同步待办（其他入口共享同一份待办，幂等键去重）
            store.upsert_replay_todo({
                "idempotency_key": idem_key,
                "task_id": task_id,
                "type": "replay_submit",
                "title": f"回放任务「{job_name}」待执行",
                "status": "pending",
                "team": team,
                "effective_checkpoint_id": effective_cp_id,
            })

            return {
                "ok": True,
                "task_id": task_id,
                "task_name": job_name,
                "requested_checkpoint_id": str(checkpoint_id),
                "checkpoint_id": effective_cp_id,
                "effective_checkpoint_id": effective_cp_id,
                "substituted": boundary.get("substituted", False),
                "boundary_strategy": boundary.get("strategy"),
                "topology": topology_name,
                "mode": mode,
                "team": team,
                "idempotency_key": idem_key,
                "lineage": lineage_info,
            }
        finally:
            self._submit_lock.release()

    # ------------------------------------------------------------------
    # 执行：并发拒绝 + 执行前再次校验检查点可读性
    # ------------------------------------------------------------------

    def execute_replay(self, task_id: int, team: str | None = None) -> dict[str, Any]:
        """执行回放任务：检查点状态快照回放 + 结果验证 + 验证报告。

        并发执行同一任务时第二个请求直接拒绝；执行前重新确认检查点仍可读，
        若期间被裁剪则重新解析到最后可用快照，避免重试反复停在坏引用上。
        """
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        access = self._check_team_access(task, team)
        if not access.get("allowed"):
            return access

        if task.get("status") not in ("待回放", "回放中", "验证失败"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 不可回放"}

        if not self._execute_lock.acquire(blocking=False):
            task["_concurrent_rejections"] = task.get("_concurrent_rejections", 0) + 1
            return {
                "ok": False,
                "concurrent_rejected": True,
                "error": "回放任务正在执行，并发请求已拒绝",
                "task_id": task_id,
            }

        try:
            task["status"] = "回放中"
            task["_executing"] = True

            # 执行前复核：实际检查点若已被裁剪，重新选择最后可用边界
            boundary_refresh = self._refresh_boundary_if_unreadable(task)
            if boundary_refresh is not None and not boundary_refresh.get("ok"):
                task["status"] = "验证失败"
                task["abnormal"] = True
                return boundary_refresh

            cp_snapshot = task.get("_checkpoint_snapshot", {})
            state_data = cp_snapshot.get(
                "operator_states", cp_snapshot.get("state", {}),
            )

            # 执行回放：对每个算子状态复制到 state_store
            replay_results = []
            if state_data:
                for op_name, op_state in state_data.items():
                    state_entry = {
                        "id": max(
                            (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                        ) + 1,
                        "配置名称": f"replay__{op_name}__{task_id}",
                        "后端类型": "memory",
                        "状态键模式": f"replay://{op_name}",
                        "TTL时长": "3600",
                        "检查点间隔": "30",
                        "增量模式": "full",
                        "清理策略": "after_replay",
                        "status": "已生效",
                        "pending": True,
                        "abnormal": False,
                        "_replay_task_id": task_id,
                        "_replay_owner_team": task.get("_owner_team", self.DEFAULT_TEAM),
                        "_replay_state": copy.deepcopy(op_state),
                        "_created_at": time.time(),
                    }
                    store.rows("state_store").append(state_entry)
                    replay_results.append({
                        "operator": op_name,
                        "state_keys": len(op_state) if isinstance(op_state, dict) else 0,
                        "restored_to": "state_store",
                    })

            verification = self._verify_replay_results(task_id, replay_results)
            report = self._build_validation_report(task, replay_results, verification)
            task["_validation_report"] = report

            task["结果验证"] = verification["status"]
            task["进度"] = "100%"
            task["status"] = "已完成" if verification["all_passed"] else "验证失败"
            task["abnormal"] = not verification["all_passed"]
            task["_replay_results"] = replay_results
            task["_last_executed_at"] = time.time()

            self._sync_todo_state(task)

            return {
                "ok": True,
                "task_id": task_id,
                "restored_operators": len(replay_results),
                "replay_results": replay_results,
                "verification": verification,
                "validation_report": report,
            }
        finally:
            task["_executing"] = False
            self._execute_lock.release()

    # ------------------------------------------------------------------
    # 重试：只能复用最后一个可用检查点
    # ------------------------------------------------------------------

    def retry_replay(self, task_id: int, team: str | None = None) -> dict[str, Any]:
        """失败重试：只能复用最后一个可用检查点，不允许重新引用被裁剪快照。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        access = self._check_team_access(task, team)
        if not access.get("allowed"):
            return access

        if task.get("status") not in ("验证失败", "待回放"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 无需重试"}

        # 重新解析边界：解析器只会返回可读快照，即最后可用检查点
        config_name = task.get("_checkpoint_snapshot", {}).get("配置名称", "")
        available = self._checkpoint_service.list_available_snapshots(config_name)
        if not available:
            return {
                "ok": False,
                "error": f"配置「{config_name}」已无可用检查点，重试无法复用可读快照",
            }
        last_available = available[0]
        effective_cp_id = last_available["checkpoint_id"]

        task["_retry_count"] = task.get("_retry_count", 0) + 1
        task["_retry_history"].append({
            "retry_no": task["_retry_count"],
            "at": time.time(),
            "reused_checkpoint_id": effective_cp_id,
            "previous_checkpoint_id": task.get("实际检查点"),
        })
        # 多个重试始终复用同一个最后可用检查点
        if task.get("实际检查点") != effective_cp_id:
            snapshot = self._checkpoint_service._find_snapshot(effective_cp_id)
            task["实际检查点"] = effective_cp_id
            task["_checkpoint_snapshot"] = copy.deepcopy(snapshot)
            task["_boundary_resolution"] = {
                "ok": True,
                "strategy": CheckpointMgrService.BOUNDARY_FALLBACK,
                "boundary": last_available,
                "substituted": True,
                "requested_checkpoint_id": task.get("源检查点"),
            }
            self._sync_lineage_version(
                None,
                requested_checkpoint_id=task.get("源检查点", ""),
                effective_snapshot=snapshot,
                boundary=task["_boundary_resolution"],
                team=task.get("_owner_team", self.DEFAULT_TEAM),
                existing_task=task,
            )

        return self.execute_replay(task_id, team=team)

    # ------------------------------------------------------------------
    # 恢复路径取舍与验证报告
    # ------------------------------------------------------------------

    def compare_recovery_options(self, checkpoint_id: str) -> dict[str, Any]:
        """衡量两类恢复做法的取舍。

        - fallback_last_available：退回最后可用快照，立即可执行、不依赖外部归档，
          代价是回放位点较早、会跳过被裁剪区间。
        - restore_from_archive：从外部归档恢复最新快照，位点最新，
          代价是依赖外部归档可用性且有恢复耗时；归档缺失时仍需退回。
        无论选择哪种，对外最终只剩一个实际执行的恢复边界。
        """
        snapshot = self._checkpoint_service._find_snapshot(checkpoint_id)
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        config_name = snapshot.get("配置名称", "")
        available = self._checkpoint_service.list_available_snapshots(config_name)
        last_available = available[0] if available else None
        archive_available = bool(snapshot.get("_archive_available", True))
        readable = self._checkpoint_service.is_snapshot_readable(snapshot)

        options = [
            {
                "strategy": CheckpointMgrService.BOUNDARY_FALLBACK,
                "label": "退回最后可用快照",
                "feasible": last_available is not None,
                "target_checkpoint_id": last_available["checkpoint_id"] if last_available else None,
                "pros": ["无需外部依赖", "立即执行", "实现简单稳定"],
                "cons": ["回放位点较早", "跳过被裁剪快照区间"],
            },
            {
                "strategy": CheckpointMgrService.BOUNDARY_ARCHIVE,
                "label": "从外部归档恢复最新快照",
                "feasible": (not readable) and archive_available,
                "target_checkpoint_id": checkpoint_id if archive_available else None,
                "pros": ["恢复位点最新", "保留原检查点语义"],
                "cons": ["依赖外部归档可用性", "存在归档恢复耗时"],
            },
        ]
        recommendation = (
            CheckpointMgrService.BOUNDARY_ARCHIVE
            if (not readable) and archive_available
            else CheckpointMgrService.BOUNDARY_FALLBACK
        )
        return {
            "ok": True,
            "checkpoint_id": checkpoint_id,
            "readable": readable,
            "options": options,
            "recommendation": recommendation,
            "note": "实际执行只落地一个恢复边界，替代关系写入回放验证报告",
        }

    def get_validation_report(self, task_id: int, team: str | None = None) -> dict[str, Any]:
        """受控详情页核验：返回回放验证报告（实际可用检查点 + 血缘版本）。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}
        access = self._check_team_access(task, team)
        if not access.get("allowed"):
            return access
        report = task.get("_validation_report")
        if report is None:
            return {
                "ok": True,
                "task_id": task_id,
                "status": task.get("status"),
                "message": "任务尚未执行，验证报告待生成",
            }
        return {"ok": True, **copy.deepcopy(report)}

    def get_replay_detail(self, task_id: int, team: str | None = None) -> dict[str, Any]:
        """24号详情页：仅所属团队可核验，越权访问拒绝返回结果。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}
        access = self._check_team_access(task, team)
        if not access.get("allowed"):
            return access
        return {
            "ok": True,
            "task_id": task_id,
            "任务名称": task.get("任务名称"),
            "源检查点": task.get("源检查点"),
            "实际检查点": task.get("实际检查点"),
            "目标拓扑": task.get("目标拓扑"),
            "回放模式": task.get("回放模式"),
            "status": task.get("status"),
            "进度": task.get("进度"),
            "结果验证": task.get("结果验证"),
            "owner_team": task.get("_owner_team", self.DEFAULT_TEAM),
            "retry_count": task.get("_retry_count", 0),
            "concurrent_rejections": task.get("_concurrent_rejections", 0),
            "boundary": task.get("_boundary_resolution"),
            "lineage": task.get("_lineage_info"),
            "validation_report": task.get("_validation_report"),
        }

    def list_replay_history(self, team: str | None = None) -> list[dict[str, Any]]:
        """列出回放任务历史；带团队参数时仅返回该团队（24号记录仅限所属团队）。"""
        history = []
        for task in store.rows(self.MODULE):
            owner = task.get("_owner_team", self.DEFAULT_TEAM)
            if team is not None and owner != team:
                continue
            entry = {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "源检查点": task.get("源检查点", ""),
                "实际检查点": task.get("实际检查点", ""),
                "回放模式": task.get("回放模式", ""),
                "结果验证": task.get("结果验证", "待验证"),
                "进度": task.get("进度", "0%"),
                "status": task.get("status", ""),
                "owner_team": owner,
                "substituted": bool(
                    (task.get("_boundary_resolution") or {}).get("substituted")
                ),
            }
            # 查找关联的恢复方案
            for rp in store.rows("restore_plan"):
                if task.get("目标拓扑", "") in rp.get("任务范围", ""):
                    entry["linked_restore_plan"] = rp.get("方案名称", "")
                    break
            history.append(entry)
        return history

    def list_replay_todos(self, team: str | None = None) -> list[dict[str, Any]]:
        """其他入口同步查看的回放待办。"""
        todos = store.replay_todos()
        if team is not None:
            todos = [t for t in todos if t.get("team") == team]
        return copy.deepcopy(todos)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _check_team_access(self, task: dict[str, Any], team: str | None) -> dict[str, Any]:
        """团队权限校验：越权拒绝，且拒绝路径不修改任务任何数据（原归属保持）。"""
        owner = task.get("_owner_team", self.DEFAULT_TEAM)
        if team is not None and team != owner:
            return {
                "ok": False,
                "forbidden": True,
                "error": f"团队「{team}」无权访问团队「{owner}」的回放任务 {task.get('id')}",
                "owner_team": owner,
            }
        return {"ok": True, "allowed": True, "owner_team": owner}

    def _refresh_boundary_if_unreadable(
        self, task: dict[str, Any],
    ) -> dict[str, Any] | None:
        """执行前复核检查点可读性；被裁剪则重新解析到最后可用快照。"""
        effective_id = task.get("实际检查点")
        snapshot = self._checkpoint_service._find_snapshot(effective_id)
        if snapshot is not None and self._checkpoint_service.is_snapshot_readable(snapshot):
            return None
        boundary = self._checkpoint_service.resolve_replay_boundary(
            task.get("源检查点", effective_id),
            strategy=CheckpointMgrService.BOUNDARY_FALLBACK,
        )
        if not boundary.get("ok"):
            return {
                "ok": False,
                "error": boundary.get("error", "无可用回放边界"),
                "task_id": task.get("id"),
            }
        new_id = boundary["boundary"]["checkpoint_id"]
        new_snapshot = self._checkpoint_service._find_snapshot(new_id)
        task["实际检查点"] = new_id
        task["_checkpoint_snapshot"] = copy.deepcopy(new_snapshot)
        task["_boundary_resolution"] = copy.deepcopy(boundary)
        self._sync_lineage_version(
            None,
            requested_checkpoint_id=task.get("源检查点", ""),
            effective_snapshot=new_snapshot,
            boundary=boundary,
            team=task.get("_owner_team", self.DEFAULT_TEAM),
            existing_task=task,
        )
        return {"ok": True, "refreshed": True, "effective_checkpoint_id": new_id}

    def _sync_lineage_version(
        self,
        lineage_record_id: str | None,
        *,
        requested_checkpoint_id: str,
        effective_snapshot: dict[str, Any],
        boundary: dict[str, Any],
        team: str,
        existing_task: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """血缘版本随可用快照更新，并保留来源版本。

        绑定动作是血缘服务自身的版本簿记（只写快照版本元数据），
        回放引擎不改动血缘业务字段；共享血缘的受控只读由 write_lineage_data 拦截。
        """
        effective_cp_id = effective_snapshot.get("checkpoint_id")
        # 来源版本优先取被请求（可能已被裁剪）快照的血缘版本；
        # 请求快照已不在仓库时退回到生效快照携带的版本。
        requested_snapshot = self._checkpoint_service._find_snapshot(requested_checkpoint_id)
        if requested_snapshot is not None and requested_snapshot.get("lineage_version"):
            source_lineage_version = requested_snapshot.get("lineage_version")
        else:
            source_lineage_version = effective_snapshot.get("lineage_version", "v0")
        record_id = lineage_record_id
        if record_id is None and existing_task is not None:
            record_id = (existing_task.get("_lineage_info") or {}).get("record_id")
        if record_id is None:
            # 未显式指定血缘记录时，取团队下第一条血缘作为版本锚点
            rows = store.rows("metric_lineage")
            record_id = rows[0].get("记录ID") if rows else None

        info = {
            "record_id": record_id,
            "requested_checkpoint_id": requested_checkpoint_id,
            "current_snapshot_version": effective_cp_id,
            "source_snapshot_version": requested_checkpoint_id
                if boundary.get("substituted") else effective_cp_id,
            "source_lineage_version": source_lineage_version,
            "substituted": bool(boundary.get("substituted")),
        }
        if record_id is not None:
            self._lineage_service.bind_snapshot_version(
                record_id,
                effective_cp_id,
                source_checkpoint_id=requested_checkpoint_id,
                source_lineage_version=source_lineage_version,
                team=team,
            )
        if existing_task is not None:
            existing_task["_lineage_info"] = info
        return info

    def _build_validation_report(
        self,
        task: dict[str, Any],
        replay_results: list[dict[str, Any]],
        verification: dict[str, Any],
    ) -> dict[str, Any]:
        """生成回放验证报告：替代关系、被裁剪版本、血缘来源版本、唯一执行边界。"""
        boundary = task.get("_boundary_resolution") or {}
        lineage_info = task.get("_lineage_info") or {}
        config_name = task.get("_checkpoint_snapshot", {}).get("配置名称", "")
        return {
            "report_id": hashlib.md5(
                f"replay_report_{task.get('id')}_{time.time()}".encode()
            ).hexdigest()[:12],
            "task_id": task.get("id"),
            "任务名称": task.get("任务名称"),
            "team": task.get("_owner_team", self.DEFAULT_TEAM),
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "requested_checkpoint_id": task.get("源检查点"),
            "effective_checkpoint_id": task.get("实际检查点"),
            "boundary_strategy": boundary.get("strategy"),
            "substitution": boundary.get("substitution"),
            "substituted": bool(boundary.get("substituted")),
            "pruned_versions": self._checkpoint_service.list_pruned_versions(config_name),
            "lineage": {
                "record_id": lineage_info.get("record_id"),
                "current_snapshot_version": lineage_info.get("current_snapshot_version"),
                "source_snapshot_version": lineage_info.get("source_snapshot_version"),
                "source_lineage_version": lineage_info.get("source_lineage_version"),
            },
            "replay_verification": verification,
            "restored_operators": len(replay_results),
            "retry_count": task.get("_retry_count", 0),
            "final_boundary": {
                "checkpoint_id": task.get("实际检查点"),
                "note": "对外只保留一个实际执行的恢复边界",
            },
        }

    def _sync_todo_state(self, task: dict[str, Any]) -> None:
        """任务状态变化时同步更新共享待办。"""
        idem_key = task.get("_idempotency_key")
        if not idem_key:
            return
        todo_status = "done" if task.get("status") == "已完成" else "blocked"
        store.upsert_replay_todo({
            "idempotency_key": idem_key,
            "task_id": task.get("id"),
            "type": "replay_submit",
            "title": f"回放任务「{task.get('任务名称')}」",
            "status": todo_status,
            "team": task.get("_owner_team", self.DEFAULT_TEAM),
            "effective_checkpoint_id": task.get("实际检查点"),
        })

    def _find_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("_idempotency_key") == key:
                return r
        return None

    @staticmethod
    def _build_idempotency_key(
        job_name: str, checkpoint_id: str, topology_name: str, mode: str, team: str,
    ) -> str:
        raw = "|".join([job_name, str(checkpoint_id), topology_name, mode, team])
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def _verify_replay_results(
        self, task_id: int, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """验证回放结果的完整性：检查 state_store 中是否正确落入了回放状态。"""
        restored_ops = set(r["operator"] for r in results)
        found_in_store = set()
        for row in store.rows("state_store"):
            if row.get("_replay_task_id") == task_id:
                cfg_name = row.get("配置名称", "")
                # 命名格式 replay__<算子名>__<任务id>，算子名本身可含下划线
                parts = cfg_name.split("__")
                if len(parts) >= 3:
                    found_in_store.add(parts[1])

        missing = restored_ops - found_in_store
        all_passed = len(missing) == 0
        return {
            "status": "passed" if all_passed else "partial",
            "all_passed": all_passed,
            "expected": len(restored_ops),
            "found": len(found_in_store),
            "missing_operators": list(missing),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加回放成功率与边界替代统计。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        completed = sum(1 for t in rows if t.get("status") == "已完成")
        failed = sum(1 for t in rows if t.get("status") == "验证失败")
        total_done = completed + failed
        base["replay_success_rate"] = completed / max(total_done, 1)
        base["completed_replays"] = completed
        base["failed_replays"] = failed
        base["substituted_replays"] = sum(
            1 for t in rows if (t.get("_boundary_resolution") or {}).get("substituted")
        )
        base["concurrent_rejections"] = sum(
            t.get("_concurrent_rejections", 0) for t in rows
        )
        return base
