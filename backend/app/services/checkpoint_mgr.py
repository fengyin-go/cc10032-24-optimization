"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力:
  - 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证
  - 向回放引擎提供「可用快照列表」，区分可读快照与已裁剪（外部路径缺失）快照
  - 回放边界选择：最新检查点被裁剪时退回到最后可用快照，或从外部归档恢复最新快照
  - 裁剪版本登记：记录被裁剪快照、替代关系，供回放验证报告追溯
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    # 回放边界两种恢复路径
    BOUNDARY_FALLBACK = "fallback_last_available"   # 退回到最后可用快照
    BOUNDARY_ARCHIVE = "restore_from_archive"       # 从外部归档恢复最新快照

    def __init__(self) -> None:
        # 归档恢复串行化，避免并发把同一个裁剪快照恢复出多份
        self._archive_lock = threading.Lock()

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self, config_name: str, operator_states: dict[str, Any],
        lineage_version: str | None = None,
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。lineage_version 记录该快照
        生成时刻对应的指标血缘版本，便于回放验证报告保留来源版本。
        """
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        snapshot = {
            "id": cp_id,
            "配置名称": config_name,
            "间隔时间": str(interval),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "3"),
            "对齐策略": config.get("对齐策略", "exactly_once"),
            "保留数量": int(config.get("保留数量", 5)),
            "外部化路径": config.get("外部化路径", "/data/checkpoints"),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": copy.deepcopy(operator_states),
            "snapshot_ts": time.time(),
            # 快照生成时刻的血缘版本；裁剪后回放仍可追溯血缘来源
            "lineage_version": lineage_version or self._current_lineage_version(),
            # 外部路径与归档状态：默认路径可读、归档可用
            "_external_missing": False,
            "_archive_available": True,
            "_restored_from_archive": False,
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(snapshot)

        # 清理旧检查点（保留最近 N 个）
        self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "interval": interval,
            "lineage_version": snapshot["lineage_version"],
        }

    # ---- 裁剪与可读判定 ----

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点，并登记被裁剪版本。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name:
                config = r
                break
        max_keep = int((config or {}).get("保留数量", 5))

        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        for s in snapshots[max_keep:]:
            if not s.get("_pruned"):
                s["status"] = "已停用"
                s["_pruned"] = True
                s["_pruned_at"] = time.time()
                self._record_pruned_snapshot(s, reason="retention_shortened")

    def mark_external_path_missing(self, checkpoint_id: str) -> dict[str, Any]:
        """标记快照的外部化路径已不存在（保留数量缩短后早期路径被清理）。

        被标记的快照仍可能保留完整状态（_archive_available=True），
        此时它「不可读但可从归档恢复」。
        """
        snapshot = self._find_snapshot(checkpoint_id)
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        snapshot["_external_missing"] = True
        # 外部路径缺失即视为当前不可读
        if snapshot.get("status") == "已启用":
            snapshot["status"] = "已停用"
            snapshot["_pruned"] = True
            snapshot.setdefault("_pruned_at", time.time())
            self._record_pruned_snapshot(snapshot, reason="external_path_missing")
        return {"ok": True, "checkpoint_id": checkpoint_id, "readable": False}

    def mark_archive_unavailable(self, checkpoint_id: str) -> dict[str, Any]:
        """标记快照无外部归档可恢复（既不可读也无法归档恢复）。"""
        snapshot = self._find_snapshot(checkpoint_id)
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        snapshot["_archive_available"] = False
        return {"ok": True, "checkpoint_id": checkpoint_id, "archive_available": False}

    def is_snapshot_readable(self, snapshot: dict[str, Any]) -> bool:
        """快照可读：存在 checkpoint_id、状态启用且外部路径未缺失。"""
        return bool(
            snapshot.get("checkpoint_id")
            and snapshot.get("status") == "已启用"
            and not snapshot.get("_external_missing")
            and not snapshot.get("_pruned")
        )

    def list_available_snapshots(
        self, config_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """向回放引擎提供可用（仍可读取）快照列表。

        只返回当前可读的快照，按快照时间倒序排列；同时附带每个快照的
        血缘版本，回放引擎据此把血缘版本随可用快照一起更新。
        """
        available = [
            r for r in store.rows(self.MODULE)
            if r.get("checkpoint_id") and self.is_snapshot_readable(r)
        ]
        if config_name:
            available = [r for r in available if r.get("配置名称") == config_name]
        available.sort(key=lambda s: s.get("snapshot_ts", 0), reverse=True)
        return [self._snapshot_summary(cp) for cp in available]

    def list_pruned_versions(self, config_name: str | None = None) -> list[dict[str, Any]]:
        """返回被裁剪版本登记，供回放验证报告记录被裁剪版本与替代关系。"""
        registry = store.pruned_checkpoint_registry()
        if config_name:
            registry = [r for r in registry if r.get("config") == config_name]
        return copy.deepcopy(registry)

    def resolve_replay_boundary(
        self,
        requested_checkpoint_id: str,
        *,
        strategy: str = BOUNDARY_FALLBACK,
        config_name: str | None = None,
    ) -> dict[str, Any]:
        """为回放任务选择一个实际可执行的恢复边界。

        - 请求快照仍可读：直接使用，无替代。
        - 请求快照被裁剪/不可读，且同配置存在较早的完整快照：
            * fallback_last_available（默认）：退回到最后可用快照。
            * restore_from_archive：若归档可用则恢复最新快照，否则退回。
        - 没有任何可用快照：返回不可恢复错误。

        无论选择哪条路径，对外只返回一个实际执行的恢复边界。
        """
        requested = self._find_snapshot(requested_checkpoint_id)
        if requested is None:
            return {
                "ok": False,
                "error": f"检查点「{requested_checkpoint_id}」不存在",
                "boundary": None,
            }

        cfg = config_name or requested.get("配置名称", "")

        # 1. 请求快照仍可读 → 原样使用
        if self.is_snapshot_readable(requested):
            return {
                "ok": True,
                "strategy": "use_requested",
                "boundary": self._snapshot_summary(requested),
                "requested_checkpoint_id": requested_checkpoint_id,
                "substituted": False,
                "pruned_versions": self.list_pruned_versions(cfg),
            }

        # 2. 请求快照不可读，先找同配置下最后可用快照
        available = self.list_available_snapshots(cfg)
        last_available = available[0] if available else None

        archive_restored = None
        if strategy == self.BOUNDARY_ARCHIVE and requested.get("_archive_available"):
            archive_restored = self.restore_from_archive(requested_checkpoint_id)

        if archive_restored is not None and archive_restored.get("ok"):
            restored = self._find_snapshot(requested_checkpoint_id)
            return {
                "ok": True,
                "strategy": self.BOUNDARY_ARCHIVE,
                "boundary": self._snapshot_summary(restored),
                "requested_checkpoint_id": requested_checkpoint_id,
                "substituted": True,
                "substitution": {
                    "from_checkpoint_id": requested_checkpoint_id,
                    "to_checkpoint_id": requested_checkpoint_id,
                    "type": "archive_restore",
                },
                "pruned_versions": self.list_pruned_versions(cfg),
            }

        # 3. 归档不可用或未选择归档 → 退回到最后可用快照
        if last_available is not None:
            return {
                "ok": True,
                "strategy": self.BOUNDARY_FALLBACK,
                "boundary": last_available,
                "requested_checkpoint_id": requested_checkpoint_id,
                "substituted": True,
                "substitution": {
                    "from_checkpoint_id": requested_checkpoint_id,
                    "to_checkpoint_id": last_available["checkpoint_id"],
                    "type": "fallback_last_available",
                },
                "pruned_versions": self.list_pruned_versions(cfg),
            }

        return {
            "ok": False,
            "error": (
                f"检查点「{requested_checkpoint_id}」不可读，且配置「{cfg}」"
                "下没有任何可恢复的可用快照"
            ),
            "boundary": None,
            "requested_checkpoint_id": requested_checkpoint_id,
            "substituted": False,
        }

    def restore_from_archive(self, checkpoint_id: str) -> dict[str, Any]:
        """从外部归档恢复被裁剪的最新快照。

        幂等：若该快照已恢复过，直接返回已有恢复结果而不重复产生边界。
        """
        with self._archive_lock:
            snapshot = self._find_snapshot(checkpoint_id)
            if snapshot is None:
                return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
            if self.is_snapshot_readable(snapshot):
                return {
                    "ok": True,
                    "checkpoint_id": checkpoint_id,
                    "already_restored": True,
                    "boundary": self._snapshot_summary(snapshot),
                }
            if not snapshot.get("_archive_available"):
                return {
                    "ok": False,
                    "error": f"检查点「{checkpoint_id}」无可用外部归档",
                }
            snapshot["_external_missing"] = False
            snapshot["_pruned"] = False
            snapshot["status"] = "已启用"
            snapshot["_restored_from_archive"] = True
            snapshot["_restored_at"] = time.time()
            self._mark_registry_restored(checkpoint_id)
            return {
                "ok": True,
                "checkpoint_id": checkpoint_id,
                "already_restored": False,
                "boundary": self._snapshot_summary(snapshot),
            }

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。"""
        snapshot = None
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == checkpoint_id:
                snapshot = r
                break
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        if not self.is_snapshot_readable(snapshot):
            return {
                "ok": False,
                "error": f"检查点「{checkpoint_id}」已裁剪或外部路径不可读，"
                "请先通过 resolve_replay_boundary 选择可用回放边界",
            }

        states = snapshot.get("operator_states", {})
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "states": states,
            "validation": validation,
            "lineage_version": snapshot.get("lineage_version"),
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = [
            r for r in store.rows(self.MODULE)
            if r.get("配置名称") == config_name and r.get("checkpoint_id")
        ]
        snapshots.sort(key=lambda s: s.get("snapshot_ts", 0))

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = snapshots[0].get("operator_states", {})
        inconsistencies = []
        for i, s in enumerate(snapshots[1:], 1):
            current = s.get("operator_states", {})
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点（仅当前可读快照）。"""
        return self.list_available_snapshots()

    # ---- 内部辅助 ----

    def _find_snapshot(self, checkpoint_id: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("checkpoint_id") == str(checkpoint_id) or r.get("id") == str(checkpoint_id):
                return r
        return None

    def _snapshot_summary(self, cp: dict[str, Any]) -> dict[str, Any]:
        states = cp.get("operator_states", {})
        return {
            "checkpoint_id": cp.get("checkpoint_id"),
            "config": cp.get("配置名称", ""),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "snapshot_ts": cp.get("snapshot_ts", 0),
            "external_path": cp.get("外部化路径", ""),
            "external_missing": bool(cp.get("_external_missing")),
            "archive_available": bool(cp.get("_archive_available", True)),
            "lineage_version": cp.get("lineage_version"),
            "restored_from_archive": bool(cp.get("_restored_from_archive")),
        }

    def _record_pruned_snapshot(
        self, snapshot: dict[str, Any], reason: str,
    ) -> None:
        """把裁剪事件登记到仓库级裁剪版本表。"""
        cp_id = snapshot.get("checkpoint_id")
        registry = store.pruned_checkpoint_registry()
        for entry in registry:
            if entry.get("checkpoint_id") == cp_id:
                entry["pruned_at"] = time.time()
                entry["reason"] = reason
                return
        registry.append({
            "checkpoint_id": cp_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": snapshot.get("snapshot_ts", 0),
            "lineage_version": snapshot.get("lineage_version"),
            "external_path": snapshot.get("外部化路径", ""),
            "reason": reason,
            "pruned_at": snapshot.get("_pruned_at", time.time()),
            "restored_checkpoint_id": None,
        })

    def _mark_registry_restored(self, checkpoint_id: str) -> None:
        for entry in store.pruned_checkpoint_registry():
            if entry.get("checkpoint_id") == checkpoint_id:
                entry["restored_checkpoint_id"] = checkpoint_id
                entry["restored_at"] = time.time()

    @staticmethod
    def _current_lineage_version() -> str:
        """读取血缘模块当前最新版本号，作为快照血缘来源版本。"""
        versions: list[int] = []
        for row in store.rows("metric_lineage"):
            raw = str(row.get("版本号", "") or "")
            if raw.startswith("v") and raw[1:].isdigit():
                versions.append(int(raw[1:]))
        return f"v{max(versions)}" if versions else "v0"

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["total_snapshots"] = sum(1 for r in rows if r.get("checkpoint_id"))
        base["available_snapshots"] = sum(
            1 for r in rows if r.get("checkpoint_id") and self.is_snapshot_readable(r)
        )
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        base["archive_restored_snapshots"] = sum(
            1 for r in rows if r.get("_restored_from_archive")
        )
        return base
