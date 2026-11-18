"""序号24：回放检查点引用、可用边界选择与回放验证报告的全链路测试。

覆盖需求:
  - 保留数量缩短后最新检查点被裁剪、较早快照仍完整时的退回与归档恢复
  - 回放任务只引用可读检查点；血缘版本随可用快照更新并保留来源版本
  - 重复/连续提交幂等（序号24只记一次）、并发提交与并发执行拒绝
  - 失败重试只复用最后一个可用检查点
  - 验证报告记录快照替代关系、被裁剪版本、唯一实际执行边界与血缘来源版本
  - 两种权限范围的血缘版本视图、团队越权拒绝、共享血缘受控只读
  - 待办同步与两类恢复做法取舍
"""
from __future__ import annotations

import threading
import time

import pytest

from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.metric_lineage import MetricLineageService
from app.services.replay_engine import ReplayEngineService
from app.store import store


@pytest.fixture
def services():
    cp = CheckpointMgrService()
    replay = ReplayEngineService()
    lineage = MetricLineageService()
    return cp, replay, lineage


@pytest.fixture
def enabled_config(services):
    """准备一个已启用的检查点配置，返回 (service, config_name)。"""
    cp, _, _ = services
    config_name = f"cp_cfg_{time.time_ns()}"
    store.rows("checkpoint_mgr").append({
        "id": 900000 + int(time.time_ns()) % 100000,
        "配置名称": config_name,
        "间隔时间": "30",
        "超时时长": "60",
        "最大并发": "3",
        "对齐策略": "exactly_once",
        "保留数量": 5,
        "外部化路径": "/data/checkpoints",
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    })
    return cp, config_name


def _topology_name() -> str:
    return "任务拓扑样例1"


def _make_snapshots(cp: CheckpointMgrService, config_name: str, count: int):
    ids = []
    for i in range(count):
        result = cp.create_checkpoint(
            config_name,
            {f"op_{i}": {"k": i, "offset": 100 + i}},
            lineage_version=f"v{i}",
        )
        assert result["ok"]
        ids.append(result["checkpoint_id"])
        time.sleep(0.005)
    return ids


class TestCheckpointBoundaryResolution:
    """最新检查点被裁剪但较早快照仍完整时的边界选择。"""

    def test_fallback_to_last_available_snapshot(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]

        # 保留数量缩短后最新快照外部路径被裁剪，但较早快照仍完整
        cp.mark_external_path_missing(latest)
        assert not cp.is_snapshot_readable(cp._find_snapshot(latest))
        assert cp.is_snapshot_readable(cp._find_snapshot(older))

        boundary = cp.resolve_replay_boundary(latest)
        assert boundary["ok"]
        assert boundary["substituted"] is True
        assert boundary["strategy"] == CheckpointMgrService.BOUNDARY_FALLBACK
        # 对外只有一个实际执行边界，即最后可用快照
        assert boundary["boundary"]["checkpoint_id"] == older
        assert boundary["substitution"]["to_checkpoint_id"] == older

    def test_restore_latest_from_archive(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest = ids[-1]
        cp.mark_external_path_missing(latest)

        result = cp.resolve_replay_boundary(
            latest, strategy=CheckpointMgrService.BOUNDARY_ARCHIVE,
        )
        assert result["ok"]
        assert result["strategy"] == CheckpointMgrService.BOUNDARY_ARCHIVE
        # 归档恢复后仍以最新快照作为唯一执行边界
        assert result["boundary"]["checkpoint_id"] == latest
        assert cp.is_snapshot_readable(cp._find_snapshot(latest))

    def test_archive_unavailable_then_fallback(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]
        cp.mark_external_path_missing(latest)
        cp.mark_archive_unavailable(latest)

        result = cp.resolve_replay_boundary(
            latest, strategy=CheckpointMgrService.BOUNDARY_ARCHIVE,
        )
        assert result["ok"]
        # 归档不可用时自动退回最后可用快照，仍只保留一个边界
        assert result["strategy"] == CheckpointMgrService.BOUNDARY_FALLBACK
        assert result["boundary"]["checkpoint_id"] == older

    def test_no_available_boundary_returns_error(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        for cp_id in ids:
            cp.mark_external_path_missing(cp_id)
            cp.mark_archive_unavailable(cp_id)
        result = cp.resolve_replay_boundary(ids[-1])
        assert not result["ok"]
        assert result["boundary"] is None

    def test_readable_checkpoint_used_directly(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        result = cp.resolve_replay_boundary(ids[-1])
        assert result["ok"]
        assert result["substituted"] is False
        assert result["boundary"]["checkpoint_id"] == ids[-1]

    def test_pruned_versions_recorded(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        cp.mark_external_path_missing(ids[-1])
        pruned = cp.list_pruned_versions(config_name)
        pruned_ids = {p["checkpoint_id"] for p in pruned}
        assert ids[-1] in pruned_ids
        entry = next(p for p in pruned if p["checkpoint_id"] == ids[-1])
        assert entry["lineage_version"] == "v2"

    def test_retention_shortened_prunes_external_paths(
        self, services, enabled_config,
    ):
        """同一配置保留数量缩短后，早期快照被裁剪并对回放不可读。"""
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 4)
        # 保留数量从 5 缩短到 2，再次创建快照触发裁剪
        config = next(
            r for r in store.rows("checkpoint_mgr")
            if r.get("配置名称") == config_name and not r.get("checkpoint_id")
        )
        config["保留数量"] = 2
        cp.create_checkpoint(config_name, {"op_new": {"k": 1}})
        available = cp.list_available_snapshots(config_name)
        available_ids = {s["checkpoint_id"] for s in available}
        # 只剩保留数量内的快照可读，早期快照已被裁剪
        assert len(available_ids) <= 2
        assert ids[0] not in available_ids
        # 被裁剪快照登记在案且不可读
        pruned_ids = {p["checkpoint_id"] for p in cp.list_pruned_versions(config_name)}
        assert ids[0] in pruned_ids
        # restore_state 对不可读快照直接拒绝，避免回放停在坏引用上
        denied = cp.restore_state(ids[0])
        assert not denied["ok"]


class TestReplayJobSubmission:
    """回放任务必须引用可读检查点，血缘版本随快照更新。"""

    def test_job_references_effective_readable_checkpoint(self, services, enabled_config):
        cp, replay, lineage = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]
        cp.mark_external_path_missing(latest)

        result = replay.submit_replay_job(
            job_name="replay_24",
            checkpoint_id=latest,
            topology_name=_topology_name(),
            idempotency_key="seq24-key-1",
        )
        assert result["ok"]
        assert result["substituted"] is True
        assert result["effective_checkpoint_id"] == older
        task = store.find("replay_engine", result["task_id"])
        # 源检查点保留原始引用，实际检查点指向可读快照
        assert task["源检查点"] == latest
        assert task["实际检查点"] == older

    def test_lineage_version_updates_with_available_snapshot(
        self, services, enabled_config,
    ):
        cp, replay, lineage = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]
        cp.mark_external_path_missing(latest)

        result = replay.submit_replay_job(
            job_name="replay_lineage",
            checkpoint_id=latest,
            topology_name=_topology_name(),
            idempotency_key="seq24-lineage-1",
        )
        assert result["ok"]
        info = result["lineage"]
        # 当前快照版本指向可用快照，来源版本保留被裁剪快照的血缘版本
        assert info["current_snapshot_version"] == older
        assert info["source_lineage_version"] == "v2"
        assert info["source_snapshot_version"] == latest

    def test_duplicate_submission_idempotent(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)

        first = replay.submit_replay_job(
            job_name="replay_dup",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            idempotency_key="seq24-dup",
        )
        second = replay.submit_replay_job(
            job_name="replay_dup",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            idempotency_key="seq24-dup",
        )
        assert first["ok"] and second["ok"]
        assert second.get("idempotent") is True
        assert first["task_id"] == second["task_id"]
        # 序号24的重复提交只记一次：只有一个任务、一条待办
        matching = [
            t for t in store.rows("replay_engine")
            if t.get("_idempotency_key") == "seq24-dup"
        ]
        assert len(matching) == 1
        todos = [t for t in store.replay_todos() if t["idempotency_key"] == "seq24-dup"]
        assert len(todos) == 1

    def test_concurrent_submission_only_one_succeeds(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        outcomes: list[dict] = []

        def worker(idx: int) -> None:
            outcomes.append(replay.submit_replay_job(
                job_name=f"concurrent_{idx}",
                checkpoint_id=ids[-1],
                topology_name=_topology_name(),
                idempotency_key=f"seq24-concurrent-{idx}",
            ))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        rejected = [o for o in outcomes if o.get("concurrent_rejected")]
        succeeded = [o for o in outcomes if o.get("ok")]
        assert len(rejected) >= 1
        assert len(succeeded) == 1


class TestReplayExecutionAndRetry:
    """执行并发拒绝、验证报告与失败重试边界。"""

    def test_execute_builds_validation_report(self, services, enabled_config):
        cp, replay, lineage = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]
        cp.mark_external_path_missing(latest)

        submitted = replay.submit_replay_job(
            job_name="replay_report",
            checkpoint_id=latest,
            topology_name=_topology_name(),
            idempotency_key="seq24-report",
        )
        executed = replay.execute_replay(submitted["task_id"])
        assert executed["ok"]
        report = executed["validation_report"]
        # 报告记录替代关系、被裁剪版本、来源血缘版本和唯一执行边界
        assert report["substitution"]["to_checkpoint_id"] == older
        assert report["effective_checkpoint_id"] == older
        assert report["requested_checkpoint_id"] == latest
        assert report["lineage"]["source_lineage_version"] == "v2"
        assert report["final_boundary"]["checkpoint_id"] == older
        assert any(p["checkpoint_id"] == latest for p in report["pruned_versions"])

    def test_concurrent_execution_rejected(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        submitted = replay.submit_replay_job(
            job_name="replay_exec_concurrent",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            idempotency_key="seq24-exec-lock",
        )
        task_id = submitted["task_id"]

        # 持有执行锁模拟另一个回放正在执行，随后的执行请求必须走拒绝路径
        assert replay._execute_lock.acquire(blocking=False)
        try:
            rejected = replay.execute_replay(task_id)
        finally:
            replay._execute_lock.release()
        assert rejected.get("concurrent_rejected") is True
        assert not rejected["ok"]

        # 锁释放后正常执行，只产生一次回放结果
        ok = replay.execute_replay(task_id)
        assert ok["ok"]
        task = store.find("replay_engine", task_id)
        assert task["_concurrent_rejections"] == 1
        # 已完成任务再次执行被状态保护拦截，不会重复执行
        again = replay.execute_replay(task_id)
        assert not again["ok"]

    def test_retry_reuses_only_last_available_checkpoint(
        self, services, enabled_config,
    ):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 4)
        latest, older = ids[-1], ids[-2]
        cp.mark_external_path_missing(latest)

        submitted = replay.submit_replay_job(
            job_name="replay_retry",
            checkpoint_id=latest,
            topology_name=_topology_name(),
            idempotency_key="seq24-retry",
        )
        task_id = submitted["task_id"]
        # 人工置为验证失败以触发重试路径
        task = store.find("replay_engine", task_id)
        task["status"] = "验证失败"

        retry = replay.retry_replay(task_id)
        assert retry["ok"]
        # 重试只能复用最后一个可用检查点，不会重新引用被裁剪的最新快照
        assert retry["validation_report"]["effective_checkpoint_id"] == older
        refreshed_task = store.find("replay_engine", task_id)
        assert refreshed_task["_retry_count"] == 1
        assert refreshed_task["_retry_history"][0]["reused_checkpoint_id"] == older

        #再次重试仍然复用同一个最后可用检查点
        refreshed_task["status"] = "验证失败"
        replay.retry_replay(task_id)
        again = store.find("replay_engine", task_id)
        assert again["_retry_count"] == 2
        assert again["实际检查点"] == older

    def test_execute_refreshes_boundary_when_pruned_between(
        self, services, enabled_config,
    ):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest, older = ids[-1], ids[-2]
        submitted = replay.submit_replay_job(
            job_name="replay_drift",
            checkpoint_id=latest,
            topology_name=_topology_name(),
            idempotency_key="seq24-drift",
        )
        task_id = submitted["task_id"]
        # 提交后、执行前，实际检查点被裁剪
        cp.mark_external_path_missing(latest)
        executed = replay.execute_replay(task_id)
        assert executed["ok"]
        task = store.find("replay_engine", task_id)
        assert task["实际检查点"] == older


class TestLineageVersionAndPermissions:
    """血缘版本视图、团队越权与共享只读。"""

    def test_two_scopes_see_different_lineage_versions(self, services):
        _, _, lineage = services
        record_id = f"lineage_scope_{time.time_ns()}"
        store.rows("metric_lineage").append({
            "id": 800000 + int(time.time_ns()) % 100000,
            "记录ID": record_id,
            "指标名称": record_id,
            "上游源端": "src",
            "status": "正常",
            "版本号": "v5",
            "_owner_team": "team_24",
        })
        lineage.bind_snapshot_version(
            record_id, "cp_effective",
            source_checkpoint_id="cp_pruned",
            source_lineage_version="v3",
        )
        team_view = lineage.get_lineage_snapshot_view(
            record_id, scope=MetricLineageService.SCOPE_TEAM,
        )
        shared_view = lineage.get_lineage_snapshot_view(
            record_id, scope=MetricLineageService.SCOPE_SHARED,
        )
        assert team_view["access"] == "team_managed"
        assert team_view["source_lineage_version"] == "v3"
        assert len(team_view["version_history"]) == 1
        # 共享只读视图不暴露来源版本与内部历史
        assert shared_view["access"] == "read_only"
        assert "version_history" not in shared_view
        assert "source_lineage_version" not in shared_view
        assert shared_view["current_snapshot_version"] == "cp_effective"

    def test_replay_engine_cannot_modify_shared_lineage(self, services):
        _, _, lineage = services
        record_id = f"lineage_ro_{time.time_ns()}"
        store.rows("metric_lineage").append({
            "id": 810000 + int(time.time_ns()) % 100000,
            "记录ID": record_id,
            "指标名称": record_id,
            "上游源端": "src",
            "status": "正常",
            "版本号": "v1",
            "_owner_team": "team_24",
        })
        before = dict(store.find("metric_lineage",
                                 next(r["id"] for r in store.rows("metric_lineage")
                                      if r["记录ID"] == record_id)))
        denied = lineage.write_lineage_data(
            record_id, caller="replay_engine", team="team_24",
            values={"版本号": "v999"},
        )
        assert not denied["ok"]
        assert denied["allowed"] is False
        after = next(r for r in store.rows("metric_lineage") if r["记录ID"] == record_id)
        # 受控只读：数据保持不变
        assert after["版本号"] == before["版本号"]

    def test_cross_team_access_rejected_and_ownership_kept(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        submitted = replay.submit_replay_job(
            job_name="replay_team",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            team="team_24",
            idempotency_key="seq24-team",
        )
        task_id = submitted["task_id"]
        # 越权团队访问 24 号记录被拒绝
        denied = replay.get_replay_detail(task_id, team="team_other")
        assert denied.get("forbidden") is True
        assert not denied["ok"]
        denied_exec = replay.execute_replay(task_id, team="team_other")
        assert denied_exec.get("forbidden") is True
        # 越权事务拒绝后回到原归属：归属团队仍是 team_24，且任务状态未被改动
        task = store.find("replay_engine", task_id)
        assert task["_owner_team"] == "team_24"
        assert task["status"] == "待回放"
        # 所属团队可正常访问
        allowed = replay.get_replay_detail(task_id, team="team_24")
        assert allowed["ok"]

    def test_history_filters_by_team(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        replay.submit_replay_job(
            job_name="replay_hist",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            team="team_24",
            idempotency_key="seq24-hist",
        )
        team_24 = replay.list_replay_history(team="team_24")
        team_other = replay.list_replay_history(team="team_other")
        assert any(t["owner_team"] == "team_24" for t in team_24)
        assert all(t["owner_team"] == "team_other" for t in team_other)
        assert all(t["owner_team"] != "team_24" for t in team_other)

    def test_permission_chain_follows_lineage_call_chain(self, services):
        _, _, lineage = services
        result = lineage.locate_permission_chain("指标血缘样例1")
        assert result["ok"]
        modules = [step["module"] for step in result["call_chain"]]
        assert modules[0] == "stream_source"
        assert modules[-1] == "metric_lineage"
        assert all(step["owner_team"] == "team_24" for step in result["call_chain"])


class TestTodosAndRecoveryTradeoff:
    """待办同步与恢复做法取舍。"""

    def test_todo_synced_and_closed_after_execution(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        submitted = replay.submit_replay_job(
            job_name="replay_todo",
            checkpoint_id=ids[-1],
            topology_name=_topology_name(),
            idempotency_key="seq24-todo",
        )
        pending = replay.list_replay_todos(team="team_24")
        assert any(t["idempotency_key"] == "seq24-todo" and t["status"] == "pending"
                   for t in pending)
        replay.execute_replay(submitted["task_id"])
        done = replay.list_replay_todos(team="team_24")
        todo = next(t for t in done if t["idempotency_key"] == "seq24-todo")
        assert todo["status"] == "done"

    def test_compare_recovery_options_tradeoff(self, services, enabled_config):
        cp, replay, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 3)
        latest = ids[-1]
        cp.mark_external_path_missing(latest)

        comparison = replay.compare_recovery_options(latest)
        assert comparison["ok"]
        strategies = {o["strategy"]: o for o in comparison["options"]}
        assert CheckpointMgrService.BOUNDARY_FALLBACK in strategies
        assert CheckpointMgrService.BOUNDARY_ARCHIVE in strategies
        # 归档可用时推荐归档恢复以获得最新位点
        assert comparison["recommendation"] == CheckpointMgrService.BOUNDARY_ARCHIVE
        # 归档不可用时推荐退回
        cp.mark_archive_unavailable(latest)
        comparison2 = replay.compare_recovery_options(latest)
        assert comparison2["recommendation"] == CheckpointMgrService.BOUNDARY_FALLBACK

    def test_archive_restore_is_idempotent(self, services, enabled_config):
        cp, _, _ = services
        _, config_name = enabled_config
        ids = _make_snapshots(cp, config_name, 2)
        latest = ids[-1]
        cp.mark_external_path_missing(latest)
        first = cp.restore_from_archive(latest)
        second = cp.restore_from_archive(latest)
        assert first["ok"] and second["ok"]
        assert first["already_restored"] is False
        assert second["already_restored"] is True
        # 只保留一个可读边界
        assert cp.is_snapshot_readable(cp._find_snapshot(latest))
