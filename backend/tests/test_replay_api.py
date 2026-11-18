"""序号24：回放恢复接口的 HTTP 层集成测试。

验证固定路径路由 /api/replay 的状态码契约：
  - 正常提交/执行 200，越权访问 403，并发冲突 409
  - 共享血缘受控只读写入 403
  - 回放历史、验证报告、可用快照与裁剪版本接口可读
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.replay import checkpoint_service as cp_service
from app.store import store


client = TestClient(app)


@pytest.fixture
def checkpoint_env():
    cp = cp_service
    config_name = f"http_cfg_{time.time_ns()}"
    store.rows("checkpoint_mgr").append({
        "id": 777000 + int(time.time_ns()) % 100000,
        "配置名称": config_name,
        "间隔时间": "30",
        "超时时长": "60",
        "最大并发": "3",
        "对齐策略": "exactly_once",
        "保留数量": 5,
        "外部化路径": "/data/cp",
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    })
    ids = []
    for i in range(3):
        result = cp.create_checkpoint(
            config_name, {f"op_{i}": {"offset": i}}, lineage_version=f"v{i}",
        )
        ids.append(result["checkpoint_id"])
        time.sleep(0.005)
    return cp, config_name, ids


def test_submit_fallback_and_report_http(checkpoint_env):
    cp, config_name, ids = checkpoint_env
    latest, older = ids[-1], ids[-2]
    cp.mark_external_path_missing(latest)

    # 可用快照列表只含可读快照
    resp = client.get(f"/api/replay/checkpoints/available?config_name={config_name}")
    assert resp.status_code == 200
    available = [item["checkpoint_id"] for item in resp.json()["items"]]
    assert latest not in available and older in available

    # 提交回放，退回到最后可用快照
    resp = client.post("/api/replay/jobs", json={
        "job_name": "http_seq24",
        "checkpoint_id": latest,
        "topology_name": "任务拓扑样例1",
        "idempotency_key": "http-seq24",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["substituted"] is True
    assert body["effective_checkpoint_id"] == older
    task_id = body["task_id"]

    # 重复提交幂等
    resp2 = client.post("/api/replay/jobs", json={
        "job_name": "http_seq24",
        "checkpoint_id": latest,
        "topology_name": "任务拓扑样例1",
        "idempotency_key": "http-seq24",
    })
    assert resp2.status_code == 200
    assert resp2.json().get("idempotent") is True
    assert resp2.json()["task_id"] == task_id

    # 越权访问 403
    forbidden = client.get(f"/api/replay/jobs/{task_id}?team=team_other")
    assert forbidden.status_code == 403

    # 所属团队执行并取得验证报告
    executed = client.post(f"/api/replay/jobs/{task_id}/execute?team=team_24")
    assert executed.status_code == 200
    report = executed.json()["validation_report"]
    assert report["effective_checkpoint_id"] == older
    assert report["requested_checkpoint_id"] == latest
    assert report["lineage"]["source_lineage_version"] == "v2"
    assert any(p["checkpoint_id"] == latest for p in report["pruned_versions"])

    # 报告接口受控可核验
    report_resp = client.get(f"/api/replay/jobs/{task_id}/report?team=team_24")
    assert report_resp.status_code == 200
    assert report_resp.json()["final_boundary"]["checkpoint_id"] == older


def test_concurrent_execution_returns_409(checkpoint_env):
    cp, _, ids = checkpoint_env
    submitted = client.post("/api/replay/jobs", json={
        "job_name": "http_lock",
        "checkpoint_id": ids[-1],
        "topology_name": "任务拓扑样例1",
        "idempotency_key": "http-lock",
    }).json()
    task_id = submitted["task_id"]

    # 模拟另一回放正在执行：占用执行锁后请求应返回 409
    from app.routers.replay import replay_service
    assert replay_service._execute_lock.acquire(blocking=False)
    try:
        resp = client.post(f"/api/replay/jobs/{task_id}/execute?team=team_24")
        assert resp.status_code == 409
    finally:
        replay_service._execute_lock.release()


def test_shared_lineage_read_only_write_403(checkpoint_env):
    cp, _, ids = checkpoint_env
    submitted = client.post("/api/replay/jobs", json={
        "job_name": "http_ro",
        "checkpoint_id": ids[-1],
        "topology_name": "任务拓扑样例1",
        "idempotency_key": "http-ro",
    }).json()
    task_id = submitted["task_id"]
    client.post(f"/api/replay/jobs/{task_id}/execute?team=team_24")
    report = client.get(
        f"/api/replay/jobs/{task_id}/report?team=team_24").json()
    record_id = report["lineage"]["record_id"]

    # 回放引擎写共享血缘 -> 403
    resp = client.post(f"/api/replay/lineage/{record_id}/write", json={
        "caller": "replay_engine", "team": "team_24", "values": {"版本号": "v999"},
    })
    assert resp.status_code == 403

    # 两种权限范围视图
    team_view = client.get(
        f"/api/replay/lineage/{record_id}/snapshot?scope=team").json()
    shared_view = client.get(
        f"/api/replay/lineage/{record_id}/snapshot?scope=shared").json()
    assert "version_history" in team_view
    assert "version_history" not in shared_view


def test_archive_restore_and_history_http(checkpoint_env):
    cp, _, ids = checkpoint_env
    latest = ids[-1]
    cp.mark_external_path_missing(latest)

    # 归档恢复
    resp = client.post(f"/api/replay/checkpoints/{latest}/archive-restore")
    assert resp.status_code == 200
    assert resp.json()["already_restored"] is False
    # 幂等：再次恢复
    again = client.post(f"/api/replay/checkpoints/{latest}/archive-restore")
    assert again.json()["already_restored"] is True

    # 回放历史按团队过滤
    history = client.get("/api/replay/history?team=team_24")
    assert history.status_code == 200
    assert all(item["owner_team"] == "team_24" for item in history.json()["items"])

    # 裁剪版本登记接口
    pruned = client.get("/api/replay/checkpoints/pruned")
    assert pruned.status_code == 200
    assert any(p["checkpoint_id"] == latest for p in pruned.json()["items"])

    # 权限链接口
    chain = client.get("/api/replay/lineage/permission-chain?metric_name=指标血缘样例1")
    assert chain.status_code == 200
    assert chain.json()["call_chain"][0]["module"] == "stream_source"
