"""回放恢复专用接口：检查点边界解析、回放任务、验证报告与血缘权限。

与通用资源路由区分：这里承载回放引擎/检查点/血缘三个模块的跨模块领域动作。
越权访问返回 403，并发冲突返回 409，保证权限与并发拒绝路径对外可核验。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.checkpoint_mgr import CheckpointMgrService
from app.services.metric_lineage import MetricLineageService
from app.services.replay_engine import ReplayEngineService

router = APIRouter(prefix="/api/replay", tags=["回放恢复"])

replay_service = ReplayEngineService()
checkpoint_service = CheckpointMgrService()
lineage_service = MetricLineageService()


class SubmitReplayPayload(BaseModel):
    job_name: str
    checkpoint_id: str
    topology_name: str
    mode: str = "full_replay"
    team: str = ReplayEngineService.DEFAULT_TEAM
    idempotency_key: str | None = None
    recovery_strategy: str = CheckpointMgrService.BOUNDARY_FALLBACK
    lineage_record_id: str | None = None


class BoundaryPayload(BaseModel):
    strategy: str = CheckpointMgrService.BOUNDARY_FALLBACK
    config_name: str | None = None


class LineageWritePayload(BaseModel):
    caller: str
    team: str
    values: dict[str, Any] = Field(default_factory=dict)


def _deny_if_needed(result: dict[str, Any]) -> dict[str, Any]:
    """把领域层的拒绝结论翻译成对应 HTTP 状态码。"""
    if result.get("forbidden"):
        raise HTTPException(status_code=403, detail=result.get("error", "越权访问已拒绝"))
    if result.get("concurrent_rejected"):
        raise HTTPException(status_code=409, detail=result.get("error", "并发请求已拒绝"))
    return result


# ---- 回放任务 ----

@router.post("/jobs")
def submit_replay_job(payload: SubmitReplayPayload) -> dict[str, Any]:
    result = replay_service.submit_replay_job(
        job_name=payload.job_name,
        checkpoint_id=payload.checkpoint_id,
        topology_name=payload.topology_name,
        mode=payload.mode,
        team=payload.team,
        idempotency_key=payload.idempotency_key,
        recovery_strategy=payload.recovery_strategy,
        lineage_record_id=payload.lineage_record_id,
    )
    return _deny_if_needed(result)


@router.post("/jobs/{task_id}/execute")
def execute_replay(task_id: int, team: str | None = None) -> dict[str, Any]:
    result = replay_service.execute_replay(task_id, team=team)
    return _deny_if_needed(result)


@router.post("/jobs/{task_id}/retry")
def retry_replay(task_id: int, team: str | None = None) -> dict[str, Any]:
    result = replay_service.retry_replay(task_id, team=team)
    return _deny_if_needed(result)


@router.get("/jobs/{task_id}")
def get_replay_detail(task_id: int, team: str | None = None) -> dict[str, Any]:
    result = replay_service.get_replay_detail(task_id, team=team)
    return _deny_if_needed(result)


@router.get("/jobs/{task_id}/report")
def get_validation_report(task_id: int, team: str | None = None) -> dict[str, Any]:
    result = replay_service.get_validation_report(task_id, team=team)
    return _deny_if_needed(result)


@router.get("/history")
def list_replay_history(team: str | None = None) -> dict[str, Any]:
    items = replay_service.list_replay_history(team=team)
    return {"ok": True, "total": len(items), "items": items}


@router.get("/todos")
def list_replay_todos(team: str | None = None) -> dict[str, Any]:
    return {"ok": True, "items": replay_service.list_replay_todos(team=team)}


# ---- 检查点边界 ----

@router.get("/checkpoints/available")
def list_available_snapshots(config_name: str | None = None) -> dict[str, Any]:
    items = checkpoint_service.list_available_snapshots(config_name)
    return {"ok": True, "total": len(items), "items": items}


@router.get("/checkpoints/pruned")
def list_pruned_versions(config_name: str | None = None) -> dict[str, Any]:
    return {"ok": True, "items": checkpoint_service.list_pruned_versions(config_name)}


@router.post("/checkpoints/{checkpoint_id}/boundary")
def resolve_boundary(checkpoint_id: str, payload: BoundaryPayload) -> dict[str, Any]:
    return checkpoint_service.resolve_replay_boundary(
        checkpoint_id, strategy=payload.strategy, config_name=payload.config_name,
    )


@router.post("/checkpoints/{checkpoint_id}/archive-restore")
def restore_from_archive(checkpoint_id: str) -> dict[str, Any]:
    result = checkpoint_service.restore_from_archive(checkpoint_id)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("error", "归档恢复失败"))
    return result


@router.get("/recovery-options/{checkpoint_id}")
def compare_recovery_options(checkpoint_id: str) -> dict[str, Any]:
    return replay_service.compare_recovery_options(checkpoint_id)


@router.post("/checkpoints/{checkpoint_id}/mark-external-missing")
def mark_external_missing(checkpoint_id: str) -> dict[str, Any]:
    return checkpoint_service.mark_external_path_missing(checkpoint_id)


# ---- 指标血缘版本与权限 ----

@router.get("/lineage/{record_id}/snapshot")
def lineage_snapshot_view(
    record_id: str, scope: str = MetricLineageService.SCOPE_TEAM,
) -> dict[str, Any]:
    return lineage_service.get_lineage_snapshot_view(record_id, scope=scope)


@router.post("/lineage/{record_id}/write")
def write_lineage_data(record_id: str, payload: LineageWritePayload) -> dict[str, Any]:
    result = lineage_service.write_lineage_data(
        record_id, payload.caller, payload.team, payload.values,
    )
    # 只读/越权写入被拒绝：返回 403，血缘数据保持不变
    if not result.get("ok"):
        raise HTTPException(status_code=403, detail=result.get("error", "写入被拒绝"))
    return result


@router.get("/lineage/permission")
def permission_overview(team: str = ReplayEngineService.DEFAULT_TEAM) -> dict[str, Any]:
    return lineage_service.permission_overview(team=team)


@router.get("/lineage/permission-chain")
def permission_chain(metric_name: str) -> dict[str, Any]:
    return lineage_service.locate_permission_chain(metric_name)
