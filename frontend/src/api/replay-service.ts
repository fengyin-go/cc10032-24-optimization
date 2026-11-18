/**
 * 序号24：回放恢复领域接口封装。
 * 对应后端 /api/replay 固定路径路由，覆盖回放任务、检查点边界、验证报告与血缘权限。
 */
import { fetchJson, request } from './client'

const BASE = '/api/replay'

export type BoundaryOption = {
  strategy: string
  label: string
  feasible: boolean
  target_checkpoint_id: string | null
  pros: string[]
  cons: string[]
}

export type ValidationReport = {
  report_id: string
  task_id: number
  任务名称: string
  team: string
  generated_at: string
  requested_checkpoint_id: string
  effective_checkpoint_id: string
  boundary_strategy: string
  substituted: boolean
  substitution: {
    from_checkpoint_id: string
    to_checkpoint_id: string
    type: string
  } | null
  pruned_versions: Array<{
    checkpoint_id: string
    config: string
    lineage_version: string
    reason: string
    pruned_at: number
  }>
  lineage: {
    record_id: string
    current_snapshot_version: string
    source_snapshot_version: string
    source_lineage_version: string
  }
  final_boundary: { checkpoint_id: string; note: string }
  retry_count: number
}

export type ReplayHistoryItem = {
  task_id: number
  任务名称: string
  源检查点: string
  实际检查点: string
  回放模式: string
  结果验证: string
  进度: string
  status: string
  owner_team: string
  substituted: boolean
}

export type SnapshotSummary = {
  checkpoint_id: string
  config: string
  operator_count: number
  operators: string[]
  snapshot_ts: number
  external_path: string
  external_missing: boolean
  archive_available: boolean
  lineage_version: string
  restored_from_archive: boolean
}

export type PrunedVersion = {
  checkpoint_id: string
  config: string
  snapshot_ts: number
  lineage_version: string
  external_path: string
  reason: string
  pruned_at: number
  restored_checkpoint_id: string | null
}

export type LineageSnapshotView = {
  ok: boolean
  记录ID: string
  指标名称: string
  scope: string
  版本号: string
  current_snapshot_version: string
  access: string
  source_snapshot_version?: string
  source_lineage_version?: string
  version_history?: Array<Record<string, unknown>>
}

export type PermissionChainStep = {
  step: number
  module: string
  reachable: boolean
  owner_team: string
}

export type PermissionOverview = {
  ok: boolean
  team: string
  team_owned_records: string[]
  shared_readonly_records: string[]
  read_only_callers: string[]
  call_chain: string[]
}

export type ReplayTodo = {
  idempotency_key: string
  task_id: number
  type: string
  title: string
  status: string
  team: string
  effective_checkpoint_id: string
}

export type SubmitResult = {
  ok: boolean
  idempotent?: boolean
  task_id: number
  task_name?: string
  error?: string
  effective_checkpoint_id?: string
  substituted?: boolean
  boundary_strategy?: string
  team?: string
  lineage?: Record<string, unknown>
}

export type ExecuteResult = {
  ok: boolean
  task_id: number
  error?: string
  concurrent_rejected?: boolean
  validation_report?: ValidationReport
}

export async function submitReplayJob(payload: {
  job_name: string
  checkpoint_id: string
  topology_name: string
  mode?: string
  team?: string
  idempotency_key?: string
  recovery_strategy?: string
}): Promise<SubmitResult> {
  const response = await request(`${BASE}/jobs`, {
    method: 'POST',
    body: JSON.stringify({ mode: 'full_replay', team: 'team_24', ...payload }),
  })
  return response.json()
}

export async function executeReplay(taskId: number, team = 'team_24'): Promise<ExecuteResult> {
  const response = await request(`${BASE}/jobs/${taskId}/execute?team=${team}`, {
    method: 'POST',
  })
  return response.json()
}

export async function retryReplay(taskId: number, team = 'team_24'): Promise<ExecuteResult> {
  const response = await request(`${BASE}/jobs/${taskId}/retry?team=${team}`, {
    method: 'POST',
  })
  return response.json()
}

export async function fetchReplayHistory(team = 'team_24'): Promise<{
  items: ReplayHistoryItem[]
  total: number
}> {
  return fetchJson(`${BASE}/history?team=${team}`)
}

export async function fetchReplayDetail(taskId: number, team = 'team_24'): Promise<Record<string, any>> {
  return fetchJson(`${BASE}/jobs/${taskId}?team=${team}`)
}

export async function fetchValidationReport(
  taskId: number,
  team = 'team_24',
): Promise<ValidationReport & { ok: boolean }> {
  return fetchJson(`${BASE}/jobs/${taskId}/report?team=${team}`)
}

export async function fetchAvailableSnapshots(configName?: string): Promise<{
  items: SnapshotSummary[]
}> {
  const query = configName ? `?config_name=${encodeURIComponent(configName)}` : ''
  return fetchJson(`${BASE}/checkpoints/available${query}`)
}

export async function fetchPrunedVersions(configName?: string): Promise<{
  items: PrunedVersion[]
}> {
  const query = configName ? `?config_name=${encodeURIComponent(configName)}` : ''
  return fetchJson(`${BASE}/checkpoints/pruned${query}`)
}

export async function resolveBoundary(
  checkpointId: string,
  strategy: string,
  configName?: string,
): Promise<Record<string, any>> {
  const response = await request(
    `${BASE}/checkpoints/${checkpointId}/boundary`,
    {
      method: 'POST',
      body: JSON.stringify({ strategy, config_name: configName ?? null }),
    },
  )
  return response.json()
}

export async function restoreFromArchive(
  checkpointId: string,
): Promise<Record<string, any>> {
  const response = await request(
    `${BASE}/checkpoints/${checkpointId}/archive-restore`,
    { method: 'POST' },
  )
  return response.json()
}

export async function compareRecoveryOptions(
  checkpointId: string,
): Promise<{ options: BoundaryOption[]; recommendation: string; readable: boolean; ok: boolean }> {
  return fetchJson(`${BASE}/recovery-options/${checkpointId}`)
}

export async function fetchLineageSnapshot(
  recordId: string,
  scope: 'team' | 'shared' = 'team',
): Promise<LineageSnapshotView> {
  return fetchJson(`${BASE}/lineage/${encodeURIComponent(recordId)}/snapshot?scope=${scope}`)
}

export async function writeLineageData(
  recordId: string,
  caller: string,
  team: string,
  values: Record<string, string>,
): Promise<{ ok: boolean; status: number; body: Record<string, any> }> {
  const response = await request(`${BASE}/lineage/${encodeURIComponent(recordId)}/write`, {
    method: 'POST',
    body: JSON.stringify({ caller, team, values }),
  })
  return {
    ok: response.ok,
    status: response.status,
    body: await response.json().catch(() => ({})),
  }
}

export async function fetchPermissionOverview(team = 'team_24'): Promise<PermissionOverview> {
  return fetchJson(`${BASE}/lineage/permission?team=${team}`)
}

export async function fetchPermissionChain(metricName: string): Promise<{
  ok: boolean
  metric: string
  call_chain: PermissionChainStep[]
  owner_team: string
}> {
  return fetchJson(`${BASE}/lineage/permission-chain?metric_name=${encodeURIComponent(metricName)}`)
}

export async function fetchReplayTodos(team = 'team_24'): Promise<{ items: ReplayTodo[] }> {
  return fetchJson(`${BASE}/todos?team=${team}`)
}
