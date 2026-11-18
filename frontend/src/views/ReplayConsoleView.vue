<template>
  <section class="page replay-console">
    <header class="page-head">
      <div>
        <h2>回放恢复控制台 · 序号24</h2>
        <p class="page-desc">
          回放任务只引用仍可读取的检查点；最新检查点被裁剪时退回最后可用快照或从外部归档恢复，
          对外只保留一个实际执行的恢复边界。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-submit" type="button" @click="showSubmit = true">
          提交回放任务
        </button>
        <button class="btn" type="button" @click="reload">刷新历史</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">回放任务数</span>
        <strong class="stat-value">{{ history.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">发生快照替代</span>
        <strong class="stat-value">{{ substitutedCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可用检查点</span>
        <strong class="stat-value">{{ availableCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">被裁剪版本</span>
        <strong class="stat-value">{{ prunedCount }}</strong>
      </article>
    </div>

    <p v-if="message" class="page-foot" :class="messageKind">{{ message }}</p>

    <h3>回放历史（团队 {{ team }}）</h3>
    <table class="data-table" data-testid="replay-history">
      <thead>
        <tr>
          <th>任务ID</th><th>任务名称</th><th>源检查点</th><th>实际检查点</th>
          <th>状态</th><th>结果验证</th><th>替代</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in history" :key="item.task_id" :data-testid="`row-${item.task_id}`">
          <td>{{ item.task_id }}</td>
          <td>{{ item['任务名称'] }}</td>
          <td class="mono">{{ item['源检查点'] }}</td>
          <td class="mono">
            {{ item['实际检查点'] }}
            <span v-if="item.substituted" class="tag warn">已替代</span>
          </td>
          <td>{{ item.status }}</td>
          <td>{{ item['结果验证'] }}</td>
          <td>{{ item.substituted ? '是' : '否' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="openReport(item.task_id)">验证报告</button>
            <button
              v-if="item.status === '验证失败' || item.status === '待回放'"
              class="link"
              type="button"
              @click="onRetry(item.task_id)"
            >
              重试（复用最后可用）
            </button>
          </td>
        </tr>
        <tr v-if="!history.length">
          <td colspan="8" class="empty-state">暂无回放任务</td>
        </tr>
      </tbody>
    </table>

    <div class="two-col">
      <div>
        <h3>可用快照（检查点模块提供）</h3>
        <table class="data-table" data-testid="available-snapshots">
          <thead><tr><th>检查点</th><th>配置</th><th>算子数</th><th>血缘版本</th></tr></thead>
          <tbody>
            <tr v-for="cp in available" :key="cp.checkpoint_id">
              <td class="mono">{{ cp.checkpoint_id }}</td>
              <td>{{ cp.config }}</td>
              <td>{{ cp.operator_count }}</td>
              <td class="mono">{{ cp.lineage_version }}</td>
            </tr>
            <tr v-if="!available.length"><td colspan="4" class="empty-state">暂无可用快照</td></tr>
          </tbody>
        </table>
      </div>
      <div>
        <h3>被裁剪版本登记</h3>
        <table class="data-table" data-testid="pruned-versions">
          <thead><tr><th>检查点</th><th>来源血缘版本</th><th>裁剪原因</th></tr></thead>
          <tbody>
            <tr v-for="p in pruned" :key="p.checkpoint_id">
              <td class="mono">{{ p.checkpoint_id }}</td>
              <td class="mono">{{ p.lineage_version }}</td>
              <td>{{ p.reason }}</td>
            </tr>
            <tr v-if="!pruned.length"><td colspan="3" class="empty-state">暂无裁剪记录</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 提交弹窗 -->
    <div v-if="showSubmit" class="modal-backdrop" data-testid="submit-modal">
      <form class="modal" @submit.prevent="onSubmit">
        <h3>提交回放任务（幂等）</h3>
        <label class="create-field"><span>任务名称</span>
          <input v-model="form.job_name" data-testid="f-job" required />
        </label>
        <label class="create-field"><span>源检查点ID</span>
          <input v-model="form.checkpoint_id" data-testid="f-cp" required />
        </label>
        <label class="create-field"><span>目标拓扑</span>
          <input v-model="form.topology_name" data-testid="f-topo" required />
        </label>
        <label class="create-field"><span>恢复策略</span>
          <select v-model="form.recovery_strategy" data-testid="f-strategy">
            <option value="fallback_last_available">退回最后可用快照</option>
            <option value="restore_from_archive">从外部归档恢复最新快照</option>
          </select>
        </label>
        <label class="create-field"><span>幂等键（序号24重复提交只记一次）</span>
          <input v-model="form.idempotency_key" data-testid="f-idem" :placeholder="autoIdem" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showSubmit = false">取消</button>
          <button class="btn primary" data-testid="f-submit" type="submit">确认提交</button>
        </div>
      </form>
    </div>

    <!-- 验证报告弹窗 -->
    <div v-if="report" class="modal-backdrop" data-testid="report-modal">
      <div class="modal wide">
        <h3>回放验证报告 · 任务{{ report.task_id }}</h3>
        <p class="page-desc">生成时间：{{ report.generated_at }} · 团队：{{ report.team }}</p>
        <table class="data-table">
          <tbody>
            <tr><th>请求检查点</th><td class="mono">{{ report.requested_checkpoint_id }}</td></tr>
            <tr><th>实际可用检查点</th><td class="mono">{{ report.effective_checkpoint_id }}</td></tr>
            <tr><th>边界策略</th><td>{{ report.boundary_strategy }}</td></tr>
            <tr>
              <th>快照替代关系</th>
              <td v-if="report.substitution" class="mono">
                {{ report.substitution.from_checkpoint_id }} → {{ report.substitution.to_checkpoint_id }}
                （{{ report.substitution.type }}）
              </td>
              <td v-else>无替代</td>
            </tr>
            <tr>
              <th>血缘来源版本</th>
              <td class="mono">
                当前快照 {{ report.lineage.current_snapshot_version }} ｜
                来源快照 {{ report.lineage.source_snapshot_version }} ｜
                来源血缘版本 {{ report.lineage.source_lineage_version }}
              </td>
            </tr>
            <tr><th>唯一执行边界</th><td class="mono">{{ report.final_boundary.checkpoint_id }}</td></tr>
            <tr><th>重试次数</th><td>{{ report.retry_count }}</td></tr>
          </tbody>
        </table>
        <h4>被裁剪版本（{{ report.pruned_versions.length }}）</h4>
        <ul class="pruned-list">
          <li v-for="p in report.pruned_versions" :key="p.checkpoint_id" class="mono">
            {{ p.checkpoint_id }} · 血缘 {{ p.lineage_version }} · {{ p.reason }}
          </li>
        </ul>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="report = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  fetchAvailableSnapshots,
  fetchPrunedVersions,
  fetchReplayHistory,
  fetchValidationReport,
  retryReplay,
  submitReplayJob,
  type PrunedVersion,
  type SnapshotSummary,
  type ValidationReport,
} from '@/api/replay-service'

const team = 'team_24'
const history = ref<Awaited<ReturnType<typeof fetchReplayHistory>>['items']>([])
const available = ref<SnapshotSummary[]>([])
const pruned = ref<PrunedVersion[]>([])
const showSubmit = ref(false)
const report = ref<(ValidationReport & { ok: boolean }) | null>(null)
const message = ref('')
const messageKind = ref('')
const autoIdem = `seq24-${Date.now()}`
const form = ref({
  job_name: '',
  checkpoint_id: '',
  topology_name: '任务拓扑样例1',
  recovery_strategy: 'fallback_last_available',
  idempotency_key: '',
})

const substitutedCount = computed(() => history.value.filter((h) => h.substituted).length)
const availableCount = computed(() => available.value.length)
const prunedCount = computed(() => pruned.value.length)

function notify(text: string, kind = '') {
  message.value = text
  messageKind.value = kind
}

async function reload() {
  try {
    const [hist, snaps, prunedResp] = await Promise.all([
      fetchReplayHistory(team),
      fetchAvailableSnapshots(),
      fetchPrunedVersions(),
    ])
    history.value = hist.items
    available.value = snaps.items
    pruned.value = prunedResp.items
  } catch (error) {
    notify(error instanceof Error ? error.message : '回放数据读取失败', 'error-text')
  }
}

async function onSubmit() {
  try {
    const result = await submitReplayJob({
      ...form.value,
      idempotency_key: form.value.idempotency_key || autoIdem,
      team,
    })
    if (!result.ok) throw new Error(result.error || '提交失败')
    notify(
      result.idempotent
        ? `重复提交已合并，复用任务 #${result.task_id}`
        : `回放任务 #${result.task_id} 已提交，实际检查点 ${result.effective_checkpoint_id}`,
    )
    showSubmit.value = false
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '提交失败', 'error-text')
  }
}

async function openReport(taskId: number) {
  try {
    report.value = await fetchValidationReport(taskId, team)
  } catch (error) {
    notify(error instanceof Error ? error.message : '报告读取失败', 'error-text')
  }
}

async function onRetry(taskId: number) {
  try {
    const result = await retryReplay(taskId, team)
    if (!result.ok) throw new Error(result.error || '重试失败')
    const cpId = result.validation_report?.effective_checkpoint_id ?? ''
    notify(`任务 #${taskId} 重试完成，复用检查点 ${cpId}`)
    await reload()
  } catch (error) {
    notify(error instanceof Error ? error.message : '重试失败', 'error-text')
  }
}

onMounted(reload)
</script>

<style scoped>
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 12px; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.tag { display: inline-block; margin-left: 6px; padding: 1px 6px; border-radius: 10px; font-size: 11px; }
.tag.warn { background: #fef3c7; color: #b45309; }
.modal.wide { width: min(760px, 100%); }
.pruned-list { max-height: 160px; overflow: auto; padding-left: 18px; }
.create-field { display: flex; flex-direction: column; gap: 4px; margin-bottom: 10px; }
.error-text { color: #b91c1c; }
</style>
