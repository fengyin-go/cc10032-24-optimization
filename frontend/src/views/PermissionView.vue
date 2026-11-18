<template>
  <section class="page permission-page">
    <header class="page-head">
      <div>
        <h2>序号24 · 权限贯通</h2>
        <p class="page-desc">
          24号记录仅限所属团队查看；共享指标血缘受控只读，回放引擎在详情页不能改动数据；
          越权访问接口拒绝返回结果，越权事务拒绝后回到原归属。
        </p>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">所属团队</span>
        <strong class="stat-value">{{ overview.team || 'team_24' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">团队血缘记录</span>
        <strong class="stat-value">{{ overview.team_owned_records?.length ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">共享只读记录</span>
        <strong class="stat-value">{{ overview.shared_readonly_records?.length ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待同步待办</span>
        <strong class="stat-value">{{ todos.length }}</strong>
      </article>
    </div>

    <p v-if="message" class="page-foot" :class="messageKind">{{ message }}</p>

    <div class="two-col">
      <div>
        <h3>血缘调用链权限定位</h3>
        <form class="filter-bar" @submit.prevent="loadChain">
          <input v-model="metricName" data-testid="metric-input" placeholder="输入指标/血缘记录ID" />
          <button class="btn primary" type="submit">定位权限</button>
        </form>
        <table class="data-table" data-testid="permission-chain">
          <thead><tr><th>步骤</th><th>模块</th><th>可达</th><th>归属团队</th></tr></thead>
          <tbody>
            <tr v-for="step in chain" :key="step.module">
              <td>{{ step.step }}</td>
              <td>{{ step.module }}</td>
              <td>{{ step.reachable ? '是' : '否' }}</td>
              <td>{{ step.owner_team }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div>
        <h3>两种权限范围的血缘版本</h3>
        <form class="filter-bar" @submit.prevent="loadViews">
          <input v-model="recordId" data-testid="record-input" placeholder="血缘记录ID" />
          <button class="btn primary" type="submit">查询版本</button>
        </form>
        <table class="data-table" data-testid="scope-views">
          <thead><tr><th>范围</th><th>访问级别</th><th>当前快照版本</th><th>来源血缘版本</th></tr></thead>
          <tbody>
            <tr>
              <td>所属团队</td>
              <td>{{ teamView.access }}</td>
              <td class="mono">{{ teamView.current_snapshot_version || '—' }}</td>
              <td class="mono">{{ teamView.source_lineage_version || '—' }}</td>
            </tr>
            <tr>
              <td>共享只读</td>
              <td>{{ sharedView.access }}</td>
              <td class="mono">{{ sharedView.current_snapshot_version || '—' }}</td>
              <td>受控隐藏</td>
            </tr>
          </tbody>
        </table>

        <h4>受控只读写入验证（回放引擎）</h4>
        <button class="btn" data-testid="readonly-write" type="button" @click="attemptReadOnlyWrite">
          以回放引擎身份尝试改动血缘
        </button>
      </div>
    </div>

    <h3>其他入口同步待办</h3>
    <table class="data-table" data-testid="todo-table">
      <thead><tr><th>任务</th><th>类型</th><th>状态</th><th>实际检查点</th><th>团队</th></tr></thead>
      <tbody>
        <tr v-for="todo in todos" :key="todo.idempotency_key">
          <td>{{ todo.title }}</td>
          <td>{{ todo.type }}</td>
          <td>{{ todo.status }}</td>
          <td class="mono">{{ todo.effective_checkpoint_id }}</td>
          <td>{{ todo.team }}</td>
        </tr>
        <tr v-if="!todos.length"><td colspan="5" class="empty-state">暂无待办</td></tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  fetchLineageSnapshot,
  fetchPermissionChain,
  fetchPermissionOverview,
  fetchReplayTodos,
  writeLineageData,
  type LineageSnapshotView,
  type PermissionChainStep,
  type PermissionOverview,
  type ReplayTodo,
} from '@/api/replay-service'

const team = 'team_24'
const overview = ref<Partial<PermissionOverview>>({})
const todos = ref<ReplayTodo[]>([])
const chain = ref<PermissionChainStep[]>([])
const metricName = ref('指标血缘样例1')
const recordId = ref('')
const teamView = ref<Partial<LineageSnapshotView>>({})
const sharedView = ref<Partial<LineageSnapshotView>>({})
const message = ref('')
const messageKind = ref('')

function notify(text: string, kind = '') {
  message.value = text
  messageKind.value = kind
}

async function loadOverview() {
  overview.value = await fetchPermissionOverview(team)
  const todoResp = await fetchReplayTodos(team)
  todos.value = todoResp.items
  if (!recordId.value && overview.value.team_owned_records?.length) {
    recordId.value = overview.value.team_owned_records[0]
  }
}

async function loadChain() {
  const resp = await fetchPermissionChain(metricName.value)
  chain.value = resp.call_chain ?? []
}

async function loadViews() {
  if (!recordId.value) return
  const [t, s] = await Promise.all([
    fetchLineageSnapshot(recordId.value, 'team'),
    fetchLineageSnapshot(recordId.value, 'shared'),
  ])
  teamView.value = t
  sharedView.value = s
}

async function attemptReadOnlyWrite() {
  if (!recordId.value) {
    notify('请先查询血缘版本', 'error-text')
    return
  }
  const result = await writeLineageData(
    recordId.value,
    'replay_engine',
    team,
    { 版本号: 'v_forbidden' },
  )
  if (!result.ok && result.status === 403) {
    notify('已拒绝：回放引擎对共享指标血缘仅受控只读，数据未改动（HTTP 403）')
  } else {
    notify('写入未被拦截，不符合预期', 'error-text')
  }
}

onMounted(async () => {
  await loadOverview()
  await loadChain()
  if (recordId.value) await loadViews()
})
</script>

<style scoped>
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.error-text { color: #b91c1c; }
.filter-bar { display: flex; gap: 8px; margin: 8px 0; }
</style>
