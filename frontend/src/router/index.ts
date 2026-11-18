import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import PermissionView from '@/views/PermissionView.vue'
import ReplayConsoleView from '@/views/ReplayConsoleView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/replay-console', name: 'replay-console', component: ReplayConsoleView },
    { path: '/permission-24', name: 'permission-24', component: PermissionView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
