import { createRouter, createWebHashHistory } from 'vue-router';
import Home from '@/views/HomeView.vue';
import Transfer from '@/views/TransferView.vue';
import Tasks from '@/views/TasksView.vue';
import Settings from '@/views/SettingsView.vue';
import OpenAPI from '@/views/OpenAPIView.vue';
import Logs from '@/views/LogsView.vue';
import About from '@/views/AboutView.vue';

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/home' },
    { path: '/home', component: Home },
    { path: '/transfer', component: Transfer },
    { path: '/tasks', component: Tasks },
    { path: '/settings', component: Settings },
    { path: '/openapi', component: OpenAPI },
    { path: '/logs', component: Logs },
    { path: '/about', component: About }
  ]
});

export default router;
