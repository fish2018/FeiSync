<template>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="sidebar-top">
        <h1>FeiSync</h1>
        <nav>
          <RouterLink to="/home">首页</RouterLink>
          <RouterLink to="/transfer">
            传输
            <span v-if="transferBadge" class="nav-badge">{{ transferBadge }}</span>
          </RouterLink>
          <RouterLink to="/tasks">任务</RouterLink>
          <RouterLink to="/settings">设置</RouterLink>
          <RouterLink to="/openapi">API</RouterLink>
          <RouterLink to="/logs">日志</RouterLink>
          <RouterLink to="/about">关于</RouterLink>
        </nav>
      </div>
      <footer class="sidebar-footer">
        <p>容量</p>
        <strong>{{ capacityText }}</strong>
      </footer>
    </aside>
    <main>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import { useTenantStore } from '@/stores/tenantStore';
import { useSecurityStore } from '@/stores/securityStore';
import { useGroupStore } from '@/stores/groupStore';
import { useUiStore } from '@/stores/uiStore';
import { useTransferStore } from '@/stores/transferStore';
import { useTaskStore } from '@/stores/taskStore';

const tenantStore = useTenantStore();
const groupStore = useGroupStore();
const securityStore = useSecurityStore();
const uiStore = useUiStore();
const transferStore = useTransferStore();
const taskStore = useTaskStore();

const allowedTenantIds = computed(() => {
  const fallback = () => {
    const set = new Set<string>();
    tenantStore.tenants
      .filter((tenant) => tenant.active)
      .forEach((tenant) => set.add(tenant.id));
    return set;
  };
  const groups = groupStore.groups;
  if (!groups.length) {
    return fallback();
  }
  if (!uiStore.activeGroupId) {
    const union = new Set<string>();
    groups.forEach((group) => {
      (group.tenantIds || []).forEach((id) => union.add(id));
    });
    return union.size ? union : fallback();
  }
  const group = groups.find((item) => item.id === uiStore.activeGroupId);
  if (!group) {
    return fallback();
  }
  const ids = new Set<string>();
  (group.tenantIds || []).forEach((id) => ids.add(id));
  return ids.size ? ids : fallback();
});

const filteredTenants = computed(() => {
  const ids = allowedTenantIds.value;
  if (!ids.size) {
    return tenantStore.tenants;
  }
  return tenantStore.tenants.filter((tenant) => ids.has(tenant.id));
});

const capacityText = computed(() => {
  const list = filteredTenants.value;
  if (!list.length) return '0.00 / 0.00 GB';
  const totalUsed = list.reduce((sum, item) => sum + item.used_gb, 0);
  const totalQuota = list.reduce((sum, item) => sum + item.quota_gb, 0);
  return `${totalUsed.toFixed(2)} / ${totalQuota.toFixed(2)} GB`;
});

const transferBadge = computed(() => transferStore.badgeCount);

onMounted(async () => {
  await securityStore.ensureServerKey();
  await tenantStore.fetchTenants();
  await groupStore.fetchGroups();
  await transferStore.initialize();
  await taskStore.initialize();
});
</script>

<style scoped>
.app-shell {
  display: grid;
  grid-template-columns: 240px 1fr;
  min-height: 100vh;
  background: #eaf2ff;
}

.sidebar {
  background: #f7fbff;
  color: #1f2a37;
  padding: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  justify-content: space-between;
  border-right: 1px solid #dfe8f6;
}

.sidebar-top h1 {
  margin: 0 0 1rem 0;
  font-size: 1.4rem;
  color: #1363df;
}

.sidebar nav {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.sidebar a {
  padding: 0.5rem 0.85rem;
  border-radius: 0.65rem;
  color: #1f2a37;
  font-weight: 600;
}

.sidebar a.router-link-active {
  background: #d5e9ff;
  color: #0b63f5;
}

.nav-badge {
  background: #ff4d4f;
  color: #fff;
  font-size: 0.7rem;
  padding: 0 0.35rem;
  border-radius: 999px;
  margin-left: 0.4rem;
  line-height: 1.2;
}

.sidebar-footer {
  border-top: 1px solid #e2eaf4;
  padding-top: 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.875rem;
  color: #4a6387;
}

main {
  padding: 0;
  background: #edf2fb;
}
</style>
