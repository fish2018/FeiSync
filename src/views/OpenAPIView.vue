<template>
  <section class="api-shell">
    <header class="api-head">
      <div>
        <h2>API 配置</h2>
        <span>密钥管理与集成说明</span>
      </div>
    </header>
    <div v-if="toasts.length" class="toast-stack">
      <div v-for="toast in toasts" :key="toast.id" class="toast-item">
        {{ toast.text }}
      </div>
    </div>

    <article class="card">
      <header>
        <h3>分组 API Key</h3>
        <span>每个分组都会自动生成独立的 API Key，仅可访问分组内的企业实例</span>
      </header>
      <div v-if="groupLoading" class="empty">加载中...</div>
      <template v-else>
        <table v-if="groups.length" class="group-table">
          <thead>
            <tr>
              <th>分组</th>
              <th>企业实例</th>
              <th>备注</th>
              <th>API Key</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="group in groups" :key="group.id">
              <td>{{ group.name }}</td>
              <td>{{ formatMembers(group) }}</td>
              <td>{{ group.remark || '—' }}</td>
              <td>
                <code>{{ group.apiKey }}</code>
              </td>
              <td class="actions">
                <button class="btn secondary" type="button" @click="copyGroupKey(group)">复制</button>
                <button class="btn primary" type="button" @click="regenerateGroupKey(group)">重置</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty">暂无分组，请先在设置页面创建。</div>
      </template>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive } from 'vue';
import { storeToRefs } from 'pinia';
import { useGroupStore, type GroupRecord } from '@/stores/groupStore';
import { useTenantStore } from '@/stores/tenantStore';

const groupStore = useGroupStore();
const tenantStore = useTenantStore();
const { groups, loading: groupLoading } = storeToRefs(groupStore);
const { tenants } = storeToRefs(tenantStore);
const toasts = reactive<{ id: number; text: string }[]>([]);

const tenantNameMap = computed(() => {
  const map = new Map<string, string>();
  tenants.value.forEach((tenant) => map.set(tenant.id, tenant.name));
  return map;
});

const formatMembers = (group: GroupRecord) => {
  if (!group.tenantIds.length) return '未分配';
  return group.tenantIds.map((id) => tenantNameMap.value.get(id) ?? id).join('、');
};

const showToast = (text: string) => {
  const id = Date.now() + Math.random();
  toasts.push({ id, text });
  setTimeout(() => {
    const index = toasts.findIndex((item) => item.id === id);
    if (index >= 0) {
      toasts.splice(index, 1);
    }
  }, 1800);
};

const copyGroupKey = async (group: GroupRecord) => {
  try {
    await navigator.clipboard.writeText(group.apiKey);
    showToast('已复制 API Key');
  } catch {
    showToast('复制失败');
  }
};

const regenerateGroupKey = async (group: GroupRecord) => {
  await groupStore.regenerateKey(group.id);
  showToast('已生成新 Key');
};

onMounted(() => {
  groupStore.fetchGroups();
});
</script>

<style scoped>
.api-shell {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  background: #fff;
  min-height: 100%;
  padding: 1.75rem 1.5rem;
}

.api-head {
  border-bottom: 1px solid #e2eaf4;
  padding-bottom: 0.75rem;
}

.api-head span {
  color: #6b7b95;
  font-size: 0.9rem;
}

.card {
  background: #fff;
  border: 1px solid #e2eaf4;
  border-radius: 1rem;
  padding: 1.25rem;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.card header {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  margin-bottom: 1rem;
}

.card header span {
  color: #6b7b95;
  font-size: 0.9rem;
}

.group-table {
  width: 100%;
  border-collapse: collapse;
}

.group-table th,
.group-table td {
  padding: 0.6rem 0.75rem;
  border-bottom: 1px solid #edf1f7;
  text-align: left;
  vertical-align: top;
}

.group-table th {
  font-size: 0.9rem;
  color: #5b6987;
}

.actions {
  display: flex;
  gap: 0.5rem;
}

.card code {
  font-family: 'SFMono-Regular', Consolas, monospace;
  background: #f4f8ff;
  padding: 0.25rem 0.5rem;
  border-radius: 0.5rem;
  color: #0f172a;
}

.empty {
  padding: 1rem;
  color: #74809a;
}

.toast-stack {
  position: fixed;
  top: 1.5rem;
  right: 2rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  z-index: 1000;
}

.toast-item {
  background: #d1fae5;
  color: #065f46;
  padding: 0.55rem 1rem;
  border-radius: 0.45rem;
  font-size: 0.9rem;
  box-shadow: 0 10px 20px rgba(5, 122, 85, 0.2);
  border: 1px solid #a7f3d0;
}
</style>
