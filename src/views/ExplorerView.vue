<template>
  <section class="page-shell">
    <header class="section-head">
      <div>
        <h2>云盘浏览器</h2>
        <p>调用官方 API 列出文件夹/文件，调用方式不变。</p>
      </div>
      <div class="mode-switch">
        <label>
          <input type="checkbox" v-model="manualMode" />
          按企业查看
        </label>
        <select v-model="selectedTenantId" :disabled="!manualMode">
          <option disabled value="">选择企业实例</option>
          <option v-for="tenant in tenants" :key="tenant.id" :value="tenant.id">
            {{ tenant.name }}
          </option>
        </select>
      </div>
    </header>

    <div class="explorer">
      <div class="path">{{ currentFolder || 'Root' }}</div>
      <div class="list">
        <article v-for="item in entries" :key="item.token" class="entry">
          <div class="info" @click="open(item)">
            <strong>{{ item.name }}</strong>
            <span class="type">{{ item.type }}</span>
          </div>
          <button class="btn danger" @click.stop="deleteEntry(item)">删除</button>
        </article>
      </div>
    </div>
  </section>
  </section>
</template>

<script setup lang="ts">
import { ref, watch, computed } from 'vue';
import { storeToRefs } from 'pinia';
import { useTenantStore } from '@/stores/tenantStore';
import { useExplorerStore } from '@/stores/explorerStore';
import { useSecurityStore } from '@/stores/securityStore';
import { invoke } from '@tauri-apps/api/core';

const tenantStore = useTenantStore();
const explorerStore = useExplorerStore();
const securityStore = useSecurityStore();
const { tenants } = storeToRefs(tenantStore);

const manualMode = ref(false);
const selectedTenantId = ref('');
const currentFolder = computed(() => explorerStore.currentFolderToken);
const entries = computed(() => explorerStore.entries);

watch(
  () => tenants.value,
  async (list) => {
    if (!list.length) return;
    if (!selectedTenantId.value) {
      selectedTenantId.value = list[0].id;
    }
  },
  { immediate: true }
);

watch(
  [manualMode, selectedTenantId, () => tenants.value],
  async ([mode, tenantId, tenantList]) => {
    if (!tenantList.length) return;
    if (!mode) {
      await explorerStore.loadRoot();
      return;
    }
    if (mode && tenantId) {
      await explorerStore.loadRoot(tenantId);
    }
  },
  { immediate: true }
);

const open = async (item: any) => {
  if (item.type === 'folder') {
    await explorerStore.loadFolder(item.token);
  }
};

const deleteEntry = async (item: any) => {
  const apiKey = await securityStore.ensureServerKey();
  await invoke('delete_file', {
    api_key: apiKey,
    payload: { token: item.token, type: item.type }
  });
  await explorerStore.reloadCurrent();
};
</script>

<style scoped>
.page-shell {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  background: #fff;
  min-height: 100%;
  padding: 1.75rem 1.5rem;
}

.explorer {
  background: white;
  border-radius: 1rem;
  padding: 1rem;
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
}

.mode-switch {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.mode-switch select:disabled {
  opacity: 0.5;
}

.path {
  font-size: 0.875rem;
  color: #6366f1;
  margin-bottom: 1rem;
}

.list {
  display: grid;
  gap: 0.5rem;
}

.entry {
  padding: 0.75rem;
  border: 1px solid #e2e8f0;
  border-radius: 0.5rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.5rem;
}

.entry:hover {
  border-color: #818cf8;
}

.entry .info {
  flex: 1;
  display: flex;
  flex-direction: column;
  cursor: pointer;
}

.type {
  color: #94a3b8;
}

.btn.danger {
  background: #dc2626;
  color: #fff;
}
</style>
