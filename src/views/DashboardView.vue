<template>
  <section class="page-shell">
    <div class="cards">
      <article>
        <h2>存储使用</h2>
        <p>{{ storageSummary }}</p>
      </article>
      <article>
        <h2>任务概览</h2>
        <p>{{ taskSummary }}</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useTenantStore } from '@/stores/tenantStore';
import { useTaskStore } from '@/stores/taskStore';

const tenantStore = useTenantStore();
const taskStore = useTaskStore();

const storageSummary = computed(() => {
  if (!tenantStore.tenants.length) return '尚未添加企业实例';
  return `${tenantStore.totalUsed.toFixed(2)} GB / ${tenantStore.totalQuota.toFixed(2)} GB`;
});

const taskSummary = computed(() => {
  return `进行中 ${taskStore.running} · 成功 ${taskStore.success} · 失败 ${taskStore.failed}`;
});
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

.cards {
  display: grid;
  gap: 1rem;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
}

article {
  background: white;
  padding: 1.5rem;
  border-radius: 1rem;
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
}
</style>
