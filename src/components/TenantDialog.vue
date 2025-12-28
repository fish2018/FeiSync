<template>
  <div class="dialog-backdrop">
    <form class="dialog" @submit.prevent="save">
      <header>
        <h3>{{ isEdit ? '编辑企业' : '新增企业实例' }}</h3>
        <button type="button" @click="$emit('close')">×</button>
      </header>

      <label>
        名称
        <input v-model="form.name" required />
      </label>

      <label>
        app_id
        <input v-model="form.app_id" required />
      </label>

      <label>
        app_secret
        <input v-model="form.app_secret" required />
      </label>

      <label>
        容量(GB)
        <input type="number" v-model.number="form.quota_gb" min="1" />
      </label>

      <label>
        平台
        <select v-model="form.platform">
          <option value="lark">Lark 国际版</option>
          <option value="feishu">飞书中国版</option>
        </select>
      </label>

      <footer>
        <button class="btn secondary" type="button" @click="$emit('close')">取消</button>
        <button class="btn primary" type="submit">保存</button>
      </footer>
    </form>
  </div>
</template>

<script setup lang="ts">
import { reactive, watch, computed } from 'vue';
import { useTenantStore } from '@/stores/tenantStore';
import type { TenantInstance } from '@/stores/tenantStore';

const props = defineProps<{ tenant?: TenantInstance | null }>();
const emit = defineEmits(['close']);
const tenantStore = useTenantStore();

const form = reactive({
  name: '',
  app_id: '',
  app_secret: '',
  quota_gb: 100,
  platform: 'lark' as 'lark' | 'feishu'
});

const isEdit = computed(() => !!props.tenant);

const resetForm = () => {
  form.name = '';
  form.app_id = '';
  form.app_secret = '';
  form.quota_gb = 100;
  form.platform = 'lark';
};

watch(
  () => props.tenant,
  async (tenant) => {
    if (tenant) {
      const detail = await tenantStore.fetchTenantDetail(tenant.id);
      form.name = detail.name;
      form.app_id = detail.app_id;
      form.app_secret = detail.app_secret || '';
      form.quota_gb = detail.quota_gb;
      form.platform = detail.platform;
    } else {
      resetForm();
    }
  },
  { immediate: true }
);

const save = async () => {
  if (isEdit.value && props.tenant) {
    await tenantStore.updateTenant({
      tenant_id: props.tenant.id,
      name: form.name,
      app_id: form.app_id,
      app_secret: form.app_secret,
      quota_gb: form.quota_gb,
      platform: form.platform
    });
  } else {
    await tenantStore.addTenant({ ...form });
  }
  emit('close');
};
</script>

<style scoped>
.dialog-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.6);
  display: grid;
  place-items: center;
}

.dialog {
  background: white;
  border-radius: 1rem;
  padding: 1.5rem;
  width: min(480px, 90vw);
  display: flex;
  flex-direction: column;
  gap: 1rem;
  position: relative;
}

header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-right: 2.5rem;
}

.dialog header button {
  position: absolute;
  top: 1rem;
  right: 1rem;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: none;
  background: #eef2ff;
  color: #1f2a37;
  font-size: 1.1rem;
  cursor: pointer;
}

.dialog header button:hover {
  background: #d9e4ff;
}

label {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

input,
select {
  width: 100%;
  padding: 0.55rem 0.65rem;
  border-radius: 0.5rem;
  border: 1px solid #cbd5f5;
  min-height: 2.8rem;
  font-size: 0.95rem;
}

footer {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
  margin-top: 0.5rem;
}
</style>
