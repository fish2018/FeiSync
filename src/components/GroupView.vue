<template>
  <section class="group-shell">
    <header class="section-head">
      <div>
        <h2>企业分组</h2>
        <span class="caption">通过分组控制 API Key 可访问的企业实例范围</span>
      </div>
      <button class="btn primary" type="button" @click="openAddDialog">新增分组</button>
    </header>
    <article class="card primary-card">
      <table v-if="groups.length" class="tenant-table group-table">
        <thead>
          <tr>
            <th>名称</th>
            <th>备注</th>
            <th>企业实例</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="group in groups" :key="group.id">
            <td>{{ group.name }}</td>
            <td>{{ group.remark || '—' }}</td>
            <td>{{ formatMembers(group) }}</td>
            <td class="actions">
              <button class="btn secondary" type="button" @click="openEditDialog(group)">编辑</button>
              <button
                class="btn danger"
                type="button"
                @click="handleRemove(group)"
                :disabled="removingId === group.id"
              >
                {{ removingId === group.id ? '删除中...' : '删除' }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty">
        <p>当前暂无分组</p>
      </div>
    </article>

    <div v-if="dialogVisible" class="dialog-backdrop">
      <form class="dialog" @submit.prevent="saveGroup">
        <header class="dialog-head">
          <h3>{{ editing ? '编辑分组' : '新增分组' }}</h3>
          <button type="button" class="close-btn" @click="closeDialog">×</button>
        </header>
        <label>
          名称
          <input v-model="form.name" required />
        </label>
        <label>
          备注
          <textarea v-model="form.remark" rows="2" placeholder="可选"></textarea>
        </label>
        <fieldset>
          <legend>企业实例</legend>
          <div class="tenant-checkboxes">
            <label v-for="tenant in tenantOptions" :key="tenant.id" class="tenant-option">
              <input type="checkbox" :value="tenant.id" v-model="form.tenantIds" />
              <span>{{ tenant.name }}</span>
            </label>
            <p v-if="!tenantOptions.length" class="hint">暂无企业实例，可先在上方列表中创建。</p>
          </div>
        </fieldset>
        <footer>
          <button class="btn secondary" type="button" @click="closeDialog">取消</button>
          <button class="btn primary" type="submit" :disabled="saving">{{ saving ? '保存中...' : '保存' }}</button>
        </footer>
      </form>
    </div>
    <div v-if="confirmState.visible" class="dialog-backdrop">
      <div class="dialog confirm-dialog">
        <p>{{ confirmState.message }}</p>
        <div class="dialog-actions">
          <button class="btn secondary" type="button" @click="finishConfirm(false)">取消</button>
          <button class="btn danger" type="button" @click="finishConfirm(true)">确认</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useGroupStore, type GroupRecord } from '@/stores/groupStore';
import { useTenantStore, type TenantInstance } from '@/stores/tenantStore';

const groupStore = useGroupStore();
const tenantStore = useTenantStore();
const { groups } = storeToRefs(groupStore);
const { tenants } = storeToRefs(tenantStore);

const dialogVisible = ref(false);
const editing = ref<GroupRecord | null>(null);
const saving = ref(false);
const removingId = ref<string | null>(null);
const confirmState = reactive({
  visible: false,
  message: '',
  resolve: null as null | ((accepted: boolean) => void)
});
const form = reactive({
  name: '',
  remark: '',
  tenantIds: [] as string[]
});

const tenantOptions = computed(() => tenants.value.map((tenant: TenantInstance) => ({ id: tenant.id, name: tenant.name })));
const tenantNameMap = computed(() => {
  const map = new Map<string, string>();
  tenantOptions.value.forEach((item) => map.set(item.id, item.name));
  return map;
});

const resetForm = () => {
  form.name = editing.value?.name ?? '';
  form.remark = editing.value?.remark ?? '';
  form.tenantIds = editing.value ? [...editing.value.tenantIds] : [];
};

const openAddDialog = () => {
  editing.value = null;
  resetForm();
  dialogVisible.value = true;
};

const openEditDialog = (group: GroupRecord) => {
  editing.value = group;
  resetForm();
  dialogVisible.value = true;
};

const closeDialog = () => {
  dialogVisible.value = false;
};

const ensureGroupPayload = () => ({
  name: form.name.trim(),
  remark: form.remark.trim(),
  tenantIds: [...form.tenantIds]
});

const saveGroup = async () => {
  if (saving.value) return;
  if (!form.name.trim()) {
    window.alert('请输入分组名称');
    return;
  }
  saving.value = true;
  try {
    console.debug('[group] save submit', {
      mode: editing.value ? 'edit' : 'create',
      tenantIds: form.tenantIds.length
    });
    if (editing.value) {
      await groupStore.updateGroup({
        id: editing.value.id,
        ...ensureGroupPayload()
      });
    } else {
      await groupStore.addGroup(ensureGroupPayload());
    }
    console.debug('[group] save success');
    dialogVisible.value = false;
    resetForm();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    window.alert(message || '保存分组失败');
    console.error('[group] save failed', error);
  } finally {
    saving.value = false;
  }
};

const openConfirm = (message: string) => {
  confirmState.visible = true;
  confirmState.message = message;
  return new Promise<boolean>((resolve) => {
    confirmState.resolve = resolve;
  });
};

const finishConfirm = (accepted: boolean) => {
  confirmState.visible = false;
  const resolver = confirmState.resolve;
  confirmState.resolve = null;
  resolver?.(accepted);
};

const handleRemove = async (group: GroupRecord) => {
  if (removingId.value) return;
  const confirmed = await openConfirm(`确认删除分组「${group.name}」吗？`);
  if (!confirmed) return;
  removingId.value = group.id;
  try {
    console.debug('[group] remove start', group.id);
    await groupStore.removeGroup(group.id);
    console.debug('[group] remove success', group.id);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[group] remove failed', error);
    window.alert(message || '删除分组失败');
  } finally {
    removingId.value = null;
  }
};

const formatMembers = (group: GroupRecord) => {
  if (!group.tenantIds.length) return '未分配';
  return group.tenantIds
    .map((id) => tenantNameMap.value.get(id) ?? id)
    .join('、');
};

onMounted(() => {
  groupStore.fetchGroups();
});
</script>

<style scoped>
.group-shell {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.section-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}


.section-head .caption {
  color: #6b7b95;
  font-size: 0.9rem;
}

.primary-card {
  background: #fff;
  border: 1px solid #e2eaf4;
  border-radius: 1rem;
  overflow: hidden;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.group-table {
  width: 100%;
  border-collapse: collapse;
}

.group-table th,
.group-table td {
  padding: 0.85rem 1rem;
  border-bottom: 1px solid #edf1f7;
  text-align: left;
}

.group-table th {
  background: #f5f7fb;
  font-weight: 600;
  color: #4a5b75;
}

.group-table tbody tr:hover {
  background: #f8fbff;
}

.group-table th:nth-child(3) {
  width: 40%;
}

.group-table th:nth-child(4),
.group-table td:nth-child(4) {
  width: 20%;
}

.actions {
  display: flex;
  gap: 0.5rem;
}

.dialog-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 20;
}

.dialog {
  background: #fff;
  padding: 1.5rem;
  border-radius: 1rem;
  width: min(520px, 90vw);
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  position: relative;
}

.dialog-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-right: 2.5rem;
}

.close-btn {
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

.close-btn:hover {
  background: #d9e4ff;
}

.dialog label,
fieldset {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  font-size: 0.95rem;
  color: #334155;
}

textarea,
input {
  border: 1px solid #d7deec;
  border-radius: 0.5rem;
  padding: 0.45rem 0.65rem;
}

fieldset {
  border: 1px solid #e4e8f3;
  border-radius: 0.75rem;
  padding: 0.75rem;
}

.tenant-checkboxes {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}

.tenant-option {
  display: inline-flex !important;
  align-items: center !important;
  gap: 0.45rem !important;
  font-weight: normal;
  flex-direction: row !important;
  line-height: 1.4;
}

.tenant-option input {
  width: auto !important;
  min-height: auto !important;
  height: auto !important;
}

.hint {
  font-size: 0.85rem;
  color: #94a3b8;
}

.dialog footer {
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
}

.empty {
  padding: 2rem;
  text-align: center;
  color: #8b9bb2;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  background: #fff;
}

.confirm-dialog {
  max-width: 360px;
  text-align: center;
}

.confirm-dialog .dialog-actions {
  justify-content: center;
}
</style>
