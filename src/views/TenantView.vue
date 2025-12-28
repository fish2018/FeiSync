<template>
  <section class="tenant-shell">
    <header class="section-head">
      <div>
        <h2>企业实例</h2>
        <span class="caption">集中管理 app_id / app_secret，按序号自动接管空间</span>
      </div>
      <button class="btn primary" @click="openAddDialog">新增实例</button>
    </header>

    <article class="card primary-card">
      <table v-if="orderedTenants.length" class="tenant-table">
        <thead>
          <tr>
            <th>序号</th>
            <th>名称</th>
            <th>客户端 ID</th>
            <th>平台</th>
            <th>容量</th>
            <th>启用</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="tenant in orderedTenants"
            :key="tenant.id"
            :data-tenant-id="tenant.id"
            :class="{ dragging: draggingId === tenant.id, 'drop-hover': hoverId === tenant.id }"
            @pointerdown="onRowPointerDown($event, tenant)"
          >
            <td class="order-cell">
              <span class="drag-handle">☰</span>
              {{ tenant.order }}
            </td>
            <td>{{ tenant.name }}</td>
            <td>{{ tenant.app_id }}</td>
            <td>{{ platformLabel(tenant.platform) }}</td>
            <td>{{ tenant.used_gb.toFixed(2) }} / {{ tenant.quota_gb }} GB</td>
            <td>
              <label class="switch">
                <input
                  type="checkbox"
                  :checked="tenant.active"
                  @change="toggleActive(tenant, ($event.target as HTMLInputElement)?.checked ?? false)"
                />
                <span></span>
              </label>
            </td>
            <td class="tenant-actions">
              <button class="btn secondary" type="button" @click="openEditDialog(tenant)">编辑</button>
              <button class="btn danger" type="button" @click="removeTenant(tenant)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty">
        <p>当前暂无企业实例</p>
      </div>
    </article>

    <TenantDialog v-if="dialogVisible" :tenant="editingTenant" @close="closeDialog" />
    <div
      v-if="dragPreview.visible && dragPreview.tenant"
      class="tenant-drag-ghost"
      :style="{
        width: `${dragPreview.width}px`,
        left: `${dragPreview.x}px`,
        top: `${dragPreview.y}px`
      }"
    >
      <div class="ghost-row">
        <span class="order-cell">
          <span class="drag-handle">☰</span>
          {{ dragPreview.tenant.order }}
        </span>
        <span>{{ dragPreview.tenant.name }}</span>
        <span>{{ dragPreview.tenant.app_id }}</span>
        <span>{{ platformLabel(dragPreview.tenant.platform) }}</span>
        <span>{{ dragPreview.tenant.used_gb.toFixed(2) }} / {{ dragPreview.tenant.quota_gb }} GB</span>
        <span>{{ dragPreview.tenant.active ? '启用' : '停用' }}</span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useTenantStore } from '@/stores/tenantStore';
import type { TenantInstance, TenantPlatform } from '@/stores/tenantStore';
import TenantDialog from '@/components/TenantDialog.vue';

const tenantStore = useTenantStore();
const { tenants } = storeToRefs(tenantStore);
const dialogVisible = ref(false);
const editingTenant = ref<TenantInstance | null>(null);
const draggingId = ref<string | null>(null);
const hoverId = ref<string | null>(null);
const hoverPlacement = ref<'before' | 'after'>('before');
const reorderState = reactive({
  pointerId: null as number | null,
  startY: 0,
  active: false,
  rowElement: null as HTMLElement | null
});
const dragPreview = reactive({
  visible: false,
  width: 0,
  x: 0,
  y: 0,
  lockX: 0,
  offsetY: 0,
  tenant: null as (TenantInstance & { order: number }) | null
});

const platformLabel = (value: TenantPlatform) => (value === 'feishu' ? '飞书中国版' : 'Lark 国际版');
const orderedTenants = computed(() => [...tenants.value].sort((a, b) => a.order - b.order));

const openAddDialog = () => {
  editingTenant.value = null;
  dialogVisible.value = true;
};

const openEditDialog = (tenant: TenantInstance) => {
  editingTenant.value = tenant;
  dialogVisible.value = true;
};

const closeDialog = () => {
  dialogVisible.value = false;
};

const removeTenant = async (tenant: TenantInstance) => {
  try {
    await tenantStore.removeTenant(tenant.id);
  } catch (err) {
    console.error(`${new Date().toISOString()} tenant.removeTenant 删除失败`, err);
  }
};

const toggleActive = async (tenant: TenantInstance, active: boolean) => {
  await tenantStore.updateTenant({
    tenant_id: tenant.id,
    active
  });
};

const applyReorder = async (sourceId: string, targetId: string, placeBefore: boolean) => {
  if (sourceId === targetId) return;
  const list = [...orderedTenants.value];
  const from = list.findIndex((item) => item.id === sourceId);
  if (from < 0) return;
  const [moved] = list.splice(from, 1);
  let insertIndex = list.findIndex((item) => item.id === targetId);
  if (insertIndex < 0) return;
  if (!placeBefore) {
    insertIndex += 1;
  }
  if (insertIndex < 0) insertIndex = 0;
  if (insertIndex > list.length) insertIndex = list.length;
  list.splice(insertIndex, 0, moved);
  const payload = list.map((item, index) => ({
    tenant_id: item.id,
    order: index + 1
  }));
  await tenantStore.reorder(payload);
};

const cleanupPointerListeners = () => {
  window.removeEventListener('pointermove', onRowPointerMove, true);
  window.removeEventListener('pointerup', onRowPointerUp, true);
  window.removeEventListener('pointercancel', cancelRowPointer, true);
};

const finishReorder = async (cancel = false) => {
  cleanupPointerListeners();
  const sourceId = draggingId.value;
  const targetId = hoverId.value;
  const placeBefore = hoverPlacement.value === 'before';
  reorderState.pointerId = null;
  reorderState.active = false;
  reorderState.rowElement = null;
  draggingId.value = null;
  hoverId.value = null;
  hoverPlacement.value = 'before';
  dragPreview.visible = false;
  dragPreview.tenant = null;
  if (cancel || !sourceId || !targetId) return;
  await applyReorder(sourceId, targetId, placeBefore);
};

const onRowPointerDown = (event: PointerEvent, tenant: TenantInstance) => {
  if (event.button !== 0) return;
  const target = event.target as HTMLElement | null;
  if (target?.closest('button, input, label, select, textarea, a')) {
    return;
  }
  event.preventDefault();
  if (reorderState.pointerId !== null) {
    void finishReorder(true);
  }
  reorderState.pointerId = event.pointerId;
  reorderState.startY = event.clientY;
  reorderState.active = false;
  reorderState.rowElement = event.currentTarget as HTMLElement | null;
  draggingId.value = tenant.id;
  hoverId.value = null;
  hoverPlacement.value = 'before';
  const rect = reorderState.rowElement?.getBoundingClientRect();
  if (rect) {
    const tableRect = (reorderState.rowElement?.closest('table') as HTMLElement | null)?.getBoundingClientRect();
    dragPreview.visible = true;
    dragPreview.width = tableRect?.width ?? rect.width;
    dragPreview.lockX = tableRect?.left ?? rect.left;
    dragPreview.offsetY = event.clientY - rect.top;
    dragPreview.x = dragPreview.lockX;
    dragPreview.y = rect.top;
    dragPreview.tenant = tenant;
  }
  window.addEventListener('pointermove', onRowPointerMove, true);
  window.addEventListener('pointerup', onRowPointerUp, true);
  window.addEventListener('pointercancel', cancelRowPointer, true);
};

const onRowPointerMove = (event: PointerEvent) => {
  if (event.pointerId !== reorderState.pointerId) return;
  const dy = Math.abs(event.clientY - reorderState.startY);
  if (!reorderState.active && dy > 5) {
    reorderState.active = true;
  }
  if (!reorderState.active) return;
  dragPreview.x = dragPreview.lockX;
  dragPreview.y = event.clientY - dragPreview.offsetY;
  const targetRow = (document.elementFromPoint(event.clientX, event.clientY) as HTMLElement | null)?.closest(
    'tr[data-tenant-id]'
  ) as HTMLElement | null;
  if (!targetRow) {
    hoverId.value = null;
    return;
  }
  const targetId = targetRow.dataset.tenantId || '';
  if (!targetId || targetId === draggingId.value) {
    hoverId.value = null;
    return;
  }
  const rect = targetRow.getBoundingClientRect();
  const before = event.clientY < rect.top + rect.height / 2;
  hoverPlacement.value = before ? 'before' : 'after';
  hoverId.value = targetId;
};

const onRowPointerUp = (event: PointerEvent) => {
  if (event.pointerId !== reorderState.pointerId) return;
  void finishReorder();
};

const cancelRowPointer = () => {
  void finishReorder(true);
};

onBeforeUnmount(() => {
  cleanupPointerListeners();
});
</script>

<style scoped>
.tenant-shell {
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
  font-size: 0.9rem;
  color: #6b7b95;
}

.card {
  background: #fff;
  border: 1px solid #e2eaf4;
  border-radius: 1rem;
  padding: 1.25rem;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.primary-card {
  padding: 0;
}

.tenant-table {
  width: 100%;
  border-collapse: collapse;
}

.tenant-table th,
.tenant-table td {
  padding: 0.85rem 1rem;
  border-bottom: 1px solid #edf1f7;
}

.tenant-table th {
  background: #f5f7fb;
  text-align: left;
  font-weight: 600;
  color: #4a5b75;
}

.tenant-table tbody tr:hover {
  background: #f8fbff;
}

.tenant-table tr.dragging {
  opacity: 0.6;
}

.tenant-table tr.drop-hover td {
  background: #eef5ff;
  border-bottom-color: #c3d8ff;
}

.order-cell {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.drag-handle {
  cursor: grab;
  color: #94a3b8;
}

.tenant-actions {
  display: flex;
  gap: 0.5rem;
}

.empty {
  padding: 2rem;
  text-align: center;
  color: #8b9bb2;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.switch {
  position: relative;
  width: 46px;
  height: 24px;
  display: inline-flex;
  align-items: center;
}

.switch input {
  opacity: 0;
  width: 0;
  height: 0;
}

.switch span {
  position: absolute;
  inset: 0;
  background: #d1d9ec;
  border-radius: 999px;
  transition: background 0.2s ease;
}

.switch span::after {
  content: '';
  position: absolute;
  width: 18px;
  height: 18px;
  border-radius: 50%;
  background: #fff;
  top: 3px;
  left: 4px;
  transition: transform 0.2s ease;
  box-shadow: 0 4px 8px rgba(15, 23, 42, 0.15);
}

.switch input:checked + span {
  background: #1e7bff;
}

.switch input:checked + span::after {
  transform: translateX(20px);
}

.tenant-drag-ghost {
  position: fixed;
  pointer-events: none;
  z-index: 2000;
  background: #fff;
  border: 1px solid #c3d8ff;
  border-radius: 0.5rem;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.18);
  opacity: 0.95;
  padding: 0.5rem 1rem;
  display: flex;
  align-items: center;
  gap: 1rem;
}

.tenant-drag-ghost .ghost-row {
  display: grid;
  grid-template-columns: 1.2fr 2fr 2fr 1.5fr 2fr 1fr;
  gap: 0.5rem;
  align-items: center;
}
</style>
