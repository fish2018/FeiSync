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
        <h3>服务配置</h3>
        <span>在本地暴露 HTTP API，支持外部系统按分组权限访问。</span>
      </header>
      <div class="service-head">
        <div class="status">
          <span class="status-dot" :class="{ running: serverStatus?.running }"></span>
          <div>
            <strong>{{ serverStatus?.running ? '服务运行中' : '服务已停止' }}</strong>
            <p>
              <span v-if="serverStatus?.running && serverStatus?.address">当前监听：{{ serverStatus?.address }}</span>
              <span v-else>默认监听 0.0.0.0，启动前可调整端口和超时。</span>
            </p>
          </div>
        </div>
        <button class="btn primary" type="button" @click="toggleService" :disabled="serviceActionLoading || serviceLoading">
          <span v-if="serviceActionLoading" class="spinner small"></span>
          <span>{{ serverStatus?.running ? '停止服务' : '启动服务' }}</span>
        </button>
      </div>
      <form class="api-form" @submit.prevent="saveConfig">
        <div class="form-grid">
          <label>
            监听端口
            <input type="number" v-model.number="form.port" min="1024" max="65535" required />
            <small>默认 6688，修改后保存并重新启动服务生效。</small>
          </label>
          <label>
            请求超时（秒）
            <input type="number" v-model.number="form.timeoutSecs" min="30" max="600" required />
            <small>范围 30-600 秒，默认 120 秒。</small>
          </label>
          <label>
            监听地址
            <input type="text" value="0.0.0.0（固定）" disabled />
            <small>服务总是监听 0.0.0.0，允许局域网访问。</small>
          </label>
        </div>
        <div class="form-actions">
          <button class="btn secondary" type="button" @click="resetForm" :disabled="serviceLoading || saving">还原配置</button>
          <button class="btn primary" type="submit" :disabled="saving || serviceLoading">
            <span v-if="saving" class="spinner small"></span>
            <span>{{ saving ? '保存中...' : '保存配置' }}</span>
          </button>
        </div>
      </form>
    </article>

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

    <article class="card">
      <header>
        <h3>API 文档</h3>
        <span>全部操作通过 POST /command/{name} 暴露，服务随应用启动自动运行，需在 Header 中携带 X-API-Key。</span>
      </header>
      <div v-if="docsLoading" class="empty">文档加载中...</div>
      <table v-else class="docs-table">
        <thead>
          <tr>
            <th>命令</th>
            <th>方法</th>
            <th>路径</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="route in docs" :key="route.command">
            <tr
              class="doc-row"
              :class="{ expanded: isExpanded(route.command) }"
              @click="toggleDoc(route.command)"
            >
              <td><code>{{ route.command }}</code></td>
              <td>{{ route.method }}</td>
              <td>{{ route.path }}</td>
              <td>{{ route.description }}</td>
            </tr>
            <tr v-if="isExpanded(route.command)" class="doc-detail-row">
              <td :colspan="4">
                <div class="doc-detail">
                  <section>
                    <h4>请求结构</h4>
                    <pre>{{ route.payload }}</pre>
                    <p class="note" v-if="route.notes">{{ route.notes }}</p>
                    <table v-if="route.payload_fields.length" class="field-table">
                      <thead>
                        <tr>
                          <th>字段</th>
                          <th>类型</th>
                          <th>必填</th>
                          <th>说明</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="field in route.payload_fields" :key="field.name">
                          <td>{{ field.name }}</td>
                          <td>{{ field.typ }}</td>
                          <td>{{ field.required ? '是' : '否' }}</td>
                          <td>{{ field.description }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </section>
                  <section>
                    <h4>返回示例</h4>
                    <pre>{{ route.response }}</pre>
                    <table v-if="route.response_fields.length" class="field-table">
                      <thead>
                        <tr>
                          <th>字段</th>
                          <th>类型</th>
                          <th>必填</th>
                          <th>说明</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr v-for="field in route.response_fields" :key="field.name">
                          <td>{{ field.name }}</td>
                          <td>{{ field.typ }}</td>
                          <td>{{ field.required ? '是' : '否' }}</td>
                          <td>{{ field.description }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </section>
                </div>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useGroupStore, type GroupRecord } from '@/stores/groupStore';
import { useTenantStore } from '@/stores/tenantStore';
import { invoke } from '@tauri-apps/api/core';

const groupStore = useGroupStore();
const tenantStore = useTenantStore();
const { groups, loading: groupLoading } = storeToRefs(groupStore);
const { tenants } = storeToRefs(tenantStore);
const toasts = reactive<{ id: number; text: string }[]>([]);

interface ApiServerConfigState {
  listen_host: string;
  port: number;
  timeout_secs: number;
}

interface ApiServerStatus {
  running: boolean;
  address: string | null;
  config: ApiServerConfigState;
}

interface ApiFieldDoc {
  name: string;
  typ: string;
  required: boolean;
  description: string;
}

interface ApiRouteDoc {
  command: string;
  method: string;
  path: string;
  description: string;
  payload: string;
  response: string;
  notes?: string;
  payload_fields: ApiFieldDoc[];
  response_fields: ApiFieldDoc[];
}

const serverStatus = ref<ApiServerStatus | null>(null);
const serviceLoading = ref(false);
const serviceActionLoading = ref(false);
const saving = ref(false);
const docs = ref<ApiRouteDoc[]>([]);
const docsLoading = ref(false);
const expandedRows = ref(new Set<string>());

const form = reactive({
  port: 6688,
  timeoutSecs: 120
});

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

const syncFormFromStatus = (status: ApiServerStatus | null) => {
  if (!status) return;
  form.port = status.config.port;
  form.timeoutSecs = status.config.timeout_secs;
};

const fetchServerStatus = async () => {
  serviceLoading.value = true;
  try {
    const status = await invoke<ApiServerStatus>('get_api_service_config');
    serverStatus.value = status;
    syncFormFromStatus(status);
  } finally {
    serviceLoading.value = false;
  }
};

const buildConfigPayload = () => ({
  listen_host: '0.0.0.0',
  port: form.port,
  timeout_secs: form.timeoutSecs
});

const saveConfig = async () => {
  saving.value = true;
  try {
    const payload = buildConfigPayload();
    const status = await invoke<ApiServerStatus>('update_api_service_config', { payload });
    serverStatus.value = status;
    syncFormFromStatus(status);
    showToast('配置已保存');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showToast(message || '保存失败');
  } finally {
    saving.value = false;
  }
};

const resetForm = () => syncFormFromStatus(serverStatus.value);

const toggleService = async () => {
  if (!serverStatus.value || serviceActionLoading.value) return;
  serviceActionLoading.value = true;
  try {
    let status: ApiServerStatus;
    if (serverStatus.value.running) {
      status = await invoke<ApiServerStatus>('stop_api_service');
    } else {
      status = await invoke<ApiServerStatus>('start_api_service');
    }
    serverStatus.value = status;
    syncFormFromStatus(status);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    showToast(message || '操作失败');
  } finally {
    serviceActionLoading.value = false;
  }
};

const fetchDocs = async () => {
  docsLoading.value = true;
  try {
    docs.value = await invoke<ApiRouteDoc[]>('list_api_routes');
    expandedRows.value = new Set<string>();
  } finally {
    docsLoading.value = false;
  }
};

const toggleDoc = (command: string) => {
  const next = new Set(expandedRows.value);
  if (next.has(command)) {
    next.delete(command);
  } else {
    next.add(command);
  }
  expandedRows.value = next;
};

const isExpanded = (command: string) => expandedRows.value.has(command);

onMounted(() => {
  groupStore.fetchGroups();
  fetchServerStatus();
  fetchDocs();
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

.service-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  margin-bottom: 1rem;
}
.service-head .status {
  display: flex;
  gap: 0.75rem;
  align-items: center;
}
.status-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #cbd5f5;
  display: inline-flex;
}
.status-dot.running {
  background: #34d399;
}
.api-form {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 1rem;
}
.api-form label {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}
.api-form input,
.api-form select {
  border: 1px solid #cbd5f5;
  border-radius: 0.65rem;
  padding: 0.5rem 0.65rem;
}
.api-form small {
  color: #7b8ba6;
}
.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
}

.empty {
  padding: 1rem;
  color: #74809a;
}

.docs-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 0.5rem;
}
.docs-table th,
.docs-table td {
  padding: 0.55rem 0.6rem;
  border-bottom: 1px solid #edf1f7;
  text-align: left;
}
.docs-table code {
  background: #eef2ff;
  border-radius: 0.4rem;
  padding: 0 0.4rem;
}
.doc-row {
  cursor: pointer;
  transition: background 0.2s ease;
}
.doc-row:hover {
  background: #f5f8ff;
}
.doc-row.expanded {
  background: #eef4ff;
}
.doc-detail-row td {
  padding: 0;
  border-bottom: none;
}
.doc-detail {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 0.75rem;
  padding: 0.75rem 0.9rem 1rem;
  background: #f9fbff;
  border-top: 1px solid #dfe7fb;
}
.doc-detail section {
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
}
.doc-detail h4 {
  margin: 0;
  font-size: 0.9rem;
  color: #1f2c46;
}
.doc-detail pre {
  margin: 0;
  background: #fff;
  border: 1px solid #e2eaf4;
  border-radius: 0.5rem;
  padding: 0.5rem 0.6rem;
  font-size: 0.78rem;
  white-space: pre-wrap;
  word-break: break-all;
}
.field-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 0.45rem;
}
.field-table th,
.field-table td {
  border: 1px solid #e2eaf4;
  padding: 0.35rem 0.45rem;
  text-align: left;
  font-size: 0.85rem;
}
.field-table th {
  background: #f1f5ff;
  color: #1f2c46;
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
