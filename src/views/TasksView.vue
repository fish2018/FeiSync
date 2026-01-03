<template>
  <section class="tasks-shell">
    <header class="tasks-head">
      <div>
        <h2>任务中心</h2>
        <p>以企业分组为单位配置单/双向同步任务，Cron 定时，云端目录可视化选择。</p>
      </div>
      <div class="head-actions">
        <button class="btn secondary" type="button" @click="refreshSchedules" :disabled="!taskStore.tasks.length">
          重算下一次执行
        </button>
        <button class="btn primary" type="button" @click="openCreateDialog">新建任务</button>
      </div>
    </header>

    <section class="task-metrics">
      <article>
        <span class="metric-label">已配置</span>
        <strong>{{ taskStore.total }}</strong>
      </article>
      <article>
        <span class="metric-label">运行中</span>
        <strong>{{ taskStore.running }}</strong>
      </article>
      <article>
        <span class="metric-label">成功</span>
        <strong>{{ taskStore.success }}</strong>
      </article>
      <article>
        <span class="metric-label">失败</span>
        <strong>{{ taskStore.failed }}</strong>
      </article>
      <article>
        <span class="metric-label">最近计划</span>
        <strong>{{ upcomingText }}</strong>
        <small v-if="upcomingTask">任务：{{ upcomingTask.name }}</small>
        <small v-else>暂无排程</small>
      </article>
    </section>

    <section class="task-filters">
      <div class="filter">
        <label>同步方向</label>
        <select v-model="directionFilter">
          <option value="all">全部</option>
          <option value="cloud_to_local">云盘 → 本地</option>
          <option value="local_to_cloud">本地 → 云盘</option>
          <option value="bidirectional">双向同步</option>
        </select>
      </div>
      <div class="filter">
        <label>状态</label>
        <select v-model="statusFilter">
          <option value="all">全部</option>
          <option value="idle">待运行</option>
          <option value="scheduled">已排程</option>
          <option value="running">执行中</option>
          <option value="success">上次成功</option>
          <option value="failed">上次失败</option>
        </select>
      </div>
      <div class="filter search-filter">
        <label>关键字</label>
        <input v-model="keyword" placeholder="按名称/目录/备注搜索" />
      </div>
      <button class="btn secondary" type="button" @click="resetFilters" :disabled="!hasFilter">重置筛选</button>
    </section>

    <section v-if="filteredTasks.length" class="task-list">
      <article
        v-for="task in filteredTasks"
        :key="task.id"
        class="task-card"
        :class="{ disabled: !task.enabled }"
      >
        <header class="task-card-head">
          <div>
            <h3>{{ task.name }}</h3>
            <div class="task-tags">
              <span class="chip direction">{{ directionLabel(task.direction) }}</span>
              <span class="chip subtle" :title="detectionTip(task.detection)">{{ detectionLabel(task.detection) }}</span>
              <span class="chip subtle">{{ conflictLabel(task.conflict) }}</span>
            </div>
          </div>
          <div class="task-head-actions">
            <div
              v-if="isTaskRunning(task)"
              class="task-run-indicator"
              :title="runIndicatorLabel(task)"
            >
              <span class="run-icon">
                <span class="run-arrow" :class="directionIconClass(task)"></span>
              </span>
            </div>
            <span class="status-chip" :class="statusClass(task)">{{ statusLabel(task) }}</span>
            <label class="switch small">
              <input type="checkbox" :checked="task.enabled" @change="toggleTask(task, $event)" />
              <span></span>
            </label>
            <button class="btn secondary" type="button" @click="queueTask(task)">立即执行</button>
            <button class="btn secondary" type="button" @click="duplicateTask(task)">复制</button>
            <button class="btn secondary" type="button" @click="openEditDialog(task)">编辑</button>
            <button class="btn danger" type="button" @click="removeTask(task)">删除</button>
          </div>
        </header>
        <div class="task-body">
          <div class="col">
            <span class="label">企业分组</span>
            <p>{{ task.groupName || '未指定' }}</p>
            <span class="label">云端租户</span>
            <p>{{ task.tenantName || task.tenantId }}</p>
            <span class="label">云端目录</span>
            <p class="mono">{{ task.remoteLabel }} ({{ task.remoteFolderToken }})</p>
            <span class="label">本地目录</span>
            <p class="mono">{{ task.localPath }}</p>
          </div>
          <div class="col">
            <span class="label">调度</span>
            <p class="mono">{{ task.schedule }}</p>
            <small>{{ taskStore.cronDescription(task.schedule) }}</small>
            <small>下次：{{ formatDate(task.nextRunAt) }}</small>
            <span class="label">最近运行</span>
            <p>{{ formatDate(task.lastRunAt) }}</p>
            <small v-if="task.lastMessage">{{ task.lastMessage }}</small>
          </div>
          <div class="col">
            <span class="label">包含规则</span>
            <div class="pattern-chips">
              <span v-for="pattern in task.includePatterns" :key="pattern" class="chip include">{{ pattern }}</span>
              <span v-if="!task.includePatterns.length" class="muted">全部文件</span>
            </div>
            <span class="label">排除规则</span>
            <div class="pattern-chips">
              <span v-for="pattern in task.excludePatterns" :key="pattern" class="chip exclude">{{ pattern }}</span>
              <span v-if="!task.excludePatterns.length" class="muted">未设置</span>
            </div>
            <span class="label" v-if="task.notes">备注</span>
            <p v-if="task.notes">{{ task.notes }}</p>
          </div>
        </div>
      </article>
    </section>
    <p v-else class="empty">暂无任务，点击“新建任务”根据企业分组挑选云端目录并配置同步计划。</p>

    <div v-if="dialogVisible" class="dialog-backdrop">
      <form class="dialog task-dialog" @submit.prevent="submitForm">
        <header class="dialog-head">
          <h3>{{ editingTask ? '编辑任务' : '新建任务' }}</h3>
          <button type="button" class="close-btn" @click="closeDialog()">×</button>
        </header>
        <label>
          名称
          <input v-model="form.name" placeholder="例：项目文档双向同步" required />
        </label>
        <fieldset class="direction-group">
          <legend>同步方向</legend>
          <label class="direction-option">
            <input type="radio" value="cloud_to_local" v-model="form.direction" />
            <span>云盘 → 本地（下载备份）</span>
          </label>
          <label class="direction-option">
            <input type="radio" value="local_to_cloud" v-model="form.direction" />
            <span>本地 → 云盘（上传备份）</span>
          </label>
          <label class="direction-option">
            <input type="radio" value="bidirectional" v-model="form.direction" />
            <span>双向同步（自动比对差异）</span>
          </label>
        </fieldset>
        <label>
          企业分组
          <select v-model="form.groupId" @change="handleGroupChange" required>
            <option value="" disabled>请选择分组</option>
            <option v-for="group in groupOptions" :key="group.id" :value="group.id">
              {{ group.name }}（{{ group.tenantIds.length }} 个企业）
            </option>
          </select>
          <small v-if="!groupOptions.length" class="hint">请先在“设置 - 企业分组”创建分组。</small>
        </label>
        <label class="remote-field">
          云端目录
          <div class="remote-display">
            <span class="mono">{{ remotePathDisplay }}</span>
            <div class="remote-actions">
              <button class="btn secondary" type="button" @click="handleSelectRemoteFolder">选择</button>
              <button class="btn secondary" type="button" @click="clearRemoteSelection" :disabled="!form.remoteFolderToken">清除</button>
            </div>
          </div>
          <small v-if="form.remoteTenantName">所属租户：{{ form.remoteTenantName }}</small>
        </label>
        <label class="local-picker">
          本地目录
          <div class="local-input">
            <input v-model="form.localPath" placeholder="/Users/demo/Documents" required />
            <button class="btn secondary" type="button" @click="pickLocalDirectory">选择</button>
          </div>
        </label>
        <label class="cron-field">
          Cron 表达式
          <input v-model="form.schedule" placeholder="*/30 * * * *" required />
          <small :class="{ error: !!cronPreview.error }">
            {{ cronPreview.error || cronPreview.description }}
            <template v-if="!cronPreview.error && cronPreview.nextRun">· 下次 {{ cronPreview.nextRun }}</template>
          </small>
          <div class="cron-presets">
            <span>快捷：</span>
            <button v-for="preset in cronPresets" :key="preset.value" type="button" class="chip preset" @click="applyCronPreset(preset.value)">
              {{ preset.label }}
            </button>
          </div>
        </label>
        <label class="inline-checkbox">
          <input type="checkbox" v-model="form.enabled" />
          <span>启用此任务</span>
        </label>
        <label>
          变更检测策略
          <select v-model="form.detection">
            <option value="metadata">元数据（latest_modify_time + token）</option>
            <option value="size">大小/修改时间</option>
            <option value="checksum">大小 + Adler32 校验</option>
          </select>
        </label>
        <label>
          冲突策略
          <select v-model="form.conflict">
            <option value="newest">谁更新晚就保留谁</option>
            <option value="prefer_local">本地优先</option>
            <option value="prefer_remote">云端优先</option>
          </select>
        </label>
        <label class="inline-checkbox">
          <input type="checkbox" v-model="form.propagateDelete" />
          <span>同步删除（任一端删除后，另一端也会执行同样删除）</span>
        </label>
        <label>
          包含规则（每行一个通配符或 glob）
          <textarea v-model="form.includeText" rows="2" placeholder="例：**/*.docx"></textarea>
        </label>
        <label>
          排除规则
          <textarea v-model="form.excludeText" rows="2" placeholder="例：**/.git/**"></textarea>
        </label>
        <label>
          备注
          <textarea v-model="form.notes" rows="2" placeholder="同步说明，可选"></textarea>
        </label>
        <footer class="dialog-actions">
          <button class="btn secondary" type="button" @click="closeDialog">取消</button>
          <button class="btn primary" type="submit">{{ editingTask ? '保存修改' : '创建任务' }}</button>
        </footer>
      </form>
    </div>

    <div v-if="folderDialog.visible" class="prompt-overlay destination-layer">
      <div class="prompt-dialog destination-dialog">
        <header class="destination-header">
          <h3>选择云端目录</h3>
          <p class="path">{{ folderPath }}</p>
        </header>
        <div class="destination-list">
          <button
            v-if="folderDialog.stack.length > 1"
            class="list-item back"
            type="button"
            @click="goFolderUp"
          >
            <span class="entry-icon icon-folder"></span>
            <span class="name">返回上一级</span>
          </button>
          <button
            v-for="item in folderDialog.items"
            :key="item.key"
            class="list-item"
            type="button"
            @click="enterFolder(item)"
          >
            <span class="entry-icon icon-folder"></span>
            <div class="list-text">
              <span class="name">{{ item.name }}</span>
              <small v-if="item.tenant_label" class="tenant">{{ item.tenant_label }}</small>
            </div>
          </button>
          <p v-if="!folderDialog.items.length && !folderDialog.loading" class="empty">当前目录暂无文件夹</p>
          <p v-if="folderDialog.loading" class="empty">加载中...</p>
        </div>
        <div class="dialog-actions destination-actions">
          <button class="btn secondary" type="button" @click="createFolderInDialog" :disabled="!currentFolderToken">
            新建文件夹
          </button>
          <span class="flex-spacer"></span>
          <button class="btn secondary" type="button" @click="closeFolderDialog()">取消</button>
          <button class="btn primary" type="button" :disabled="!currentFolderToken" @click="confirmFolderDialog">
            使用该目录
          </button>
        </div>
      </div>
    </div>
    <div v-if="confirmState.visible" class="prompt-overlay">
      <div class="prompt-dialog">
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
import { computed, reactive, ref, watch } from 'vue';
import { invoke } from '@tauri-apps/api/core';
import {
  useTaskStore,
  type ConflictStrategy,
  type SyncTask,
  type TaskDirection,
  type TaskDetectionMode,
  type TaskStatus
} from '@/stores/taskStore';
import { useGroupStore } from '@/stores/groupStore';
import { useTenantStore } from '@/stores/tenantStore';
import { useSecurityStore } from '@/stores/securityStore';

interface DriveEntry {
  token: string;
  name: string;
  type: string;
  parent_token?: string;
}

interface PickerSelection {
  token: string;
  tenantId: string;
  tenantName: string;
  path: string;
}

interface FolderDialogEntry extends DriveEntry {
  key: string;
  tenantId: string;
  tenant_label?: string;
}

interface FolderStackNode {
  name: string;
  token: string | null;
  tenantId: string | null;
  tenantName: string | null;
}

const taskStore = useTaskStore();
const groupStore = useGroupStore();
const tenantStore = useTenantStore();
const securityStore = useSecurityStore();

const confirmState = reactive({
  visible: false,
  message: '',
  resolve: null as null | ((accepted: boolean) => void)
});

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

const directionFilter = ref<'all' | TaskDirection>('all');
const statusFilter = ref<'all' | TaskStatus>('all');
const keyword = ref('');

const dialogVisible = ref(false);
const editingTask = ref<SyncTask | null>(null);

const groupOptions = computed(() => groupStore.groups);
const tenantNameMap = computed(() => {
  const map = new Map<string, string>();
  tenantStore.tenants.forEach((tenant) => map.set(tenant.id, tenant.name));
  return map;
});

const form = reactive({
  name: '',
  direction: 'bidirectional' as TaskDirection,
  groupId: '',
  remoteFolderToken: '',
  remoteLabel: '',
  remoteTenantId: '',
  remoteTenantName: '',
  localPath: '',
  schedule: '0 * * * *',
  enabled: true,
  detection: 'checksum' as TaskDetectionMode,
  conflict: 'newest' as ConflictStrategy,
  propagateDelete: true,
  includeText: '',
  excludeText: '',
  notes: ''
});

const cronPreview = reactive({
  description: taskStore.cronDescription(form.schedule),
  nextRun: '',
  error: ''
});

const selectedGroup = computed(() => groupOptions.value.find((group) => group.id === form.groupId) || null);

watch(
  () => groupOptions.value,
  (list) => {
    if (!form.groupId && list.length) {
      setGroupId(list[0].id, true);
    }
  },
  { immediate: true }
);

const filteredTasks = computed(() => {
  const dir = directionFilter.value;
  const status = statusFilter.value;
  const term = keyword.value.trim().toLowerCase();
  return [...taskStore.tasks]
    .filter((task) => (dir === 'all' ? true : task.direction === dir))
    .filter((task) => (status === 'all' ? true : task.lastStatus === status))
    .filter((task) => {
      if (!term) return true;
      const text = [
        task.name,
        task.remoteLabel,
        task.localPath,
        task.notes || '',
        task.groupName || '',
        task.tenantName || '',
        task.tenantId
      ]
        .join(' ')
        .toLowerCase();
      return text.includes(term);
    })
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
});

const hasFilter = computed(
  () => directionFilter.value !== 'all' || statusFilter.value !== 'all' || !!keyword.value.trim()
);

const upcomingTask = computed(() => {
  const candidates = taskStore.tasks.filter((task) => task.enabled && task.nextRunAt);
  const sorted = [...candidates].sort((a, b) => (a.nextRunAt || '').localeCompare(b.nextRunAt || ''));
  return sorted[0] ?? null;
});

const upcomingText = computed(() => formatDate(upcomingTask.value?.nextRunAt));

const remotePathDisplay = computed(() => form.remoteLabel || '未选择目录');

const statusLabel = (task: SyncTask) => {
  switch (task.lastStatus) {
    case 'running':
      return '执行中';
    case 'success':
      return '上次成功';
    case 'failed':
      return '上次失败';
    case 'scheduled':
      return '已排程';
    default:
      return '待运行';
  }
};

const statusClass = (task: SyncTask) => `status-${task.lastStatus}`;

const detectionLabel = (mode: TaskDetectionMode) => {
  switch (mode) {
    case 'metadata':
      return '元数据';
    case 'size':
      return '大小/时间';
    default:
      return 'Checksum';
  }
};

const detectionTip = (mode: TaskDetectionMode) => {
  switch (mode) {
    case 'metadata':
      return '对比 latest_modify_time + doc_token';
    case 'size':
      return '仅比较 size + mtime，适合单向备份';
    default:
      return '组合 size + Adler32 checksum';
  }
};

const conflictLabel = (mode: ConflictStrategy) => {
  switch (mode) {
    case 'prefer_local':
      return '本地优先';
    case 'prefer_remote':
      return '云端优先';
    default:
      return '时间戳优先';
  }
};

const directionLabel = (direction: TaskDirection) => {
  switch (direction) {
    case 'cloud_to_local':
      return '云盘 → 本地';
    case 'local_to_cloud':
      return '本地 → 云盘';
    default:
      return '双向同步';
  }
};

const isTaskRunning = (task: SyncTask) => task.lastStatus === 'running';

const directionIconClass = (task: SyncTask) => {
  switch (task.direction) {
    case 'local_to_cloud':
      return 'upload';
    case 'cloud_to_local':
      return 'download';
    default:
      return 'sync';
  }
};

const runIndicatorLabel = (task: SyncTask) => {
  switch (task.direction) {
    case 'local_to_cloud':
      return '正在上传本地变更';
    case 'cloud_to_local':
      return '正在下载云端变更';
    default:
      return '正在执行双向同步';
  }
};

const formatDate = (value?: string | null) => {
  if (!value) return '—';
  const ts = Date.parse(value);
  if (Number.isNaN(ts)) return value;
  return new Date(ts).toLocaleString();
};

const resetFilters = () => {
  directionFilter.value = 'all';
  statusFilter.value = 'all';
  keyword.value = '';
};

const parsePatterns = (text: string) =>
  text
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter((item) => !!item);

function clearRemoteSelection() {
  form.remoteFolderToken = '';
  form.remoteLabel = '';
  form.remoteTenantId = '';
  form.remoteTenantName = '';
}

function setGroupId(id: string, preserveRemote = false) {
  form.groupId = id;
  if (!preserveRemote) {
    clearRemoteSelection();
  }
}

const handleGroupChange = () => {
  setGroupId(form.groupId);
};

const buildPayload = () => {
  const group = selectedGroup.value;
  return {
    name: form.name.trim(),
    direction: form.direction,
    groupId: group?.id || '',
    groupName: group?.name,
    tenantId: form.remoteTenantId,
    tenantName: form.remoteTenantName,
    remoteFolderToken: form.remoteFolderToken,
    remoteLabel: form.remoteLabel.trim(),
    localPath: form.localPath.trim(),
    schedule: form.schedule.trim(),
    enabled: form.enabled,
    detection: form.detection,
    conflict: form.conflict,
    propagateDelete: form.propagateDelete,
    includePatterns: parsePatterns(form.includeText),
    excludePatterns: parsePatterns(form.excludeText),
    notes: form.notes.trim()
  };
};

const resetForm = () => {
  form.name = '';
  form.direction = 'bidirectional';
  setGroupId(groupOptions.value[0]?.id ?? '', false);
  clearRemoteSelection();
  form.localPath = '';
  form.schedule = '0 * * * *';
  form.enabled = true;
  form.detection = 'checksum';
  form.conflict = 'newest';
  form.propagateDelete = true;
  form.includeText = '';
  form.excludeText = '';
  form.notes = '';
};

const openCreateDialog = () => {
  editingTask.value = null;
  resetForm();
  dialogVisible.value = true;
  updateCronPreview();
};

const openEditDialog = (task: SyncTask) => {
  editingTask.value = task;
  form.name = task.name;
  form.direction = task.direction;
  setGroupId(task.groupId, true);
  form.remoteFolderToken = task.remoteFolderToken;
  form.remoteLabel = task.remoteLabel;
  form.remoteTenantId = task.tenantId;
  form.remoteTenantName = task.tenantName || task.tenantId;
  form.localPath = task.localPath;
  form.schedule = task.schedule;
  form.enabled = task.enabled;
  form.detection = task.detection;
  form.conflict = task.conflict;
  form.propagateDelete = task.propagateDelete;
  form.includeText = task.includePatterns.join('\n');
  form.excludeText = task.excludePatterns.join('\n');
  form.notes = task.notes || '';
  dialogVisible.value = true;
  updateCronPreview();
};

const closeDialog = () => {
  dialogVisible.value = false;
};

const submitForm = async () => {
  if (!form.name.trim()) {
    window.alert('请输入任务名称');
    return;
  }
  if (!form.groupId) {
    window.alert('请选择企业分组');
    return;
  }
  if (!form.remoteFolderToken || !form.remoteTenantId) {
    window.alert('请先选择需要同步的云端目录');
    return;
  }
  if (!form.localPath.trim()) {
    window.alert('请选择本地目录');
    return;
  }
  if (!taskStore.validateCron(form.schedule.trim())) {
    cronPreview.error = 'Cron 表达式格式不正确';
    return;
  }
  const payload = buildPayload();
  try {
    if (editingTask.value) {
      await taskStore.updateTask(editingTask.value.id, payload);
    } else {
      await taskStore.createTask(payload);
    }
    closeDialog();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    window.alert(message || '保存任务失败');
  }
};

const toggleTask = async (task: SyncTask, event: Event) => {
  const checked = (event.target as HTMLInputElement).checked;
  await taskStore.toggleTask(task.id, checked);
};

const queueTask = async (task: SyncTask) => {
  await taskStore.triggerTask(task.id);
};

const duplicateTask = async (task: SyncTask) => {
  await taskStore.duplicateTask(task.id);
};

const askConfirm = (message: string) => openConfirm(message);

const removeTask = async (task: SyncTask) => {
  if (!(await askConfirm(`确认删除任务「${task.name}」吗？`))) return;
  try {
    await taskStore.removeTask(task.id);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    window.alert(message || '删除任务失败');
  }
};

const refreshSchedules = async () => {
  await taskStore.fetchTasks();
};

const updateCronPreview = () => {
  const expression = form.schedule.trim();
  if (!expression) {
    cronPreview.error = '请输入 Cron 表达式';
    cronPreview.nextRun = '';
    return;
  }
  if (!taskStore.validateCron(expression)) {
    cronPreview.error = 'Cron 表达式不合法';
    cronPreview.nextRun = '';
    return;
  }
  cronPreview.error = '';
  cronPreview.description = taskStore.cronDescription(expression);
  const next = taskStore.previewNextRun(expression, form.enabled);
  cronPreview.nextRun = next ? formatDate(next) : '暂无法计算';
};

watch(
  () => [form.schedule, form.enabled],
  () => updateCronPreview(),
  { immediate: true }
);

const cronPresets = [
  { label: '每 5 分钟', value: '*/5 * * * *' },
  { label: '每小时整点', value: '0 * * * *' },
  { label: '每日 02:00', value: '0 2 * * *' },
  { label: '每周一 09:30', value: '30 9 * * 1' },
  { label: '每月 1 日 03:00', value: '0 3 1 * *' }
];

const applyCronPreset = (value: string) => {
  form.schedule = value;
};

const pickLocalDirectory = async () => {
  try {
    const result = await invoke<string | null>('pick_directory_dialog');
    if (result) {
      form.localPath = result;
    }
  } catch (error) {
    console.error('pick_directory_dialog failed', error);
    window.alert('选择目录失败，请重试');
  }
};

const folderDialog = reactive({
  visible: false,
  loading: false,
  groupName: '',
  items: [] as FolderDialogEntry[],
  stack: [] as FolderStackNode[],
  allowedTenantIds: [] as string[],
  resolve: null as null | ((value: PickerSelection | null) => void)
});

const folderPath = computed(() => folderDialog.stack.map((item) => item.name).join(' / '));
const currentFolderNode = computed(() => folderDialog.stack[folderDialog.stack.length - 1] || null);
const currentFolderToken = computed(() => currentFolderNode.value?.token || null);

const isFolderEntry = (entry: DriveEntry) => entry.type?.toLowerCase() === 'folder';

const fetchAggregatedRootMap = async (allowedIds: string[]) => {
  const apiKey = await securityStore.ensureServerKey();
  const response = await invoke<{ aggregate: true; entries: Record<string, DriveEntry[]> }>('list_root_entries', {
    api_key: apiKey,
    aggregate: true
  });
  const allowed = new Set(allowedIds);
  return Object.fromEntries(Object.entries(response.entries).filter(([id]) => allowed.has(id)));
};

const loadAggregatedRootEntries = async (allowedIds: string[]) => {
  if (!allowedIds.length) return [];
  const map = await fetchAggregatedRootMap(allowedIds);
  const flattened: FolderDialogEntry[] = [];
  for (const [tenantId, list] of Object.entries(map)) {
    const label = tenantNameMap.value.get(tenantId) || '企业空间';
    list
      .filter((entry) => isFolderEntry(entry))
      .forEach((entry) => flattened.push({ ...entry, key: entry.token, tenantId, tenant_label: label }));
  }
  return flattened;
};

const listFolderEntries = async (token: string) => {
  const apiKey = await securityStore.ensureServerKey();
  return invoke<DriveEntry[]>('list_folder_entries', {
    folder_token: token,
    folderToken: token,
    api_key: apiKey
  });
};

const createRemoteFolder = async (parentToken: string, name: string) => {
  const apiKey = await securityStore.ensureServerKey();
  await invoke('create_folder', {
    api_key: apiKey,
    payload: { parent_token: parentToken, name }
  });
};

const loadFolderEntries = async () => {
  if (!folderDialog.visible) return;
  folderDialog.loading = true;
  try {
    const current = currentFolderNode.value;
    if (!current || !current.token) {
      folderDialog.items = await loadAggregatedRootEntries(folderDialog.allowedTenantIds);
    } else {
      const tenantId = current.tenantId || '';
      const tenantName = current.tenantName || tenantNameMap.value.get(tenantId) || '云空间';
      const entries = await listFolderEntries(current.token);
      folderDialog.items = entries
        .filter((entry) => isFolderEntry(entry))
        .map((entry) => ({
          ...entry,
          key: entry.token,
          tenantId,
          tenant_label: tenantName
        }));
    }
  } finally {
    folderDialog.loading = false;
  }
};

const openFolderDialog = async () => {
  const group = selectedGroup.value;
  if (!group) {
    window.alert('请选择企业分组');
    return null;
  }
  if (!group.tenantIds.length) {
    window.alert('该分组暂无企业实例');
    return null;
  }
  folderDialog.groupName = group.name;
  folderDialog.allowedTenantIds = [...group.tenantIds];
  folderDialog.stack = [{ name: group.name, token: null, tenantId: null, tenantName: null }];
  folderDialog.visible = true;
  await loadFolderEntries();
  return new Promise<PickerSelection | null>((resolve) => {
    folderDialog.resolve = resolve;
  });
};

const closeFolderDialog = (selection: PickerSelection | null = null) => {
  const resolver = folderDialog.resolve;
  folderDialog.visible = false;
  folderDialog.resolve = null;
  resolver?.(selection);
};

const enterFolder = async (item: FolderDialogEntry) => {
  folderDialog.stack = [
    ...folderDialog.stack,
    { name: item.name, token: item.token, tenantId: item.tenantId, tenantName: item.tenant_label || null }
  ];
  await loadFolderEntries();
};

const goFolderUp = async () => {
  if (folderDialog.stack.length <= 1) return;
  folderDialog.stack = folderDialog.stack.slice(0, -1);
  await loadFolderEntries();
};

const confirmFolderDialog = () => {
  const node = currentFolderNode.value;
  if (!node?.token || !node.tenantId) {
    window.alert('请选择具体的云端文件夹');
    return;
  }
  const tenantName = node.tenantName || tenantNameMap.value.get(node.tenantId) || '云空间';
  const pathParts = folderDialog.stack.slice(1).map((item) => item.name);
  closeFolderDialog({
    token: node.token,
    tenantId: node.tenantId,
    tenantName,
    path: pathParts.join(' / ') || tenantName
  });
};

const createFolderInDialog = async () => {
  const token = currentFolderToken.value;
  if (!token) {
    window.alert('请进入某个租户或文件夹后再新建');
    return;
  }
  const name = window.prompt('请输入新建文件夹名称');
  if (!name || !name.trim()) return;
  try {
    await createRemoteFolder(token, name.trim());
    await loadFolderEntries();
  } catch (error) {
    window.alert(error instanceof Error ? error.message : String(error));
  }
};

const handleSelectRemoteFolder = async () => {
  const selection = await openFolderDialog();
  if (!selection) return;
  form.remoteFolderToken = selection.token;
  form.remoteTenantId = selection.tenantId;
  form.remoteTenantName = selection.tenantName;
  form.remoteLabel = selection.path;
};
</script>

<style scoped>
/* styles same as earlier version placed here */
.tasks-shell { display:flex; flex-direction:column; gap:1.5rem; background:#fff; min-height:100%; padding:1.75rem 1.5rem; }
.tasks-head { display:flex; justify-content:space-between; align-items:center; gap:1rem; }
.head-actions { display:flex; gap:0.75rem; }
.task-metrics { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:0.75rem; }
.task-metrics article { background:white; border-radius:1rem; border:1px solid #e2eaf4; padding:1rem 1.25rem; display:flex; flex-direction:column; gap:0.2rem; }
.task-metrics strong { font-size:1.5rem; }
.task-metrics small { color:#4a6387; }
.task-filters { display:flex; flex-wrap:wrap; gap:1rem; align-items:flex-end; background:#f8fbff; border-radius:1rem; padding:1rem; }
.task-filters .filter { display:flex; flex-direction:column; gap:0.35rem; min-width:200px; }
.task-filters select,.task-filters input { padding:0.55rem 0.65rem; border-radius:0.65rem; border:1px solid #cbd5f5; }
.search-filter { flex:1; min-width:240px; }
.task-list { display:flex; flex-direction:column; gap:1rem; }
.task-card { border:1px solid #dce6f9; border-radius:1rem; padding:1.25rem; box-shadow:0 8px 20px rgba(15,23,42,0.08); display:flex; flex-direction:column; gap:1rem; }
.task-card.disabled { opacity:0.6; }
.task-card-head { display:flex; justify-content:space-between; gap:1rem; align-items:center; }
.task-tags { display:flex; flex-wrap:wrap; gap:0.4rem; margin-top:0.35rem; }
.task-head-actions { display:flex; align-items:center; gap:0.5rem; flex-wrap:wrap; justify-content:flex-end; }
.task-run-indicator { display:flex; align-items:center; }
.run-icon { width:28px; height:28px; position:relative; display:inline-flex; align-items:center; justify-content:center; margin-right:0.25rem; }
.run-icon::before { content:''; position:absolute; inset:0; border-radius:50%; border:2px solid #bfdbfe; border-top-color:#2563eb; animation:run-spin 1s linear infinite; }
.run-arrow { position:relative; display:block; }
.run-arrow.upload { width:0; height:0; border-left:6px solid transparent; border-right:6px solid transparent; border-bottom:10px solid #1d4ed8; }
.run-arrow.download { width:0; height:0; border-left:6px solid transparent; border-right:6px solid transparent; border-top:10px solid #1d4ed8; }
.run-arrow.sync { width:12px; height:12px; }
.run-arrow.sync::before,
.run-arrow.sync::after { content:''; position:absolute; left:50%; transform:translateX(-50%); border-left:5px solid transparent; border-right:5px solid transparent; }
.run-arrow.sync::before { top:-2px; border-bottom:8px solid #1d4ed8; }
.run-arrow.sync::after { bottom:-2px; border-top:8px solid #1d4ed8; }
@keyframes run-spin { from { transform:rotate(0deg); } to { transform:rotate(360deg); } }
.task-body { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:1rem; }
.label { font-size:0.8rem; color:#5b6b8c; }
.mono { font-family:'SFMono-Regular',Consolas,monospace; }
.pattern-chips { display:flex; flex-wrap:wrap; gap:0.35rem; }
.chip { padding:0.2rem 0.7rem; border-radius:999px; background:#e8f0ff; color:#1d3785; font-size:0.8rem; }
.chip.subtle { background:#f2f5fb; color:#4a6387; }
.chip.direction { background:#e6f7f3; color:#0f5132; }
.chip.include { background:#d6f4ff; color:#075985; }
.chip.exclude { background:#ffe3e3; color:#8a1124; }
.chip.preset { cursor:pointer; background:#edf2ff; color:#1d3785; }
.status-chip { padding:0.2rem 0.6rem; border-radius:0.5rem; font-size:0.85rem; }
.status-running { background:#fff3cd; color:#915c00; }
.status-success { background:#d1fae5; color:#065f46; }
.status-failed { background:#fee2e2; color:#991b1b; }
.status-idle,.status-scheduled { background:#e0e7ff; color:#312e81; }
.remote-field .remote-display { display:flex; justify-content:space-between; align-items:center; gap:1rem; padding:0.55rem 0.65rem; border:1px solid #cbd5f5; border-radius:0.65rem; }
.remote-actions { display:flex; gap:0.5rem; }
.local-picker .local-input { display:flex; gap:0.5rem; }
.local-picker input { flex:1; }
.direction-group { border:1px solid #dfe8f6; border-radius:0.75rem; padding:0.75rem 1rem; display:flex; flex-direction:column; gap:0.5rem; }
.direction-group legend { padding:0 0.25rem; font-weight:600; }
.task-dialog label.direction-option {
  flex-direction: row;
  display: flex;
  align-items: center;
  gap: 0.4rem;
  width: auto;
}
.task-dialog label.direction-option input {
  width: auto;
  margin: 0;
}
.task-dialog label.inline-checkbox {
  flex-direction: row;
  display: flex;
  align-items: center;
  gap: 0.4rem;
  width: auto;
}
.task-dialog label.inline-checkbox input {
  width: auto;
  margin: 0;
}
.cron-field small { color:#4a6387; }
.cron-field small.error { color:#b91c1c; }
.cron-presets { display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center; margin-top:0.35rem; }
.dialog-backdrop { position:fixed; inset:0; background:rgba(15,23,42,0.5); display:flex; align-items:flex-start; justify-content:center; padding:4vh 1rem; overflow:auto; z-index:50; }
.task-dialog { background:white; border-radius:1rem; padding:1.5rem; width:min(720px,95vw); display:flex; flex-direction:column; gap:1rem; position:relative; }
.dialog-head { display:flex; justify-content:space-between; align-items:center; }
.close-btn { border:none; background:#edf2ff; border-radius:50%; width:32px; height:32px; font-size:1.1rem; }
.task-dialog label { display:flex; flex-direction:column; gap:0.35rem; }
.task-dialog input,.task-dialog select,.task-dialog textarea { border-radius:0.65rem; border:1px solid #cbd5f5; padding:0.55rem 0.65rem; font-size:0.95rem; }
.task-dialog textarea { resize:vertical; }
.dialog-actions { display:flex; justify-content:flex-end; gap:0.75rem; }
.prompt-overlay { position:fixed; inset:0; background:rgba(15,23,42,0.45); display:grid; place-items:center; z-index:4000; padding:1rem; }
.prompt-overlay.destination-layer { z-index:9000; position:fixed; inset:0; background:rgba(15,23,42,0.45); display:grid; place-items:center; padding:1rem; }
.prompt-dialog { background:white; padding:1.25rem; border-radius:0.75rem; width:min(360px,90vw); display:flex; flex-direction:column; gap:0.75rem; box-shadow:0 20px 45px rgba(15,23,42,0.25); }
.destination-dialog { width:min(520px,96vw); gap:0.75rem; }
.destination-header { border-bottom:1px solid #e2e8f0; padding-bottom:0.35rem; }
.destination-header .path { margin:0.35rem 0 0; font-size:0.9rem; color:#7c8da6; }
.destination-list { max-height:360px; overflow-y:auto; border:1px solid #e2e8f0; border-radius:0.75rem; background:#fcfdff; }
.destination-list .list-item { width:100%; display:flex; align-items:center; gap:0.6rem; padding:0.65rem 1rem; border:none; background:transparent; border-bottom:1px solid #eef2ff; cursor:pointer; }
.destination-list .list-item:last-child { border-bottom:none; }
.destination-list .list-item .entry-icon { width:28px; height:28px; display:inline-flex; align-items:center; justify-content:center; background:#fef9c3; color:#d97706; border-radius:0.35rem; }
.destination-list .list-text { display:flex; flex-direction:column; align-items:flex-start; }
.destination-list .tenant { color:#9aa5b8; font-size:0.8rem; }
.destination-actions { display:flex; align-items:center; gap:0.5rem; }
.flex-spacer { flex:1; }
.hint { color:#8b9bb8; }
.empty { text-align:center; color:#526285; padding:0.5rem 0; }
</style>
