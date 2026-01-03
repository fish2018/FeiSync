<template>
  <section class="home-shell">
    <header class="toolbar">
      <div class="toolbar-left">
        <div class="mode-toggle">
          <label for="group-filter">企业分组</label>
          <select id="group-filter" v-model="selectedGroupId" @change="handleGroupSelect">
            <option value="">全部分组</option>
            <option v-for="group in groupOptions" :key="group.id" :value="group.id">
              {{ group.name }}
            </option>
          </select>
        </div>
        <div class="search-box">
          <input v-model="keyword" @keyup.enter="applySearch" placeholder="搜索文件" />
          <button class="btn secondary" @click="applySearch" :disabled="searching || actionBusy">
            <span v-if="searching" class="spinner small"></span>
            <span>{{ searching ? '搜索中...' : '搜索' }}</span>
          </button>
        </div>
      </div>
      <div class="toolbar-right">
        <span v-if="actionBusy" class="hint">{{ actionMessage || '执行中...' }}</span>
        <button class="btn secondary" @click="toggleViewMode">{{ viewMode === 'list' ? '图标模式' : '列表模式' }}</button>
        <button class="btn secondary refresh-btn" @click="refresh" :disabled="actionBusy || refreshing">
          <span v-if="refreshing" class="spinner"></span>
          <span>{{ refreshing ? '刷新中...' : '刷新' }}</span>
        </button>
        <button class="btn primary" @click="pendingAction('upload')" :disabled="actionBusy">上传</button>
        <button class="btn primary" @click="pendingAction('new-folder')" :disabled="actionBusy">新建文件夹</button>
      </div>
    </header>
    <nav class="breadcrumb" v-if="breadcrumbDisplay.length">
      <template v-for="(crumb, index) in breadcrumbDisplay" :key="`${crumb.token || 'root'}-${index}`">
        <button
          class="crumb"
          :class="{ active: index === breadcrumbDisplay.length - 1 }"
          @click="navigateBreadcrumb(index)"
        >
          {{ crumb.name }}
        </button>
        <span v-if="index < breadcrumbDisplay.length - 1" class="separator">></span>
      </template>
    </nav>

    <div class="batch-actions" v-if="selectedCount">
      <span>已选择 {{ selectedCount }} 项</span>
      <div class="actions">
        <button class="btn secondary" @click="pendingAction('download')" :disabled="actionBusy">下载</button>
        <button class="btn secondary" @click="pendingAction('move')" :disabled="actionBusy">移动</button>
        <button class="btn secondary" @click="pendingAction('copy')" :disabled="actionBusy">复制</button>
        <button class="btn secondary" @click="pendingAction('rename')" :disabled="actionBusy || !canRenameSelection">重命名</button>
        <button class="btn danger" type="button" @click="() => handleDelete()" :disabled="actionBusy">
          删除
        </button>
      </div>
    </div>
    <div v-if="uploadChoiceState.visible" class="prompt-overlay">
      <div class="prompt-dialog">
        <p>请选择上传类型</p>
        <div class="dialog-actions stacked">
          <button class="btn secondary" @click="finishUploadChoice('file')">上传文件</button>
          <button class="btn primary" @click="finishUploadChoice('folder')">上传文件夹</button>
        </div>
        <div class="dialog-actions">
          <button class="btn" @click="cancelUploadChoice">取消</button>
        </div>
      </div>
    </div>

    <div class="search-loading" v-if="searching">正在搜索中，请稍候...</div>
    <div v-if="viewMode === 'list'" class="list-view">
      <label v-if="displayedEntries.length" class="list-select-all">
        <input type="checkbox" :checked="isAllSelected === true" @change="toggleAll" />
        全部选中
      </label>
      <table>
        <thead>
          <tr>
            <th><input type="checkbox" :checked="isAllSelected === true" @change="toggleAll" /></th>
            <th @click="toggleSort('name')">文件名</th>
            <th @click="toggleSort('type')">类型</th>
            <th @click="toggleSort('size')">大小</th>
            <th @click="toggleSort('updated_at')">修改时间</th>
            <th v-if="showPathColumn">路径</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in displayedEntries"
            :key="item.token"
            :data-entry-token="item.token"
            :data-droppable="isFolderEntry(item) ? 'true' : 'false'"
            :class="{ 'droppable-hover': dragState.over === item.token }"
          >
            <td>
              <input type="checkbox" :checked="selection.has(item.token)" @change="toggleSelection(item.token)" />
            </td>
            <td
              class="name-cell"
              @click="handleEntryClick(item)"
              @pointerdown="handleEntryPointerDown($event, item)"
            >
              <span class="entry-icon" :class="iconClass(item)">
                <svg
                  v-if="isFolderEntry(item)"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                  focusable="false"
                >
                  <path
                    d="M3.5 7.25C3.5 6.01 4.51 5 5.75 5h3.89c.58 0 1.13.27 1.48.73l.64.82c.24.31.61.5 1 .5h5.49c1.04 0 1.88.84 1.88 1.88v7.82c0 1.1-.9 2-2 2H5.75c-1.24 0-2.25-1-2.25-2.25v-8Z"
                  />
                </svg>
                <span v-else>{{ iconLabel(item) }}</span>
              </span>
              <span class="entry-name">{{ item.name }}</span>
            </td>
            <td>{{ item.type }}</td>
            <td>{{ formatSize(item.size) }}</td>
            <td>{{ formatTime(item.update_time) }}</td>
            <td v-if="showPathColumn" class="path-cell">{{ item.path_display }}</td>
          </tr>
          <tr v-if="!displayedEntries.length">
            <td colspan="6" class="empty">暂无文件</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="grid-view">
      <label v-if="displayedEntries.length" class="grid-select-all">
        <input type="checkbox" :checked="isAllSelected === true" @change="toggleAll" />
        全部选中
      </label>
      <div class="grid-cards">
        <article
          v-for="item in displayedEntries"
          :key="item.token"
          :class="['grid-card', { selected: selection.has(item.token), 'droppable-hover': dragState.over === item.token }]"
          :data-entry-token="item.token"
          :data-droppable="isFolderEntry(item) ? 'true' : 'false'"
        >
          <div
            class="grid-body"
            @click="handleGridClick($event, item)"
            @dblclick.stop.prevent="handleGridOpen(item)"
            @pointerdown="handleEntryPointerDown($event, item)"
          >
            <div class="grid-icon-shell">
              <span class="entry-icon icon-large" :class="iconClass(item)">
                <svg
                  v-if="isFolderEntry(item)"
                  viewBox="0 0 24 24"
                  aria-hidden="true"
                  focusable="false"
                >
                  <path
                    d="M3.5 7.25C3.5 6.01 4.51 5 5.75 5h3.89c.58 0 1.13.27 1.48.73l.64.82c.24.31.61.5 1 .5h5.49c1.04 0 1.88.84 1.88 1.88v7.82c0 1.1-.9 2-2 2H5.75c-1.24 0-2.25-1-2.25-2.25v-8Z"
                  />
                </svg>
                <span v-else>{{ iconLabel(item) }}</span>
              </span>
            </div>
            <h4 class="grid-name">{{ item.name }}</h4>
          </div>
        </article>
      </div>
      <p v-if="!displayedEntries.length" class="empty">暂无文件</p>
    </div>
    <div v-if="promptState.visible" class="prompt-overlay">
      <div class="prompt-dialog">
        <p>{{ promptState.title }}</p>
        <input v-model="promptState.value" @keyup.enter="confirmPrompt" autofocus />
        <div class="dialog-actions">
          <button class="btn secondary" @click="cancelPrompt">取消</button>
          <button class="btn primary" @click="confirmPrompt">确定</button>
        </div>
      </div>
    </div>
    <div v-if="destinationDialog.visible" class="prompt-overlay destination-layer">
      <div class="prompt-dialog destination-dialog">
        <header class="destination-header">
          <h3>{{ destinationDialog.mode === 'move' ? '移动到' : '复制到' }}</h3>
          <p class="path">{{ destinationPath }}</p>
        </header>
        <div class="destination-list">
          <button
            v-if="destinationDialog.stack.length > 1"
            class="list-item back"
            type="button"
            @click="goDestinationUp"
          >
            <span class="entry-icon icon-folder"></span>
            <span class="name">返回上一级</span>
          </button>
          <button
            v-for="item in destinationDialog.items"
            :key="item.token"
            class="list-item"
            type="button"
            :class="{ disabled: isDestinationBlocked(item.token) }"
            :disabled="isDestinationBlocked(item.token)"
            @click="enterDestinationFolder(item)"
          >
            <span class="entry-icon icon-folder"></span>
            <div class="list-text">
              <span class="name">{{ item.name }}</span>
              <small v-if="item.tenant_label" class="tenant">{{ item.tenant_label }}</small>
            </div>
          </button>
          <p v-if="!destinationDialog.items.length && !destinationDialog.loading" class="empty">当前目录暂无文件夹</p>
          <p v-if="destinationDialog.loading" class="empty">加载中...</p>
        </div>
        <div class="dialog-actions destination-actions">
          <button class="btn secondary" type="button" @click="createFolderInDestination">新建文件夹</button>
          <span class="flex-spacer"></span>
          <button class="btn secondary" type="button" @click="cancelDestination">取消</button>
          <button
            class="btn primary"
            type="button"
            :disabled="!currentDestinationToken"
            @click="confirmDestination"
          >
            {{ destinationDialog.mode === 'move' ? '移动到此' : '复制到此' }}
          </button>
        </div>
      </div>
    </div>
    <div v-if="confirmState.visible" class="prompt-overlay">
      <div class="prompt-dialog">
        <p>{{ confirmState.message }}</p>
        <div class="dialog-actions">
          <button class="btn secondary" @click="finishConfirm(false)">取消</button>
          <button class="btn danger" @click="finishConfirm(true)">确认</button>
        </div>
      </div>
    </div>
    <div v-if="dragState.active" class="drag-ghost" :style="ghostStyle">
      <span>{{ dragState.tokens.length }} 项</span>
    </div>
    <div v-if="dropOverlay.visible" class="drop-overlay">
      <div class="drop-message">松手即可上传到 {{ dropOverlay.targetName }}</div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { storeToRefs } from 'pinia';
import { useTenantStore } from '@/stores/tenantStore';
import { useGroupStore } from '@/stores/groupStore';
import { useExplorerStore } from '@/stores/explorerStore';
import type { DriveEntry } from '@/stores/explorerStore';
import { useSecurityStore } from '@/stores/securityStore';
import { useUiStore } from '@/stores/uiStore';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import type { UnlistenFn } from '@tauri-apps/api/event';

const tenantStore = useTenantStore();
const explorerStore = useExplorerStore();
const securityStore = useSecurityStore();
const groupStore = useGroupStore();
const { tenants } = storeToRefs(tenantStore);
const { groups } = storeToRefs(groupStore);
const uiStore = useUiStore();
const selectedGroupId = computed({
  get: () => uiStore.activeGroupId,
  set: (value: string) => uiStore.setActiveGroup(value || '')
});
onMounted(() => {
  if (!groups.value.length) {
    groupStore.fetchGroups();
  }
});
const keyword = ref('');
const appliedKeyword = ref('');
const viewMode = ref<'list' | 'grid'>('list');
const sortField = ref<'name' | 'type' | 'size' | 'updated_at'>('name');
const sortDir = ref<'asc' | 'desc'>('asc');
const selection = ref<Set<string>>(new Set());
const actionBusy = ref(false);
const actionMessage = ref('');
const refreshing = ref(false);
const globalResults = ref<DriveEntry[]>([]);
const searching = ref(false);
const searchRunId = ref(0);

interface LocalPickerItem {
  path: string;
  type: 'file' | 'folder';
}

const groupFilterActive = computed(() => !!selectedGroupId.value);
const currentGroup = computed(() => groups.value.find((group) => group.id === selectedGroupId.value) || null);
const currentGroupName = computed(() => currentGroup.value?.name || '');
const groupOptions = computed(() => groups.value.filter((group) => (group.tenantIds || []).length));
const allGroupTenantIds = computed(() => {
  const ids = new Set<string>();
  groups.value.forEach((group) => {
    (group.tenantIds || []).forEach((id) => ids.add(id));
  });
  return ids;
});
const selectedGroupTenantIds = computed(() => {
  const fallback = () => {
    const set = new Set<string>();
    tenants.value
      .filter((tenant) => tenant.active)
      .forEach((tenant) => set.add(tenant.id));
    return set;
  };
  if (!groups.value.length) {
    return fallback();
  }
  if (groupFilterActive.value) {
    const ids = new Set<string>();
    (currentGroup.value?.tenantIds || []).forEach((id) => ids.add(id));
    return ids.size ? ids : fallback();
  }
  const union = new Set<string>();
  groups.value.forEach((group) => {
    (group.tenantIds || []).forEach((id) => union.add(id));
  });
  return union.size ? union : fallback();
});

const rawEntries = computed<DriveEntry[]>(() => explorerStore.entries);
const baseEntries = computed(() => (appliedKeyword.value ? globalResults.value : rawEntries.value));
const selectedEntries = computed(() => baseEntries.value.filter((entry) => selection.value.has(entry.token)));
const rootLabel = computed(() => (groupFilterActive.value ? currentGroupName.value || '企业分组' : '我的空间'));
const ROOT_PLACEHOLDER = '__root__';
const breadcrumbs = ref<{ name: string; token: string | null }[]>([]);
const tenantRootCache = new Map<string, string>();

const cacheTenantRootToken = (tenantId?: string | null, token?: string | null) => {
  if (tenantId && token) {
    tenantRootCache.set(tenantId, token);
  }
};

const ensureTenantRootToken = async (tenantId: string) => {
  const cached = tenantRootCache.get(tenantId);
  if (cached) return cached;
  const apiKey = await securityStore.ensureServerKey();
  const { rootToken } = await invoke<{ rootToken: string }>('list_root_entries', {
    api_key: apiKey,
    apiKey,
    tenant_id: tenantId,
    tenantId
  });
  cacheTenantRootToken(tenantId, rootToken);
  return rootToken;
};

const promptState = reactive({
  visible: false,
  title: '',
  value: '',
  resolve: null as null | ((value: string | null) => void)
});

const confirmState = reactive({
  visible: false,
  message: '',
  resolve: null as null | ((accepted: boolean) => void)
});

const uploadChoiceState = reactive({
  visible: false,
  resolve: null as null | ((value: 'file' | 'folder' | null) => void)
});

const destinationDialog = reactive({
  visible: false,
  mode: 'move' as 'move' | 'copy',
  loading: false,
  items: [] as Array<DriveEntry & { tenant_label?: string }>,
  stack: [] as Array<{ name: string; token: string | null }>,
  resolve: null as null | ((value: string | null) => void),
  blockedTokens: [] as string[]
});
const destinationPath = computed(() => destinationDialog.stack.map((item) => item.name).join(' / '));
const currentDestinationToken = computed(() => destinationDialog.stack[destinationDialog.stack.length - 1]?.token || null);
const destinationBlockedSet = computed(() => new Set(destinationDialog.blockedTokens));
const isDestinationBlocked = (token?: string | null) =>
  !!token && destinationBlockedSet.value.has(token);

const dragState = reactive({
  active: false,
  pending: false,
  pointerId: null as number | null,
  startX: 0,
  startY: 0,
  tokens: [] as string[],
  over: '',
  hoverEntry: null as DriveEntry | null,
  x: 0,
  y: 0,
  suppressClick: false
});

const ghostStyle = computed(() => ({
  transform: `translate(${dragState.x}px, ${dragState.y}px)`
}));

const dropOverlay = reactive({
  visible: false,
  targetName: ''
});

const pointerPosition = reactive({
  x: 0,
  y: 0
});

const pointerTrackingHandlers: Array<{ type: keyof WindowEventMap; handler: EventListener; options?: boolean | AddEventListenerOptions }> = [];
const fileDropUnlisteners: UnlistenFn[] = [];
const logDropEvent = (..._args: any[]) => {};
const formatLog = (fn: string, action: string, payload?: Record<string, unknown>, result?: string) => {
  const time = new Date().toISOString();
  const input = payload ? JSON.stringify(payload) : '{}';
  const suffix = result ? ` 结果=${result}` : '';
  return `${time} ${fn} ${action} 输入=${input}${suffix}`;
};
const logOperation = (fn: string, action: string, payload?: Record<string, unknown>, result?: string) => {
  console.log(formatLog(fn, action, payload, result));
};
const logError = (fn: string, action: string, error: unknown, payload?: Record<string, unknown>) => {
  console.error(
    formatLog(fn, action, payload, error instanceof Error ? error.message : String(error))
  );
};
const runWithLimit = async <T>(factories: Array<() => Promise<T>>, limit = 5) => {
  if (!factories.length) {
    return [] as T[];
  }
  let cursor = 0;
  const results: (T | undefined)[] = new Array(factories.length);
  const worker = async () => {
    while (cursor < factories.length) {
      const current = cursor++;
      const value = await factories[current]();
      results[current] = value;
    }
  };
  const workers = Array.from({ length: Math.min(limit, factories.length) }, worker);
  await Promise.all(workers);
  return results.filter((item): item is T => item !== undefined);
};

const entryMap = computed(() => {
  const map = new Map<string, DriveEntry>();
  baseEntries.value.forEach((entry) => map.set(entry.token, entry));
  return map;
});


interface DisplayEntry extends DriveEntry {
  path_display?: string;
}

const computePathDisplay = (entry: DriveEntry) => {
  if (entry.path) {
    const cleaned = stripRootPrefix(entry.path);
    if (entry.tenant_name) {
      if (cleaned.startsWith(`${entry.tenant_name} /`)) {
        return cleaned;
      }
      if (cleaned === entry.tenant_name) {
        return cleaned;
      }
      return `${entry.tenant_name} / ${cleaned}`;
    }
    return cleaned;
  }
  const parts = breadcrumbs.value.map((crumb) => crumb.name);
  if (!parts.length) parts.push(rootLabel.value);
  if (parts[0].toLowerCase() === 'root') {
    parts.shift();
  }
  if (parts[parts.length - 1] !== entry.name) {
    parts.push(entry.name);
  }
  const base = stripRootPrefix(parts.join(' / '));
  if (groupFilterActive.value && currentGroupName.value) {
    return `${currentGroupName.value} / ${base}`;
  }
  return base;
};

const displayedEntries = computed<DisplayEntry[]>(() =>
  baseEntries.value
    .slice()
    .sort((a, b) => compareEntry(a, b))
    .map((entry) => ({
      ...entry,
      path_display: computePathDisplay(entry)
    }))
);

const selectedCount = computed(() => selectedEntries.value.length);
const isAllSelected = computed(() => !!baseEntries.value.length && selection.value.size === baseEntries.value.length);
const canRenameSelection = computed(() => selectedEntries.value.length > 0);
const showPathColumn = computed(() => !!appliedKeyword.value);

const resetBreadcrumbs = () => {
  const rootToken = explorerStore.currentFolderToken || (groupFilterActive.value ? null : ROOT_PLACEHOLDER);
  breadcrumbs.value = [{ name: rootLabel.value, token: rootToken }];
};
resetBreadcrumbs();

const fetchAggregatedRootMap = async (allowedIds?: string[]) => {
  const apiKey = await securityStore.ensureServerKey();
  const response = await invoke<{ aggregate: true; entries: Record<string, DriveEntry[]> }>('list_root_entries', {
    api_key: apiKey,
    apiKey,
    aggregate: true
  });
  if (allowedIds && allowedIds.length) {
    const allowedSet = new Set(allowedIds);
    return Object.fromEntries(Object.entries(response.entries).filter(([id]) => allowedSet.has(id)));
  }
  return response.entries;
};

const applyAggregatedEntries = (entriesMap: Record<string, DriveEntry[]>) => {
  const flattened: DriveEntry[] = [];
  for (const [tenantId, list] of Object.entries(entriesMap)) {
    const label = tenants.value.find((item) => item.id === tenantId)?.name || '企业空间';
    list.forEach((entry) => flattened.push({ ...entry, tenant_name: label }));
  }
  explorerStore.currentTenantId = '';
  explorerStore.currentFolderToken = '';
  explorerStore.entries = flattened;
};

const getAllowedTenantIds = () => Array.from(selectedGroupTenantIds.value);

const loadAggregatedView = async () => {
  const targetIds = getAllowedTenantIds();
  if (!targetIds.length) {
    explorerStore.currentTenantId = '';
    explorerStore.currentFolderToken = '';
    explorerStore.entries = [];
    return;
  }
  const entriesMap = await fetchAggregatedRootMap(targetIds);
  applyAggregatedEntries(entriesMap);
};

const loadRoot = async (tenantId?: string | null) => {
  const target = tenantId ?? null;
  logOperation('loadRoot', '开始加载目录', {
    tenant: target || null,
    group: selectedGroupId.value || null
  });
  if (target) {
    await explorerStore.loadRoot(target);
    cacheTenantRootToken(target, explorerStore.currentFolderToken);
  } else {
    await loadAggregatedView();
  }
  selection.value = new Set();
  resetBreadcrumbs();
};

const syncTreeAndSearch = async () => {
  if (destinationDialog.visible) {
    await loadDestinationEntries(currentDestinationToken.value);
  }
  if (appliedKeyword.value) {
    await runSearch(appliedKeyword.value);
  } else {
    globalResults.value = [];
    searching.value = false;
  }
};

const pickAutoTenant = () => {
  const ordered = [...availableTenants.value].sort((a, b) => a.order - b.order);
  const EPSILON = 0.001;
  return ordered.find((tenant) => tenant.quota_gb - tenant.used_gb > EPSILON) ?? null;
};

const ensureWritableParentToken = async (initialToken?: string | null) => {
  let parentToken = typeof initialToken === 'undefined' ? ensureParentToken() : initialToken;
  let tenantName: string | null = null;
  if (!parentToken) {
    const tenant = pickAutoTenant();
    if (!tenant) {
      showAlert('所有启用的企业实例容量均已用尽，请扩容或启用更多实例后再试。');
      return { token: null, tenantName: null };
    }
    tenantName = tenant.name;
    parentToken = await ensureTenantRootToken(tenant.id);
  }
  if (!parentToken) {
    showAlert('请先加载一个文件夹');
    return { token: null, tenantName };
  }
  return { token: parentToken, tenantName };
};

const refresh = async () => {
  if (refreshing.value) return;
  refreshing.value = true;
  try {
    if (explorerStore.currentFolderToken) {
      await explorerStore.reloadCurrent();
    } else {
      await loadRoot();
    }
    if (appliedKeyword.value) {
      await runSearch(appliedKeyword.value);
    }
  } finally {
    refreshing.value = false;
  }
};

const toggleViewMode = () => {
  viewMode.value = viewMode.value === 'list' ? 'grid' : 'list';
};

const toggleSelection = (token: string) => {
  const copy = new Set(selection.value);
  if (copy.has(token)) copy.delete(token);
  else copy.add(token);
  selection.value = copy;
};

const toggleAll = () => {
  if (isAllSelected.value) {
    selection.value = new Set();
  } else {
    selection.value = new Set(baseEntries.value.map((item) => item.token));
  }
};

const toggleSort = (field: 'name' | 'type' | 'size' | 'updated_at') => {
  if (sortField.value === field) {
    sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc';
  } else {
    sortField.value = field;
    sortDir.value = 'asc';
  }
};

const compareEntry = (a: any, b: any) => {
  const field = sortField.value;
  let left: any = '';
  let right: any = '';
  switch (field) {
    case 'name':
      left = a.name;
      right = b.name;
      break;
    case 'type':
      left = a.type;
      right = b.type;
      break;
    case 'size':
      left = a.size || 0;
      right = b.size || 0;
      break;
    case 'updated_at':
      left = a.update_time || '';
      right = b.update_time || '';
      break;
  }
  if (left < right) return sortDir.value === 'asc' ? -1 : 1;
  if (left > right) return sortDir.value === 'asc' ? 1 : -1;
  return 0;
};

const formatSize = (size?: number) => {
  if (!size || size <= 0) return '--';
  const units = ['B', 'KB', 'MB', 'GB'];
  let value = size;
  let idx = 0;
  while (value > 1024 && idx < units.length - 1) {
    value /= 1024;
    idx++;
  }
  return `${value.toFixed(1)} ${units[idx]}`;
};

const formatTime = (time?: string) => {
  if (!time) return '--';
  let date: Date;
  if (!Number.isNaN(Number(time))) {
    date = new Date(Number(time) * 1000);
  } else {
    const parsed = Date.parse(time);
    date = Number.isNaN(parsed) ? new Date(time) : new Date(parsed);
  }
  if (Number.isNaN(date.getTime())) return time;
  return date.toLocaleString();
};

const stripRootPrefix = (value: string) => value.replace(/^Root\s*\/\s*/i, '').replace(/^Root\s*/i, '');

const breadcrumbDisplay = computed(() => {
  const list = [...breadcrumbs.value];
  if (appliedKeyword.value) {
    list.push({ name: `${appliedKeyword.value} 搜索结果`, token: '__search__' });
  }
  return list;
});

const openItem = async (item: DriveEntry) => {
  if (item.type === 'folder') {
    clearSearchState();
    await explorerStore.loadFolder(item.token);
    breadcrumbs.value = [...breadcrumbs.value, { name: item.name, token: item.token }];
    selection.value = new Set();
  }
};

const showAlert = (message: string) => {
  if (typeof window !== 'undefined' && typeof window.alert === 'function') {
    window.alert(message);
  } else {
    logOperation('showAlert', '提示信息', { message });
  }
};

const openPrompt = (title: string, defaultValue = '') => {
  promptState.visible = true;
  promptState.title = title;
  promptState.value = defaultValue;
  return new Promise<string | null>((resolve) => {
    promptState.resolve = resolve;
  });
};

const finishPrompt = (value: string | null) => {
  const trimmed = value?.trim();
  promptState.visible = false;
  const resolver = promptState.resolve;
  promptState.resolve = null;
  resolver?.(trimmed && trimmed.length ? trimmed : null);
};

const confirmPrompt = () => finishPrompt(promptState.value);
const cancelPrompt = () => finishPrompt(null);

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

const openUploadChoice = () => {
  uploadChoiceState.visible = true;
  return new Promise<'file' | 'folder' | null>((resolve) => {
    uploadChoiceState.resolve = resolve;
  });
};

const finishUploadChoice = (value: 'file' | 'folder') => {
  uploadChoiceState.visible = false;
  const resolver = uploadChoiceState.resolve;
  uploadChoiceState.resolve = null;
  resolver?.(value);
};

const cancelUploadChoice = () => {
  uploadChoiceState.visible = false;
  const resolver = uploadChoiceState.resolve;
  uploadChoiceState.resolve = null;
  resolver?.(null);
};

const isFolderEntry = (entry: DriveEntry) => (entry.type || '').toLowerCase() === 'folder';

const iconClass = (entry: DriveEntry) => {
  if (isFolderEntry(entry)) return 'icon-folder';
  const ext = (entry.name.split('.').pop() || '').toLowerCase();
  if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'webp', 'heic'].includes(ext)) return 'icon-image';
  if (['mp4', 'mov', 'avi', 'mkv', 'webm'].includes(ext)) return 'icon-video';
  if (['mp3', 'wav', 'flac', 'aac', 'ogg'].includes(ext)) return 'icon-audio';
  if (['pdf'].includes(ext)) return 'icon-pdf';
  if (['doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx'].includes(ext)) return 'icon-doc';
  if (['zip', 'rar', '7z', 'tar', 'gz'].includes(ext)) return 'icon-archive';
  return 'icon-file';
};

const pickPathsViaSystemDialog = async (): Promise<string[] | null> => {
  try {
    const items = await invoke<LocalPickerItem[]>('pick_entries_dialog', {
      payload: { multiple: true }
    });
    if (!items?.length) {
      return [];
    }
    return items
      .map((item) => item.path)
      .filter((value): value is string => typeof value === 'string' && !!value.length);
  } catch (error) {
    logError('pick_entries_dialog', '系统选择失败', error);
    return null;
  }
};

const iconLabel = (entry: DriveEntry) => {
  if (isFolderEntry(entry)) return 'DIR';
  const ext = (entry.name.split('.').pop() || '').toUpperCase();
  return ext.slice(0, 3) || 'FILE';
};


const navigateBreadcrumb = async (index: number) => {
  const list = breadcrumbDisplay.value;
  if (!list.length) return;
  const target = list[index];
  if (!target) return;
  if (target.token === '__search__') {
    await cancelSearch();
    return;
  }
  if (target.token === ROOT_PLACEHOLDER || (!target.token && index === 0)) {
    await cancelSearch(true);
    return;
  }
  if (!target.token) return;
  if (index === list.length - 1 && !appliedKeyword.value) return;
  await explorerStore.loadFolder(target.token);
  breadcrumbs.value = list
    .filter((crumb) => crumb.token && crumb.token !== '__search__')
    .slice(0, index + 1);
  selection.value = new Set();
};

const withBusy = async (task: () => Promise<void>, message = '操作执行中...') => {
  if (actionBusy.value) return;
  actionBusy.value = true;
  actionMessage.value = message;
  try {
    await task();
  } catch (error) {
    logError('withBusy', message, error);
    const tip = error instanceof Error ? error.message : String(error);
    showAlert(tip);
  } finally {
    actionBusy.value = false;
    actionMessage.value = '';
  }
};

const ensureParentToken = () => explorerStore.currentFolderToken || null;

const collectTargets = (items?: DriveEntry[]): DriveEntry[] => {
  if (items?.length) return items;
  if (!selectedEntries.value.length) {
    showAlert('请选择至少一个条目');
    return [];
  }
  return selectedEntries.value;
};

const clearSearchState = () => {
  keyword.value = '';
  appliedKeyword.value = '';
  globalResults.value = [];
  searching.value = false;
};

const runSearch = async (term: string) => {
  const normalized = term.trim();
  logOperation('runSearch', '开始搜索', { keyword: normalized });
  appliedKeyword.value = normalized;
  selection.value = new Set();
  globalResults.value = [];
  if (!normalized) {
    searching.value = false;
    return;
  }
  const runId = ++searchRunId.value;
  searching.value = true;
  const lower = normalized.toLowerCase();
  const apiKey = await securityStore.ensureServerKey();

  const searchFromRoot = async (entries: DriveEntry[], basePath: string, tenantLabel: string) => {
    const queue: Array<{ token: string; path: string; label: string }> = [];
    const processEntries = (items: DriveEntry[], parentPath: string, label: string) => {
      if (searchRunId.value !== runId) return;
      const matches: DriveEntry[] = [];
      for (const entry of items) {
        const path = `${parentPath} / ${entry.name}`;
        if (entry.name.toLowerCase().includes(lower)) {
          matches.push({ ...entry, path, tenant_name: label });
        }
        if ((entry.type || '').toLowerCase() === 'folder') {
          queue.push({ token: entry.token, path, label });
        }
      }
      if (matches.length) {
        globalResults.value = [...globalResults.value, ...matches];
      }
    };
    processEntries(entries, basePath, tenantLabel);
    while (queue.length && searchRunId.value === runId) {
      const current = queue.shift()!;
      const children = await invoke<DriveEntry[]>('list_folder_entries', {
        folder_token: current.token,
        folderToken: current.token,
        api_key: apiKey
      });
      processEntries(children, current.path, current.label);
    }
  };

  try {
    const tenantsList = availableTenants.value;
    if (!tenantsList.length) {
      showAlert(groupFilterActive.value ? '所选分组暂无启用的企业实例' : '暂无启用的企业实例');
      searching.value = false;
      return;
    }
    const factories = tenantsList.map(
      (tenant) => async () => {
        if (searchRunId.value !== runId) return;
        const { entries, rootToken } = await invoke<{
          rootToken: string;
          entries: DriveEntry[];
        }>('list_root_entries', {
          api_key: apiKey,
          apiKey,
          tenant_id: tenant.id,
          tenantId: tenant.id
        });
        if (searchRunId.value !== runId) return;
        cacheTenantRootToken(tenant.id, rootToken);
        await searchFromRoot(entries, tenant.name, tenant.name);
      }
    );
    await runWithLimit(factories, 5);
  } catch (error) {
    if (searchRunId.value === runId) {
      logError('runSearch', '搜索失败', error, { keyword: term });
      const message = error instanceof Error ? error.message : String(error);
      showAlert(message);
    }
  } finally {
    if (searchRunId.value === runId) {
      searching.value = false;
      logOperation('runSearch', '搜索结束', { keyword: normalized, results: globalResults.value.length });
    }
  }
};

const getEntriesForParent = async (parentToken: string) => {
  if (parentToken === explorerStore.currentFolderToken) {
    return rawEntries.value;
  }
  return fetchFolderEntries(parentToken);
};

const createRemoteFolder = async (parentToken: string, name: string) => {
  await ensureUniqueName(parentToken, name);
  const apiKey = await securityStore.ensureServerKey();
  logOperation('createRemoteFolder', '新建文件夹', { parentToken, name });
  return invoke<{ token: string }>('create_folder', {
    api_key: apiKey,
    payload: { parent_token: parentToken, name }
  });
};

const fetchFolderEntries = async (folderToken: string) => {
  const apiKey = await securityStore.ensureServerKey();
  return invoke<DriveEntry[]>('list_folder_entries', {
    folder_token: folderToken,
    folderToken,
    api_key: apiKey
  });
};

const loadAggregatedRootEntries = async (allowedIds?: string[]) => {
  if (allowedIds && !allowedIds.length) {
    return [];
  }
  const entriesMap = await fetchAggregatedRootMap(allowedIds && allowedIds.length ? allowedIds : undefined);
  const flattened: Array<DriveEntry & { tenant_label?: string }> = [];
  for (const [tenantId, list] of Object.entries(entriesMap)) {
    const label = tenants.value.find((item) => item.id === tenantId)?.name || '企业空间';
    list
      .filter((entry) => isFolderEntry(entry))
      .forEach((entry) => flattened.push({ ...entry, tenant_label: label }));
  }
  return flattened;
};

const loadDestinationEntries = async (token: string | null) => {
  destinationDialog.loading = true;
  try {
    let items: Array<DriveEntry & { tenant_label?: string }> = [];
    const allowedIds = getAllowedTenantIds();
    if (token) {
      const entries = await fetchFolderEntries(token);
      items = entries.filter((entry) => isFolderEntry(entry));
    } else {
      items = await loadAggregatedRootEntries(allowedIds && allowedIds.length ? allowedIds : undefined);
    }
    destinationDialog.items = items;
  } finally {
    destinationDialog.loading = false;
  }
};

const buildDestinationStack = () => {
  const crumbs = breadcrumbs.value
    .filter((crumb) => crumb.token && crumb.token !== '__search__')
    .map((crumb) => ({
      name: crumb.name,
      token: crumb.token === ROOT_PLACEHOLDER ? null : crumb.token
    }));
  if (!crumbs.length) {
    crumbs.push({
      name: rootLabel.value,
      token: explorerStore.currentFolderToken || null
    });
  }
  destinationDialog.stack = crumbs;
};

const buildBlockedTokens = (entries: DriveEntry[]) =>
  entries.filter((entry) => isFolderEntry(entry)).map((entry) => entry.token);

const openDestinationDialog = async (mode: 'move' | 'copy', blockedEntries: DriveEntry[] = []) => {
  destinationDialog.mode = mode;
  destinationDialog.visible = true;
  destinationDialog.blockedTokens = buildBlockedTokens(blockedEntries);
  buildDestinationStack();
  await loadDestinationEntries(destinationDialog.stack[destinationDialog.stack.length - 1]?.token || null);
  return new Promise<string | null>((resolve) => {
    destinationDialog.resolve = resolve;
  });
};

const enterDestinationFolder = async (folder: DriveEntry) => {
  if (isDestinationBlocked(folder.token)) {
    showAlert('不能选择当前选中的文件夹作为目标');
    return;
  }
  destinationDialog.stack = [...destinationDialog.stack, { name: folder.name, token: folder.token }];
  await loadDestinationEntries(folder.token);
};

const goDestinationUp = async () => {
  if (destinationDialog.stack.length <= 1) return;
  destinationDialog.stack = destinationDialog.stack.slice(0, -1);
  await loadDestinationEntries(destinationDialog.stack[destinationDialog.stack.length - 1]?.token || null);
};

const cancelDestination = () => {
  const resolver = destinationDialog.resolve;
  destinationDialog.visible = false;
  destinationDialog.resolve = null;
  resolver?.(null);
};

const confirmDestination = () => {
  const token = currentDestinationToken.value;
  if (!token) {
    showAlert('请选择目标文件夹');
    return;
  }
  if (isDestinationBlocked(token)) {
    showAlert('不能选择当前选中的文件夹作为目标');
    return;
  }
  const resolver = destinationDialog.resolve;
  destinationDialog.visible = false;
  destinationDialog.resolve = null;
  resolver?.(token);
};

const createFolderInDestination = async () => {
  const parentToken = currentDestinationToken.value;
  if (!parentToken) {
    showAlert('请选择目标文件夹');
    return;
  }
  const name = await openPrompt('新建文件夹名称');
  if (!name) return;
  try {
    await createRemoteFolder(parentToken, name);
    await loadDestinationEntries(parentToken);
    await refresh();
  } catch (error) {
    showAlert(error instanceof Error ? error.message : String(error));
  }
};

const ensureUniqueName = async (parentToken: string, name: string, excludeToken?: string) => {
  const entries = await getEntriesForParent(parentToken);
  const conflict = entries.find((entry) => entry.token !== excludeToken && entry.name === name);
  if (conflict) {
    throw new Error('同一目录下文件名必须唯一');
  }
};

const uploadFilesViaDialog = async (initialParent: string | null) => {
  const { token: parentToken } = await ensureWritableParentToken(initialParent);
  if (!parentToken) return;
  const files = await invoke<string[]>('pick_files_dialog', {
    payload: { multiple: true }
  });
  if (!files.length) return;
  const apiKey = await securityStore.ensureServerKey();
  for (const filePath of files) {
    const baseName = filePath.split(/[/\\]/).pop() || filePath;
    logOperation('uploadFile', '上传文件', { parentToken, filePath, baseName });
    try {
      await ensureUniqueName(parentToken, baseName);
    } catch (error) {
      showAlert(error instanceof Error ? error.message : String(error));
      continue;
    }
    await invoke('upload_file', {
      api_key: apiKey,
      payload: { parent_token: parentToken, file_path: filePath }
    });
  }
  await refresh();
};

const uploadFolderViaDialog = async (initialParent: string | null) => {
  const { token: parentToken } = await ensureWritableParentToken(initialParent);
  if (!parentToken) return;
  const dirPath = await invoke<string | null>('pick_directory_dialog');
  if (!dirPath) return;
  const name = basename(dirPath);
  if (!name) {
    showAlert('无法解析文件夹名称');
    return;
  }
  logOperation('uploadFolder', '选择文件夹上传', { parentToken, dirPath, name });
  try {
    await ensureUniqueName(parentToken, name);
  } catch (error) {
    showAlert(error instanceof Error ? error.message : String(error));
    return;
  }
  const apiKey = await securityStore.ensureServerKey();
  await invoke('upload_folder', {
    api_key: apiKey,
    payload: { parent_token: parentToken, dir_path: dirPath }
  });
  await refresh();
};

const handleUpload = async () => {
  logOperation('handleUpload', '触发上传', { parent: ensureParentToken() });
  const { token: parentToken } = await ensureWritableParentToken();
  if (!parentToken) return;
  const unifiedPaths = await pickPathsViaSystemDialog();
  if (Array.isArray(unifiedPaths)) {
    if (!unifiedPaths.length) return;
    await uploadPathsToParent(unifiedPaths, parentToken);
    return;
  }
  const mode = await openUploadChoice();
  if (!mode) return;
  if (mode === 'folder') {
    await withBusy(() => uploadFolderViaDialog(parentToken), '上传文件夹中...');
  } else {
    await withBusy(() => uploadFilesViaDialog(parentToken), '上传中...');
  }
};

const handleCreateFolder = () =>
  withBusy(async () => {
    logOperation('handleCreateFolder', '准备新建文件夹', { parent: ensureParentToken() });
    const { token: parentToken, tenantName: allocatedTenantName } = await ensureWritableParentToken();
    if (!parentToken) return;
    const name = await openPrompt('请输入文件夹名称');
    if (!name) return;
    try {
      await createRemoteFolder(parentToken, name);
      if (allocatedTenantName) {
        logOperation('handleCreateFolder', '已分配企业实例', {
          tenant: allocatedTenantName,
          parentToken
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      showAlert(message);
      throw error;
    }
    await refresh();
  }, '创建文件夹...');

const handleDownload = (items?: DriveEntry[]) =>
  withBusy(async () => {
    const targets = collectTargets(items);
    if (!targets.length) return;
    const destDir = await invoke<string | null>('pick_directory_dialog');
    if (!destDir) return;
    const apiKey = await securityStore.ensureServerKey();
    for (const entry of targets) {
      const isFolder = (entry.type || '').toLowerCase() === 'folder';
      logOperation('handleDownload', '下载条目', {
        token: entry.token,
        name: entry.name,
        type: entry.type,
        isFolder,
        destDir
      });
      if (isFolder) {
        await invoke('download_folder', {
          api_key: apiKey,
          payload: {
            token: entry.token,
            dest_dir: destDir,
            folder_name: entry.name
          }
        });
      } else {
        await invoke('download_file', {
          api_key: apiKey,
          payload: {
            token: entry.token,
            dest_dir: destDir,
            file_name: entry.name,
            size: entry.size || 0
          }
        });
      }
    }
  }, '下载中...');

const handleMove = async (items?: DriveEntry[]) => {
  const targets = collectTargets(items);
  if (!targets.length) return;
  logOperation('handleMove', '准备移动', { count: targets.length });
  const destination = await openDestinationDialog('move', targets);
  if (!destination) return;
  await performMove(targets, destination);
};

const handleCopy = async (items?: DriveEntry[]) => {
  const targets = collectTargets(items);
  if (!targets.length) return;
  logOperation('handleCopy', '准备复制', { count: targets.length });
  const destination = await openDestinationDialog('copy', targets);
  if (!destination) return;
  await withBusy(async () => {
    const apiKey = await securityStore.ensureServerKey();
    const single = targets.length === 1;
    for (const entry of targets) {
      const defaultName = `${entry.name} 副本`;
      const name = single ? await openPrompt(`请输入 ${entry.name} 的新名称`, defaultName) : defaultName;
      if (!name) continue;
      if (isFolderEntry(entry) && entry.token === destination) {
        showAlert(`不能将文件夹「${entry.name}」复制到自身`);
        continue;
      }
      logOperation('handleCopy', '复制条目', {
        source: entry.token,
        name: entry.name,
        newName: name,
        destination
      });
      try {
        await ensureUniqueName(destination, name);
      } catch (error) {
        showAlert(error instanceof Error ? error.message : String(error));
        continue;
      }
      await invoke('copy_file', {
        api_key: apiKey,
        payload: {
          token: entry.token,
          type: entry.type,
          target_parent: destination,
          name
        }
      });
    }
    await refresh();
  }, '复制中...');
};

const renameEntry = async (entry: DriveEntry, newName: string) => {
  const isFolder = entry.type?.toLowerCase() === 'folder';
  const parentToken = entry.parent_token || explorerStore.currentFolderToken;
  if (!parentToken) {
    showAlert('无法获取父目录，无法重命名');
    return;
  }
  await ensureUniqueName(parentToken, newName, entry.token);
  logOperation('renameEntry', '重命名', {
    token: entry.token,
    oldName: entry.name,
    newName,
    isFolder
  });
  if (isFolder) {
    await renameFolder(entry, newName, parentToken);
    return;
  }
  const apiKey = await securityStore.ensureServerKey();
  await invoke('copy_file', {
    api_key: apiKey,
    payload: {
      token: entry.token,
      type: entry.type,
      target_parent: parentToken,
      name: newName
    }
  });
  await invoke('delete_file', {
    api_key: apiKey,
    payload: { token: entry.token, type: entry.type }
  });
};

const renameFolder = async (entry: DriveEntry, newName: string, parentToken: string) => {
  logOperation('renameFolder', '重命名文件夹', {
    token: entry.token,
    newName,
    parentToken
  });
  const children = await fetchFolderEntries(entry.token);
  const newFolder = await createRemoteFolder(parentToken, newName);
  const apiKey = await securityStore.ensureServerKey();
  for (const child of children) {
    await invoke('move_file', {
      api_key: apiKey,
      payload: {
        token: child.token,
        type: child.type,
        target_parent: newFolder.token
      }
    });
  }
  await invoke('delete_file', {
    api_key: apiKey,
    payload: { token: entry.token, type: 'folder' }
  });
};

const handleRename = (items?: DriveEntry[]) =>
  withBusy(async () => {
    const targets = collectTargets(items);
    if (!targets.length) {
      showAlert('请选择需要重命名的条目');
      return;
    }
    for (const entry of targets) {
      const newName = await openPrompt(`请输入 ${entry.name} 的新名称`, entry.name);
      if (!newName || newName === entry.name) continue;
      try {
        await renameEntry(entry, newName);
      } catch (error) {
        showAlert(error instanceof Error ? error.message : String(error));
      }
    }
    selection.value = new Set();
    await refresh();
  }, '重命名中...');

const handleDelete = async (items?: DriveEntry[]) => {
  const targets = collectTargets(items);
  if (!targets.length) return;
  const confirmed = await openConfirm(`确认删除选中的 ${targets.length} 项吗？`);
  if (!confirmed) return;
  await withBusy(async () => {
    const apiKey = await securityStore.ensureServerKey();
    for (const entry of targets) {
      logOperation('handleDelete', '删除条目', {
        token: entry.token,
        name: entry.name,
        type: entry.type
      });
      await invoke('delete_file', {
        api_key: apiKey,
        payload: { token: entry.token, type: entry.type }
      });
    }
    selection.value = new Set();
    await refresh();
  }, '删除中...');
};

const pendingAction = (action: string) => {
  switch (action) {
    case 'upload':
      return handleUpload();
    case 'new-folder':
      return handleCreateFolder();
    case 'download':
      return handleDownload();
    case 'move':
      return handleMove();
    case 'copy':
      return handleCopy();
    case 'rename':
      return handleRename();
    default:
      return null;
  }
};

const applySearch = () => {
  runSearch(keyword.value.trim());
};

const cancelSearch = async (forceReload = false) => {
  if (!appliedKeyword.value && !globalResults.value.length && !forceReload) return;
  searchRunId.value++;
  keyword.value = '';
  appliedKeyword.value = '';
  globalResults.value = [];
  searching.value = false;
  selection.value = new Set();
  await performLoad(null);
};

const sortedTenants = computed(() => [...tenants.value].sort((a, b) => a.order - b.order));
const allActiveTenants = computed(() => sortedTenants.value.filter((tenant) => tenant.active));
const availableTenants = computed(() => {
  const allow = selectedGroupTenantIds.value;
  if (!allow.size) return [];
  return allActiveTenants.value.filter((tenant) => allow.has(tenant.id));
});

let rootLoadTicket = 0;
const performLoad = async (target?: string | null) => {
  const resolvedTenant = typeof target === 'string' && target.length ? target : null;
  const aggregate = !resolvedTenant;
  logOperation('performLoad', '请求目录数据', {
    group: selectedGroupId.value || null,
    target: target || null,
    resolvedTenant: resolvedTenant || null,
    aggregate
  });
  const currentTicket = ++rootLoadTicket;
  await loadRoot(resolvedTenant ?? undefined);
  if (currentTicket !== rootLoadTicket) return;
  logOperation('performLoad', '目录加载完成', {
    resolvedTenant: resolvedTenant || null,
    aggregate
  });
  await syncTreeAndSearch();
};

const handleGroupSelect = () => {
  logOperation('handleGroupSelect', '选择企业分组', {
    group: selectedGroupId.value || null
  });
};

watch(
  selectedGroupId,
  async (value, previous) => {
    if (value !== previous) {
      if (value && !availableTenants.value.length) {
        showAlert('所选分组暂无启用的企业实例');
      }
      await performLoad(null);
    }
  },
  { immediate: true }
);

watch(
  () => groups.value.map((group) => group.id).join('|'),
  () => {
    if (selectedGroupId.value && !groups.value.some((group) => group.id === selectedGroupId.value)) {
      selectedGroupId.value = '';
    }
  }
);

watch(
  () => availableTenants.value.map((item) => `${item.id}:${item.active}`).join('|'),
  async () => {
    if (groupFilterActive.value) {
      if (!availableTenants.value.length) {
        showAlert('所选分组暂无启用的企业实例');
      } else {
        await performLoad(null);
      }
    }
  }
);

watch(
  () => Array.from(allGroupTenantIds.value).sort().join('|'),
  async () => {
    if (!groupFilterActive.value) {
      await performLoad(null);
    }
  }
);

watch(
  () => rawEntries.value,
  () => {
    selection.value = new Set();
  }
);

onMounted(async () => {
  registerPointerTracking();
  await initFileDropListeners();
  await securityStore.ensureServerKey();
});

onBeforeUnmount(() => {
  cleanupPointerTracking();
  disposeFileDropListeners();
});

const performMove = async (targets: DriveEntry[], destination: string) => {
  await withBusy(async () => {
    logOperation('performMove', '移动文件', {
      destination,
      targets: targets.map((item) => item.name)
    });
    const apiKey = await securityStore.ensureServerKey();
    for (const entry of targets) {
      if (entry.parent_token === destination) continue;
      if (isFolderEntry(entry) && entry.token === destination) {
        showAlert(`不能将文件夹「${entry.name}」移动到自身`);
        continue;
      }
      try {
        await ensureUniqueName(destination, entry.name, entry.token);
      } catch (error) {
        showAlert(error instanceof Error ? error.message : String(error));
        continue;
      }
      await invoke('move_file', {
        api_key: apiKey,
        payload: {
          token: entry.token,
          type: entry.type,
          target_parent: destination
        }
      });
    }
    selection.value = new Set();
    await refresh();
  }, '移动中...');
};


const entryFromElement = (element?: Element | null): DriveEntry | null => {
  let current: Element | null | undefined = element;
  while (current) {
    const token = (current as HTMLElement).dataset?.entryToken;
    if (token) {
      const entry = entryMap.value.get(token);
      if (entry) {
        return entry;
      }
    }
    current = current.parentElement ?? undefined;
  }
  return null;
};

const isDroppableFolder = (entry: DriveEntry) => {
  const type = (entry.type || '').toLowerCase();
  return type === 'folder' || type === 'dir' || type.startsWith('folder');
};

const handleEntryPointerDown = (event: PointerEvent, entry: DriveEntry) => {
  if (event.button !== 0) return;
  event.preventDefault();
  dragState.pointerId = event.pointerId;
  dragState.startX = event.clientX;
  dragState.startY = event.clientY;
  dragState.x = event.clientX + 16;
  dragState.y = event.clientY + 16;
  dragState.tokens =
    selection.value.size && selection.value.has(entry.token)
      ? Array.from(selection.value)
      : [entry.token];
  dragState.pending = true;
  dragState.active = false;
  dragState.over = '';
  dragState.hoverEntry = null;
  window.addEventListener('pointermove', handlePointerMove, true);
  window.addEventListener('pointerup', handlePointerUp, true);
  window.addEventListener('pointercancel', handlePointerCancel, true);
};

const handlePointerMove = (event: PointerEvent) => {
  if (event.pointerId !== dragState.pointerId) return;
  const dx = event.clientX - dragState.startX;
  const dy = event.clientY - dragState.startY;
  if (!dragState.active && Math.hypot(dx, dy) > 5) {
    dragState.active = true;
    dragState.pending = false;
    dragState.suppressClick = true;
  }
  if (!dragState.active) return;
  dragState.x = event.clientX + 16;
  dragState.y = event.clientY + 16;
  const target = entryFromElement(document.elementFromPoint(event.clientX, event.clientY));
  if (target && isDroppableFolder(target) && !dragState.tokens.includes(target.token)) {
    dragState.over = target.token;
    dragState.hoverEntry = target;
  } else {
    dragState.over = '';
    dragState.hoverEntry = null;
  }
  event.preventDefault();
};

const finalizeDrag = async (target?: DriveEntry | null) => {
  const destination =
    target && isDroppableFolder(target) && !dragState.tokens.includes(target.token) ? target.token : '';
  if (dragState.active && destination) {
    const entries = dragState.tokens
      .map((token) => entryMap.value.get(token))
      .filter((item): item is DriveEntry => !!item);
    if (entries.length) {
      await performMove(entries, destination);
    }
  }
  dragState.pointerId = null;
  dragState.active = false;
  dragState.pending = false;
  dragState.over = '';
  dragState.hoverEntry = null;
  dragState.tokens = [];
  window.removeEventListener('pointermove', handlePointerMove, true);
  window.removeEventListener('pointerup', handlePointerUp, true);
  window.removeEventListener('pointercancel', handlePointerCancel, true);
  requestAnimationFrame(() => {
    dragState.suppressClick = false;
  });
};

const handlePointerUp = (event: PointerEvent) => {
  if (event.pointerId !== dragState.pointerId) return;
  void finalizeDrag(dragState.hoverEntry);
};

const handlePointerCancel = () => {
  void finalizeDrag(null);
};

const handleEntryClick = (entry: DriveEntry) => {
  if (dragState.active || dragState.pending || dragState.suppressClick) {
    dragState.suppressClick = false;
    return;
  }
  openItem(entry);
};

const handleGridOpen = (entry: DriveEntry) => {
  openItem(entry);
};

const handleGridClick = (event: MouseEvent, entry: DriveEntry) => {
  if (event.detail > 1) return;
  if (dragState.active || dragState.pending || dragState.suppressClick) {
    dragState.suppressClick = false;
    return;
  }
  toggleSelection(entry.token);
};

const registerPointerTracking = () => {
  const moveHandler = (event: MouseEvent) => {
    pointerPosition.x = event.clientX;
    pointerPosition.y = event.clientY;
    if (dropOverlay.visible) {
      dropOverlay.targetName = currentDropTargetName();
    }
  };
  const handler = moveHandler as EventListener;
  window.addEventListener('mousemove', handler);
  pointerTrackingHandlers.push({ type: 'mousemove', handler });
};

const cleanupPointerTracking = () => {
  pointerTrackingHandlers.forEach(({ type, handler, options }) => window.removeEventListener(type, handler, options));
  pointerTrackingHandlers.length = 0;
};

const currentDropTargetEntry = () => {
  const element = document.elementFromPoint(pointerPosition.x, pointerPosition.y);
  const entry = entryFromElement(element);
  return entry && isDroppableFolder(entry) ? entry : null;
};

const currentDropTargetName = () => {
  const entryName = currentDropTargetEntry()?.name;
  if (entryName) return entryName;
  const list = breadcrumbDisplay.value;
  const last = list.length ? list[list.length - 1].name : rootLabel.value;
  return last;
};

const classifyLocalPaths = async (paths: string[]) => {
  const files: string[] = [];
  const folders: string[] = [];
  for (const filePath of paths) {
    logDropEvent('classify path', filePath);
    try {
      const info = await invoke<{ is_dir: boolean; is_file: boolean }>('inspect_local_path', { path: filePath });
      if (info.is_dir) folders.push(filePath);
      else if (info.is_file) files.push(filePath);
      else logDropEvent('unknown path type', filePath, info);
    } catch (error) {
      logError('classifyLocalPaths', '读取路径失败', error, { path: filePath });
    }
  }
  logDropEvent('classification result', { files, folders });
  return { files, folders };
};

const basename = (input: string) => {
  if (!input) return '';
  const normalized = input.replace(/[/\\\\]+$/, '');
  const parts = normalized.split(/[/\\\\]/);
  return parts[parts.length - 1] || normalized;
};

const uploadPathsToParent = async (paths: string[], parentToken: string | null) => {
  const { token: resolvedParent, tenantName } = await ensureWritableParentToken(parentToken);
  if (!resolvedParent) return;
  logOperation('uploadPathsToParent', '拖拽上传', {
    parentToken: resolvedParent,
    count: paths.length,
    tenant: tenantName || undefined
  });
  const { files, folders } = await classifyLocalPaths(paths);
  if (!files.length && !folders.length) return;
  await withBusy(async () => {
    const apiKey = await securityStore.ensureServerKey();
    for (const filePath of files) {
      const name = basename(filePath);
      logOperation('uploadPathsToParent', '上传文件', { parentToken: resolvedParent, filePath });
      try {
        await ensureUniqueName(resolvedParent, name);
      } catch (error) {
        showAlert(error instanceof Error ? error.message : String(error));
        continue;
      }
      await invoke('upload_file', {
        api_key: apiKey,
        payload: { parent_token: resolvedParent, file_path: filePath }
      });
    }
    for (const dirPath of folders) {
      const name = basename(dirPath);
      logOperation('uploadPathsToParent', '上传文件夹', { parentToken: resolvedParent, dirPath });
      try {
        await ensureUniqueName(resolvedParent, name);
      } catch (error) {
        showAlert(error instanceof Error ? error.message : String(error));
        continue;
      }
      await invoke('upload_folder', {
        api_key: apiKey,
        payload: { parent_token: resolvedParent, dir_path: dirPath }
      });
    }
    await refresh();
    logOperation('uploadPathsToParent', '上传完成', {
      parentToken: resolvedParent,
      files: files.length,
      folders: folders.length
    });
  }, '上传中...');
};

const handleDroppedPaths = async (paths: string[]) => {
  if (!paths.length) return;
  const entry = currentDropTargetEntry();
  const parentToken = entry?.token || ensureParentToken();
  logDropEvent('handleDroppedPaths', { parentToken, entry: entry?.name });
  await uploadPathsToParent(paths, parentToken);
};

interface DragPayload {
  position?: { x: number; y: number };
  paths?: string[] | null;
}

const initFileDropListeners = async () => {
  try {
    const enter = await listen<DragPayload>('tauri://drag-enter', (event) => {
      logDropEvent('tauri drag enter', event.payload);
      if (event.payload?.position) {
        pointerPosition.x = event.payload.position.x;
        pointerPosition.y = event.payload.position.y;
      }
      dropOverlay.visible = true;
      dropOverlay.targetName = currentDropTargetName();
    });
    const over = await listen<DragPayload>('tauri://drag-over', (event) => {
      if (event.payload?.position) {
        pointerPosition.x = event.payload.position.x;
        pointerPosition.y = event.payload.position.y;
      }
      if (dropOverlay.visible) {
        dropOverlay.targetName = currentDropTargetName();
      }
    });
    const leave = await listen('tauri://drag-leave', () => {
      logDropEvent('tauri drag leave');
      dropOverlay.visible = false;
      dropOverlay.targetName = '';
    });
    const drop = await listen<DragPayload>('tauri://drag-drop', async (event) => {
      logDropEvent('tauri drag drop', event.payload);
      dropOverlay.visible = false;
      dropOverlay.targetName = '';
      const paths = Array.isArray(event.payload?.paths) ? (event.payload?.paths as string[]) : [];
      if (paths.length) {
        await handleDroppedPaths(paths);
      } else {
        logDropEvent('tauri drop payload empty');
      }
    });
    fileDropUnlisteners.push(enter, over, leave, drop);
    logDropEvent('tauri listeners registered');
  } catch (error) {
    logError('initFileDropListeners', '拖拽监听初始化失败', error);
  }
};

const disposeFileDropListeners = () => {
  fileDropUnlisteners.forEach((dispose) => dispose());
  fileDropUnlisteners.length = 0;
};


</script>

<style scoped>
.home-shell {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  background: #fff;
  min-height: 100%;
  padding: 1.75rem 1.5rem;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  align-items: center;
  flex-wrap: wrap;
  background: #fff;
  border: none;
  border-radius: 1rem;
  padding: 0.75rem 1rem;
}

.toolbar-left {
  display: flex;
  gap: 1rem;
  align-items: center;
  flex-wrap: wrap;
}

.mode-toggle {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.mode-toggle select {
  padding: 0.4rem 0.6rem;
  border-radius: 0.5rem;
  border: 1px solid #cbd5f5;
}

.search-box {
  display: flex;
  gap: 0.5rem;
}

.search-box input {
  padding: 0.4rem 0.6rem;
  border-radius: 999px;
  border: 1px solid #d7e5fc;
  background: #f8fbff;
}

.toolbar-right {
  display: flex;
  gap: 0.5rem;
  flex-wrap: wrap;
}

.breadcrumb {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  align-items: center;
  font-size: 0.9rem;
}

.breadcrumb .crumb {
  padding: 0.25rem 0.55rem;
  border-radius: 0.5rem;
  border: 1px solid transparent;
  background: #e7f1ff;
  color: #1666d1;
  cursor: pointer;
}

.breadcrumb .crumb.active {
  border-color: #1d7bff;
  background: #1d7bff;
  color: #fff;
}

.breadcrumb .separator {
  color: #9ea9c5;
  margin-right: 0.35rem;
}

.search-hint {
  margin-left: 0.5rem;
  color: #1b74ff;
  font-size: 0.85rem;
}

.hint {
  color: #1b74ff;
  font-size: 0.85rem;
}

.batch-actions {
  background: white;
  padding: 0.85rem 1.1rem;
  border-radius: 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border: 1px solid #e1e8f4;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.batch-actions .actions {
  display: flex;
  gap: 0.5rem;
}

.search-loading {
  margin: 0.5rem 0;
  color: #1b74ff;
  font-size: 0.95rem;
}

.list-view table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border-radius: 1rem;
  overflow: hidden;
  border: 1px solid #e3e9f3;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

.list-select-all {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  font-size: 0.9rem;
  color: #4a6387;
  margin: 0.5rem 0;
}

.list-view th,
.list-view td {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid #edf1f7;
  text-align: left;
}

.list-view th {
  background: #f5f8ff;
  cursor: pointer;
}

.name-cell {
  cursor: pointer;
  font-weight: 600;
  display: flex;
  align-items: center;
  gap: 0.45rem;
}

.row-actions {
  display: flex;
  gap: 0.5rem;
}

.btn:not(:disabled):hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 24px rgba(29, 123, 255, 0.2);
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.refresh-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
}

.spinner {
  width: 14px;
  height: 14px;
  border-radius: 50%;
  border: 2px solid #1d7bff;
  border-top-color: transparent;
  animation: spin 0.8s linear infinite;
}

.spinner.small {
  width: 12px;
  height: 12px;
  border-width: 2px;
}

.grid-view {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.grid-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(140px, 140px));
  justify-content: flex-start;
  gap: 1.25rem 1rem;
}

.grid-card {
  background: transparent;
  border-radius: 0.85rem;
  display: flex;
  justify-content: center;
  text-align: center;
}

.droppable-hover {
  outline: 2px dashed #1d7bff;
  outline-offset: 6px;
}

.grid-body {
  cursor: pointer;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.6rem;
  width: 100%;
  padding: 0.45rem 0.25rem;
  border-radius: 0.75rem;
  border: 1px solid transparent;
  user-select: none;
  transition: border-color 0.15s ease, background 0.15s ease;
}

.grid-body:hover {
  border-color: #bcd7ff;
  background: rgba(29, 123, 255, 0.08);
}

.grid-card.selected .grid-body {
  border-color: #1d7bff;
  background: rgba(29, 123, 255, 0.12);
}

.grid-icon-shell {
  display: flex;
  justify-content: center;
  width: 100%;
}

.grid-name {
  font-size: 0.95rem;
  font-weight: 600;
  color: #102042;
  word-break: break-word;
}

.grid-select-all {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  font-size: 0.9rem;
  color: #4a6387;
}

.empty {
  text-align: center;
  color: #9aa5b8;
  padding: 1rem;
}

.btn.danger {
  background: #ff4d4f;
  color: white;
  box-shadow: 0 10px 24px rgba(255, 77, 79, 0.2);
}

.destination-dialog {
  width: min(520px, 96vw);
  gap: 0.75rem;
}

.destination-header {
  border-bottom: 1px solid #e2e8f0;
  padding-bottom: 0.35rem;
}

.destination-header .path {
  margin: 0.35rem 0 0;
  font-size: 0.9rem;
  color: #7c8da6;
}

.destination-list {
  max-height: 360px;
  overflow-y: auto;
  border: 1px solid #e2e8f0;
  border-radius: 0.75rem;
  background: #fcfdff;
}

.destination-list .list-item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 0.6rem;
  padding: 0.65rem 1rem;
  border: none;
  background: transparent;
  border-bottom: 1px solid #eef2ff;
  cursor: pointer;
}

.destination-list .list-item.disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.destination-list .list-item:last-child {
  border-bottom: none;
}

.destination-list .list-item .entry-icon {
  width: 28px;
  height: 28px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.destination-list .list-text {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
}

.destination-list .tenant {
  color: #9aa5b8;
  font-size: 0.8rem;
}

.destination-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.flex-spacer {
  flex: 1;
}

.prompt-overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: grid;
  place-items: center;
  z-index: 9999;
}

.prompt-overlay.destination-layer {
  z-index: 9000;
}

.prompt-dialog {
  background: white;
  padding: 1.25rem;
  border-radius: 0.75rem;
  width: min(360px, 90vw);
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  box-shadow: 0 20px 45px rgba(15, 23, 42, 0.25);
}

.prompt-dialog input {
  padding: 0.5rem 0.75rem;
  border-radius: 0.5rem;
  border: 1px solid #cbd5f5;
}

.dialog-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
}

.dialog-actions.stacked {
  flex-direction: column;
  align-items: stretch;
}

.entry-icon {
  width: 22px;
  height: 22px;
  border-radius: 0.35rem;
  font-size: 0.65rem;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: #e0e7ff;
  color: #312e81;
  flex-shrink: 0;
}

.entry-icon.icon-folder {
  background: #fef9c3;
  color: #d97706;
}

.entry-icon.icon-image {
  background: #dbeafe;
  color: #1d4ed8;
}

.entry-icon.icon-video {
  background: #ffe4e6;
  color: #be123c;
}

.entry-icon.icon-audio {
  background: #ede9fe;
  color: #5b21b6;
}

.entry-icon.icon-pdf {
  background: #fee2e2;
  color: #991b1b;
}

.entry-icon.icon-doc {
  background: #e0f2fe;
  color: #0f172a;
}

.entry-icon.icon-archive {
  background: #fef9c3;
  color: #854d0e;
}

.entry-icon.icon-file {
  background: #f1f5f9;
  color: #334155;
}
.entry-icon.icon-large {
  width: 54px;
  height: 54px;
  font-size: 0.9rem;
}
.entry-icon svg {
  width: 100%;
  height: 100%;
  fill: currentColor;
}

.entry-name {
  flex: 1;
}

.grid-title {
  display: flex;
  align-items: center;
  gap: 0.4rem;
}

.drag-ghost {
  position: fixed;
  top: 0;
  left: 0;
  pointer-events: none;
  padding: 0.4rem 0.8rem;
  background: rgba(79, 70, 229, 0.9);
  color: #fff;
  border-radius: 0.45rem;
  font-size: 0.85rem;
  font-weight: 600;
  z-index: 10000;
}

.drop-overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9000;
}

.drop-overlay .drop-message {
  background: #fff;
  padding: 1rem 1.5rem;
  border-radius: 0.75rem;
  font-weight: 600;
  color: #111827;
  box-shadow: 0 20px 45px rgba(15, 23, 42, 0.25);
}
</style>
