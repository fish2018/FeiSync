<template>
  <section class="transfer-shell">
    <aside class="transfer-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        type="button"
        @click="switchTab(tab.key)"
      >
        <span class="tab-label">{{ tab.label }}</span>
        <span v-if="tab.count" class="tab-badge">{{ tab.count }}</span>
      </button>
    </aside>
    <section class="transfer-panel">
      <header class="panel-header">
        <div>
          <h2>传输中心</h2>
          <p>共有 {{ store.tasks.length }} 个任务，当前展示 {{ displayedTasks.length }} 个</p>
        </div>
        <div class="panel-actions">
          <template v-if="activeTab !== 'finished'">
            <button class="btn secondary" @click="pauseBulk" :disabled="!hasTargets">全部暂停</button>
            <button class="btn secondary" @click="resumeBulk" :disabled="!hasTargets">全部开始</button>
            <button class="btn secondary" @click="cancelBulk" :disabled="!hasTargets">全部删除</button>
          </template>
          <button class="btn secondary" v-else @click="clearFinishedRecords" :disabled="!displayedTasks.length">
            清空全部记录
          </button>
        </div>
      </header>
      <div class="overall-progress">
        <div class="progress-meta">
          <span>{{ progressLabel }}</span>
          <span class="speed" v-if="showSpeed">
            <span class="speed-title">{{ speedTitle }}</span>
            <span class="speed-value">{{ formatSpeed(overallSpeed) }}</span>
            <span class="eta" v-if="overallEtaText">
              <span class="eta-title">剩余</span>
              <span class="eta-value">{{ overallEtaText }}</span>
            </span>
          </span>
        </div>
        <div class="progress-bar">
          <span :style="{ width: `${progressPercent}%` }"></span>
        </div>
      </div>
      <div class="transfer-table" v-if="displayedTasks.length">
        <header class="table-head">
          <label>
            <input type="checkbox" :checked="allSelected" @change="toggleAll" />
            <span>文件名</span>
          </label>
          <span>大小</span>
          <span>状态</span>
          <span>操作</span>
        </header>
        <section class="table-body">
          <article class="table-row" v-for="task in displayedTasks" :key="task.id">
            <div class="name-cell">
              <label>
                <input type="checkbox" :checked="selection.has(task.id)" @change="toggleSelection(task.id)" />
              </label>
              <span class="entry-icon" :class="task.direction"></span>
              <div class="name">
                <strong>{{ task.name }}</strong>
                <small>{{ formatDate(task.updated_at) }}</small>
              </div>
            </div>
            <div class="size">{{ formatSize(task.size) }}</div>
            <div class="status">
              <span>{{ statusLabel(task) }}</span>
              <small v-if="task.message && task.status === 'failed'">{{ task.message }}</small>
            </div>
            <div class="actions">
              <button
                class="icon-btn"
                v-if="task.status === 'running'"
                title="暂停"
                @click="store.pauseTask(task.id)"
              >
                <span>⏸</span>
              </button>
              <button
                class="icon-btn"
                v-else-if="task.status === 'paused'"
                title="开始"
                @click="store.resumeTask(task.id)"
              >
                <span>▶</span>
              </button>
              <button
                class="icon-btn"
                v-if="task.status === 'failed'"
                title="重新开始"
                @click="store.restartTask(task.id)"
              >
                <span>↻</span>
              </button>
              <button
                class="icon-btn danger"
                v-if="task.status === 'running' || task.status === 'paused' || task.status === 'pending'"
                title="取消任务"
                @click="store.cancelTask(task.id)"
              >
                <span>✕</span>
              </button>
              <button
                class="icon-btn"
                v-else
                title="删除记录"
                @click="store.deleteTask(task.id)"
              >
                <span>🗑</span>
              </button>
              <button class="icon-btn" :disabled="!task.local_path" title="打开所在目录" @click="openLocation(task)">
                <span>🔍</span>
              </button>
            </div>
          </article>
        </section>
      </div>
      <p class="empty" v-else>当前列表为空</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { invoke } from '@tauri-apps/api/core';
import { useTransferStore, type TransferTask } from '@/stores/transferStore';

type TransferTab = 'upload' | 'download' | 'finished';

const store = useTransferStore();
const activeTab = ref<TransferTab>('upload');
const selection = ref<Set<string>>(new Set());

const isFinished = (task: TransferTask) => task.status === 'success' || task.status === 'failed';

const uploadTabCount = computed(
  () => store.tasks.filter((task) => task.direction === 'upload' && !isFinished(task)).length
);
const downloadTabCount = computed(
  () => store.tasks.filter((task) => task.direction === 'download' && !isFinished(task)).length
);
const finishedTabCount = computed(() => store.tasks.filter((task) => isFinished(task)).length);

const tabs = computed(() => [
  { key: 'upload' as TransferTab, label: '正在上传', count: uploadTabCount.value },
  { key: 'download' as TransferTab, label: '正在下载', count: downloadTabCount.value },
  { key: 'finished' as TransferTab, label: '传输完成', count: finishedTabCount.value }
]);

const displayedTasks = computed(() => {
  return store.tasks.filter((task) => {
    if (activeTab.value === 'upload') return task.direction === 'upload' && !isFinished(task);
    if (activeTab.value === 'download') return task.direction === 'download' && !isFinished(task);
    return isFinished(task);
  });
});

watch(
  displayedTasks,
  (list) => {
    selection.value = new Set([...selection.value].filter((id) => list.some((task) => task.id === id)));
  },
  { immediate: true }
);

const selectionList = computed(() => displayedTasks.value.filter((task) => selection.value.has(task.id)));
const operateTargets = computed(() => (selectionList.value.length ? selectionList.value : displayedTasks.value));
const allSelected = computed(
  () => displayedTasks.value.length > 0 && selection.value.size === displayedTasks.value.length
);
const hasTargets = computed(() => operateTargets.value.length > 0);

const progressPercent = computed(() => {
  if (activeTab.value === 'finished') {
    if (!store.tasks.length) return 0;
    return (store.finishedTasks.length / store.tasks.length) * 100;
  }
  const sized = displayedTasks.value.filter((task) => task.size > 0);
  if (!sized.length) return 0;
  const totalSize = sized.reduce((sum, task) => sum + task.size, 0);
  if (!totalSize) return 0;
  const finished = sized.reduce((sum, task) => sum + Math.min(task.transferred, task.size), 0);
  return (finished / totalSize) * 100;
});

const progressLabel = computed(() => {
  const ratio = `${progressPercent.value.toFixed(0)}%`;
  if (activeTab.value === 'finished') {
    return `全部任务完成度 ${ratio}`;
  }
  const label = activeTab.value === 'upload' ? '上传任务进度' : '下载任务进度';
  return `${label} ${ratio}`;
});

const showSpeed = computed(() => activeTab.value !== 'finished');
const speedTitle = computed(() => (activeTab.value === 'upload' ? '上传速度' : '下载速度'));

const overallSpeed = computed(() => {
  if (!showSpeed.value) return 0;
  const ids = displayedTasks.value.map((task) => task.id);
  return store.totalSpeed(ids);
});

const remainingBytes = computed(() => {
  if (!showSpeed.value) return 0;
  return displayedTasks.value.reduce((sum, task) => {
    if (task.status !== 'running' || !task.size) return sum;
    return sum + Math.max(task.size - task.transferred, 0);
  }, 0);
});

const overallEtaText = computed(() => {
  if (!showSpeed.value) return '';
  const speed = overallSpeed.value;
  const remaining = remainingBytes.value;
  if (!speed || speed <= 0 || !remaining) return '';
  const seconds = remaining / speed;
  return secondsToText(seconds);
});

const formatSize = (value?: number) => {
  if (!value || value <= 0) return '--';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let current = value;
  let idx = 0;
  while (current >= 1024 && idx < units.length - 1) {
    current /= 1024;
    idx++;
  }
  return `${current.toFixed(2)} ${units[idx]}`;
};

const formatSpeed = (value: number) => {
  if (!value || value <= 0) return '--';
  const units = ['B/s', 'KB/s', 'MB/s', 'GB/s'];
  let v = value;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(2)} ${units[i]}`;
};

const secondsToText = (seconds: number) => {
  if (!Number.isFinite(seconds) || seconds <= 0) return '';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  if (mins > 0) {
    return `${mins}分${secs}秒`;
  }
  return `${secs}秒`;
};

const etaText = (task: TransferTask) => {
  const speed = store.getSpeed(task.id);
  if (!speed || !task.size) return '';
  const remaining = Math.max(task.size - task.transferred, 0);
  if (remaining <= 0) return '';
  const seconds = remaining / speed;
  return secondsToText(seconds);
};

const statusLabel = (task: TransferTask) => {
  const speed = store.getSpeed(task.id);
  switch (task.status) {
    case 'running': {
      const eta = etaText(task);
      return `传输中 · ${formatSpeed(speed)}${eta ? ` · 剩余 ${eta}` : ''}`;
    }
    case 'paused':
      return '已暂停';
    case 'pending':
      return '排队中';
    case 'success':
      return '完成';
    case 'failed':
      return '失败';
    default:
      return '未知状态';
  }
};

const formatDate = (iso: string) => {
  const ts = Date.parse(iso);
  if (Number.isNaN(ts)) return iso;
  return new Date(ts).toLocaleString();
};

const toggleSelection = (id: string) => {
  const copy = new Set(selection.value);
  if (copy.has(id)) copy.delete(id);
  else copy.add(id);
  selection.value = copy;
};

const toggleAll = (event: Event) => {
  const checked = (event.target as HTMLInputElement).checked;
  if (!checked) {
    selection.value = new Set();
    return;
  }
  selection.value = new Set(displayedTasks.value.map((task) => task.id));
};

const switchTab = (tab: TransferTab) => {
  activeTab.value = tab;
  selection.value = new Set();
};

const idsFromTargets = () => operateTargets.value.map((task) => task.id);

const pauseBulk = async () => {
  const ids = idsFromTargets();
  if (!ids.length) return;
  await store.pauseMany(ids);
};

const resumeBulk = async () => {
  const ids = idsFromTargets();
  if (!ids.length) return;
  await store.resumeMany(ids);
};

const cancelBulk = async () => {
  const ids = idsFromTargets();
  if (!ids.length) return;
  await store.cancelMany(ids);
};

const clearFinishedRecords = () => store.clearFinished('finished');

const openLocation = async (task: TransferTask) => {
  if (!task.local_path) return;
  await invoke('reveal_local_path', { path: task.local_path });
};

onMounted(() => {
  store.initialize();
});
</script>

<style scoped>
.transfer-shell {
  display: grid;
  grid-template-columns: 200px 1fr;
  min-height: 100%;
  background: #f5f8ff;
}

.transfer-tabs {
  background: #f0f5ff;
  border-right: 1px solid #e0e7ff;
  padding: 2rem 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
}

.tab {
  border: none;
  border-radius: 0.85rem;
  padding: 0.85rem 1rem;
  background: transparent;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 600;
  color: #4a5a7f;
  cursor: pointer;
  transition: background 0.2s ease;
}

.tab.active {
  background: #fff;
  color: #0f62fe;
  box-shadow: 0 8px 20px rgba(15, 98, 254, 0.12);
}

.tab-badge {
  background: #ff5c5c;
  color: #fff;
  padding: 0 0.4rem;
  border-radius: 999px;
  font-size: 0.75rem;
}

.transfer-panel {
  background: #fff;
  padding: 2rem;
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.panel-header h2 {
  margin: 0;
  font-size: 1.5rem;
}

.panel-header p {
  margin: 0.2rem 0 0;
  color: #6b7280;
}

.panel-actions {
  display: flex;
  gap: 0.6rem;
  flex-wrap: wrap;
}

.overall-progress {
  background: #f8fbff;
  border-radius: 1rem;
  padding: 1rem 1.25rem;
  border: 1px solid #e0e8fb;
}

.progress-meta {
  display: flex;
  justify-content: space-between;
  color: #4b5563;
  font-size: 0.9rem;
  margin-bottom: 0.5rem;
}

.progress-meta .speed {
  display: flex;
  align-items: center;
  gap: 0.4rem;
}

.speed-title {
  color: #6b7280;
}

.speed-value {
  display: inline-block;
  min-width: 90px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.eta {
  display: flex;
  align-items: center;
  gap: 0.3rem;
}

.eta-title {
  color: #6b7280;
}

.eta-value {
  min-width: 80px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.progress-bar {
  width: 100%;
  height: 10px;
  border-radius: 999px;
  background: #e3ecff;
  overflow: hidden;
}

.progress-bar span {
  display: block;
  height: 100%;
  background: linear-gradient(90deg, #1f7cfe, #33c5ff);
  border-radius: inherit;
}

.transfer-table {
  border: 1px solid #e4eaf5;
  border-radius: 1.25rem;
  overflow: hidden;
}

.table-head,
.table-row {
  display: grid;
  grid-template-columns: 3fr 1fr 2fr 1.4fr;
  align-items: center;
  padding: 0.85rem 1.1rem;
  gap: 1rem;
}

.table-head {
  background: #f7f9ff;
  font-weight: 600;
  color: #4a5d7d;
}

.table-head label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.table-body {
  display: flex;
  flex-direction: column;
}

.table-row {
  border-top: 1px solid #eef2fb;
}

.name-cell {
  display: flex;
  align-items: center;
  gap: 0.8rem;
}

.entry-icon {
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: #e2ebff;
}

.entry-icon.download {
  background: #dff7f0;
}

.name {
  display: flex;
  flex-direction: column;
}

.name strong {
  font-size: 0.95rem;
}

.name small {
  color: #9ca3af;
}

.size,
.status {
  color: #4b5563;
}

.status small {
  display: block;
  color: #d93025;
}

.actions {
  display: flex;
  gap: 0.3rem;
  align-items: center;
}

.icon-btn {
  width: 34px;
  height: 34px;
  border-radius: 50%;
  border: 1px solid #dfe6f5;
  background: #fff;
  cursor: pointer;
  display: grid;
  place-items: center;
  font-size: 0.85rem;
}

.icon-btn.danger {
  border-color: #ffd2d2;
  color: #d93025;
}

.empty {
  text-align: center;
  color: #9ca3af;
  border: 1px dashed #dfe6f5;
  border-radius: 1rem;
  padding: 2rem;
}

@media (max-width: 1024px) {
  .transfer-shell {
    grid-template-columns: 1fr;
  }

  .transfer-tabs {
    flex-direction: row;
    border-right: none;
    border-bottom: 1px solid #e0e7ff;
  }

  .tab {
    flex: 1;
    justify-content: center;
  }

  .panel-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }

  .panel-actions {
    width: 100%;
    flex-wrap: wrap;
  }

  .table-head,
  .table-row {
    grid-template-columns: 2fr 1fr 1.5fr;
  }

  .actions {
    grid-column: span 3;
  }
}
</style>
