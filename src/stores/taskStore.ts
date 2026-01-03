import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';
import { useSecurityStore } from './securityStore';
import {
  computeNextOccurrence,
  describeCronExpression,
  isCronExpressionValid,
  normalizeCronExpression
} from '@/utils/cron';

export type TaskDirection = 'cloud_to_local' | 'local_to_cloud' | 'bidirectional';
export type TaskDetectionMode = 'metadata' | 'size' | 'checksum';
export type ConflictStrategy = 'prefer_remote' | 'prefer_local' | 'newest';
export type TaskStatus = 'idle' | 'scheduled' | 'running' | 'success' | 'failed';

export interface SyncTask {
  id: string;
  name: string;
  direction: TaskDirection;
  groupId: string;
  groupName?: string;
  tenantId: string;
  tenantName?: string;
  remoteFolderToken: string;
  remoteLabel: string;
  localPath: string;
  schedule: string;
  enabled: boolean;
  detection: TaskDetectionMode;
  conflict: ConflictStrategy;
  propagateDelete: boolean;
  includePatterns: string[];
  excludePatterns: string[];
  notes?: string;
  createdAt: string;
  updatedAt: string;
  nextRunAt: string | null;
  lastRunAt: string | null;
  lastStatus: TaskStatus;
  lastMessage?: string;
  consecutiveFailures: number;
  linkedTransferIds?: string[];
}

export interface SyncLogEntry {
  task_id: string;
  timestamp: string;
  level: string;
  message: string;
}

export interface TaskPayload {
  name: string;
  direction: TaskDirection;
  groupId: string;
  groupName?: string;
  tenantId: string;
  tenantName?: string;
  remoteFolderToken: string;
  remoteLabel: string;
  localPath: string;
  schedule: string;
  enabled: boolean;
  detection: TaskDetectionMode;
  conflict: ConflictStrategy;
  propagateDelete: boolean;
  includePatterns: string[];
  excludePatterns: string[];
  notes?: string;
}

const sanitizePatterns = (patterns: string[]) => {
  const uniq = new Set(patterns.map((item) => item.trim()).filter((item) => !!item));
  return [...uniq.values()];
};

const fromBackendTask = (task: any): SyncTask => ({
  id: task.id,
  name: task.name,
  direction: task.direction,
  groupId: task.group_id,
  groupName: task.group_name || undefined,
  tenantId: task.tenant_id,
  tenantName: task.tenant_name || undefined,
  remoteFolderToken: task.remote_folder_token,
  remoteLabel: task.remote_label,
  localPath: task.local_path,
  schedule: task.schedule,
  enabled: task.enabled,
  detection: task.detection,
  conflict: task.conflict,
  propagateDelete: Boolean(task.propagate_delete),
  includePatterns: task.include_patterns || [],
  excludePatterns: task.exclude_patterns || [],
  notes: task.notes || undefined,
  createdAt: task.created_at,
  updatedAt: task.updated_at,
  nextRunAt: task.next_run_at || null,
  lastRunAt: task.last_run_at || null,
  lastStatus: task.last_status,
  lastMessage: task.last_message || undefined,
  consecutiveFailures: task.consecutive_failures || 0,
  linkedTransferIds: task.linked_transfer_ids || []
});

const normalizeTask = (task: SyncTask): SyncTask => {
  const schedule = normalizeCronExpression(task.schedule || '* * * * *');
  return {
    ...task,
    schedule,
    includePatterns: sanitizePatterns(task.includePatterns || []),
    excludePatterns: sanitizePatterns(task.excludePatterns || [])
  };
};

const mapToBackendPayload = (payload: Partial<TaskPayload>) => {
  const data: Record<string, unknown> = {};
  if ('name' in payload && payload.name !== undefined) data.name = payload.name;
  if ('direction' in payload && payload.direction !== undefined) data.direction = payload.direction;
  if ('groupId' in payload && payload.groupId !== undefined) data.group_id = payload.groupId;
  if ('groupName' in payload) data.group_name = payload.groupName;
  if ('tenantId' in payload && payload.tenantId !== undefined) data.tenant_id = payload.tenantId;
  if ('tenantName' in payload) data.tenant_name = payload.tenantName;
  if ('remoteFolderToken' in payload && payload.remoteFolderToken !== undefined) {
    data.remote_folder_token = payload.remoteFolderToken;
  }
  if ('remoteLabel' in payload && payload.remoteLabel !== undefined) data.remote_label = payload.remoteLabel;
  if ('localPath' in payload && payload.localPath !== undefined) data.local_path = payload.localPath;
  if ('schedule' in payload && payload.schedule !== undefined) data.schedule = payload.schedule;
  if ('enabled' in payload && payload.enabled !== undefined) data.enabled = payload.enabled;
  if ('detection' in payload && payload.detection !== undefined) data.detection = payload.detection;
  if ('conflict' in payload && payload.conflict !== undefined) data.conflict = payload.conflict;
  if ('propagateDelete' in payload && payload.propagateDelete !== undefined) {
    data.propagate_delete = payload.propagateDelete;
  }
  if ('includePatterns' in payload && payload.includePatterns !== undefined) {
    data.include_patterns = sanitizePatterns(payload.includePatterns || []);
  }
  if ('excludePatterns' in payload && payload.excludePatterns !== undefined) {
    data.exclude_patterns = sanitizePatterns(payload.excludePatterns || []);
  }
  if ('notes' in payload) data.notes = payload.notes;
  return data;
};

export const useTaskStore = defineStore('task', {
  state: () => ({
    tasks: [] as SyncTask[],
    initialized: false,
    loading: false
  }),
  getters: {
    running(state) {
      return state.tasks.filter((task) => task.lastStatus === 'running').length;
    },
    success(state) {
      return state.tasks.filter((task) => task.lastStatus === 'success').length;
    },
    failed(state) {
      return state.tasks.filter((task) => task.lastStatus === 'failed').length;
    },
    total(state) {
      return state.tasks.length;
    }
  },
  actions: {
    async withApiKey<T>(handler: (apiKey: string) => Promise<T>) {
      const securityStore = useSecurityStore();
      const apiKey = await securityStore.ensureServerKey();
      return handler(apiKey);
    },
    async initialize() {
      if (this.initialized) return;
      await this.fetchTasks();
      this.initialized = true;
    },
    async fetchTasks() {
      this.loading = true;
      try {
        const list = await this.withApiKey((apiKey) =>
          invoke<any[]>('list_sync_tasks', { api_key: apiKey })
        );
        this.tasks = list.map((task) => normalizeTask(fromBackendTask(task)));
      } finally {
        this.loading = false;
      }
    },
    validateCron(expression: string) {
      return isCronExpressionValid(expression);
    },
    cronDescription(expression: string) {
      return describeCronExpression(expression);
    },
    previewNextRun(expression: string, enabled = true) {
      if (!enabled) return null;
      const normalized = normalizeCronExpression(expression);
      try {
        const next = computeNextOccurrence(normalized);
        return next ? next.toISOString() : null;
      } catch {
        return null;
      }
    },
    async createTask(payload: TaskPayload) {
      if (!this.validateCron(payload.schedule)) {
        throw new Error('Cron 表达式无效');
      }
      const mapped = mapToBackendPayload(payload) as Record<string, unknown>;
      const record = await this.withApiKey((apiKey) =>
        invoke<any>('create_sync_task', {
          api_key: apiKey,
          payload: mapped
        })
      );
      const normalized = normalizeTask(fromBackendTask(record));
      this.tasks = [normalized, ...this.tasks];
      return normalized;
    },
    async updateTask(id: string, payload: Partial<TaskPayload>) {
      const mapped = mapToBackendPayload(payload);
      const record = await this.withApiKey((apiKey) =>
        invoke<any>('update_sync_task', {
          api_key: apiKey,
          payload: { task_id: id, ...mapped }
        })
      );
      const normalized = normalizeTask(fromBackendTask(record));
      this.tasks = this.tasks.map((task) => (task.id === id ? normalized : task));
      return normalized;
    },
    async removeTask(id: string) {
      await this.withApiKey((apiKey) =>
        invoke('delete_sync_task', {
          api_key: apiKey,
          payload: { task_id: id }
        })
      );
      this.tasks = this.tasks.filter((task) => task.id !== id);
    },
    async toggleTask(id: string, enabled: boolean) {
      return this.updateTask(id, { enabled });
    },
    async duplicateTask(id: string) {
      const original = this.tasks.find((task) => task.id === id);
      if (!original) return null;
      const payload: TaskPayload = {
        name: `${original.name} 副本`,
        direction: original.direction,
        groupId: original.groupId,
        groupName: original.groupName,
        tenantId: original.tenantId,
        tenantName: original.tenantName,
        remoteFolderToken: original.remoteFolderToken,
        remoteLabel: original.remoteLabel,
        localPath: original.localPath,
        schedule: original.schedule,
        enabled: false,
        detection: original.detection,
        conflict: original.conflict,
        propagateDelete: original.propagateDelete,
        includePatterns: [...original.includePatterns],
        excludePatterns: [...original.excludePatterns],
        notes: original.notes
      };
      return this.createTask(payload);
    },
    async triggerTask(id: string) {
      const now = new Date().toISOString();
      this.tasks = this.tasks.map((task) =>
        task.id === id
          ? {
              ...task,
              lastStatus: 'running',
              lastMessage: '同步任务正在执行中…',
              lastRunAt: now
            }
          : task
      );
      await this.withApiKey((apiKey) =>
        invoke('trigger_sync_task', {
          api_key: apiKey,
          payload: { task_id: id }
        })
      );
      await this.fetchTasks();
    },
    async fetchLogs(taskId: string, limit = 100) {
      return this.withApiKey((apiKey) =>
        invoke<SyncLogEntry[]>('list_sync_logs', {
          api_key: apiKey,
          payload: { task_id: taskId, limit }
        })
      );
    }
  }
});
