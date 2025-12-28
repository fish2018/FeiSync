import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { useSecurityStore } from './securityStore';

export type TransferDirection = 'upload' | 'download';
export type TransferStatus = 'pending' | 'running' | 'paused' | 'success' | 'failed';
export type TransferKind = 'file_upload' | 'folder_upload' | 'file_download' | 'folder_download';

export interface UploadResumePayload {
  mode: 'upload_file';
  upload_id: string;
  block_size: number;
  next_seq: number;
  parent_token: string;
  file_path: string;
  file_name: string;
  size: number;
}

export interface DownloadResumePayload {
  mode: 'download_file';
  temp_path: string;
  target_path: string;
  downloaded: number;
  token: string;
  file_name: string;
}

export type TransferResume = UploadResumePayload | DownloadResumePayload;

export interface TransferTask {
  id: string;
  direction: TransferDirection;
  kind: TransferKind;
  name: string;
  tenant_id?: string;
  parent_token?: string;
  resource_token?: string;
  local_path?: string;
  remote_path?: string;
  size: number;
  transferred: number;
  status: TransferStatus;
  message?: string;
  created_at: string;
  updated_at: string;
  resume?: TransferResume | null;
}

const metricMap = new Map<string, { bytes: number; time: number }>();

const isRunning = (task: TransferTask) => task.status === 'running';
const isFinished = (task: TransferTask) => task.status === 'success';
const isQueued = (task: TransferTask) =>
  task.status === 'running' || task.status === 'paused' || task.status === 'pending';

export const useTransferStore = defineStore('transfer', {
  state: () => ({
    tasks: [] as TransferTask[],
    initialized: false,
    loading: false,
    unlisten: null as UnlistenFn | null,
    speeds: {} as Record<string, number>
  }),
  getters: {
    runningCount: (state) => state.tasks.filter((task) => isRunning(task)).length,
    badgeCount: (state) => state.tasks.filter((task) => isQueued(task)).length,
    uploadQueueCount: (state) =>
      state.tasks.filter((task) => task.direction === 'upload' && isQueued(task)).length,
    downloadQueueCount: (state) =>
      state.tasks.filter((task) => task.direction === 'download' && isQueued(task)).length,
    finishedTasks: (state) => state.tasks.filter((task) => isFinished(task)),
    hasFinished(): boolean {
      return this.finishedTasks.length > 0;
    }
  },
  actions: {
    async initialize() {
      if (this.initialized) return;
      await this.fetch();
      this.unlisten = await listen<TransferTask>('transfer://event', (event) => {
        this.upsert(event.payload);
      });
      this.initialized = true;
    },
    async fetch() {
      this.loading = true;
      try {
        const items = await this.invokeWithKey<TransferTask[]>('list_transfer_tasks');
        this.tasks = items;
        const now = Date.now();
        for (const task of items) {
          metricMap.set(task.id, { bytes: task.transferred, time: now });
          this.speeds[task.id] = 0;
        }
      } finally {
        this.loading = false;
      }
    },
    computeSpeed(task: TransferTask) {
      const now = Date.now();
      const previous = metricMap.get(task.id);
      let speed = 0;
      if (previous && task.status === 'running') {
        const deltaBytes = task.transferred - previous.bytes;
        const deltaSeconds = Math.max((now - previous.time) / 1000, 0.001);
        if (deltaBytes >= 0) {
          speed = deltaBytes / deltaSeconds;
        }
      }
      metricMap.set(task.id, { bytes: task.transferred, time: now });
      this.speeds = { ...this.speeds, [task.id]: speed };
    },
    upsert(task: TransferTask) {
      const index = this.tasks.findIndex((item) => item.id === task.id);
      if (index >= 0) {
        const clone = [...this.tasks];
        clone.splice(index, 1, task);
        this.tasks = clone;
      } else {
        this.tasks = [task, ...this.tasks];
      }
      this.computeSpeed(task);
    },
    getSpeed(id: string) {
      return this.speeds[id] || 0;
    },
    totalSpeed(taskIds?: string[]) {
      if (taskIds) {
        if (!taskIds.length) return 0;
        return taskIds.reduce((sum, id) => sum + (this.speeds[id] || 0), 0);
      }
      return Object.values(this.speeds).reduce((sum, speed) => sum + speed, 0);
    },
    async invokeWithKey<T = unknown>(command: string, payload: Record<string, unknown> = {}) {
      const securityStore = useSecurityStore();
      const apiKey = await securityStore.ensureServerKey();
      return invoke<T>(command, { api_key: apiKey, ...payload });
    },
    async pauseTask(id: string) {
      await this.invokeWithKey('pause_active_transfer', { task_id: id, taskId: id });
    },
    async resumeTask(id: string) {
      await this.invokeWithKey('resume_transfer_task', { task_id: id, taskId: id });
    },
    async cancelTask(id: string) {
      await this.invokeWithKey('cancel_transfer_task', { task_id: id, taskId: id });
    },
    async deleteTask(id: string) {
      await this.invokeWithKey('delete_transfer_task', { task_id: id, taskId: id });
      await this.fetch();
    },
    async restartTask(id: string) {
      await this.invokeWithKey('resume_transfer_task', { task_id: id, taskId: id });
    },
    async pauseMany(ids: string[]) {
      await Promise.all(ids.map((id) => this.pauseTask(id)));
    },
    async resumeMany(ids: string[]) {
      await Promise.all(ids.map((id) => this.resumeTask(id)));
    },
    async cancelMany(ids: string[]) {
      await Promise.all(ids.map((id) => this.cancelTask(id)));
    },
    async clearFinished(mode: 'finished' | 'success' | 'failed' = 'finished') {
      await this.invokeWithKey('clear_transfer_history', { mode });
      await this.fetch();
    }
  }
});
