import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';
import { useSecurityStore } from './securityStore';

export interface DriveEntry {
  token: string;
  name: string;
  type: string;
  parent_token?: string;
  size?: number;
  update_time?: string;
  path?: string;
  tenant_name?: string;
}

let activeLoadTicket = 0;
const logExplorer = (fn: string, action: string, payload?: Record<string, unknown>) => {
  const time = new Date().toISOString();
  const input = payload ? JSON.stringify(payload) : '{}';
  console.log(`${time} explorer.${fn} ${action} 输入=${input}`);
};

export const useExplorerStore = defineStore('explorer', {
  state: () => ({
    currentTenantId: '',
    currentFolderToken: '',
    entries: [] as DriveEntry[]
  }),
  actions: {
    async loadRoot(tenantId?: string | null, aggregate = false) {
      const ticket = ++activeLoadTicket;
      const securityStore = useSecurityStore();
      const apiKey = await securityStore.ensureServerKey();
      const basePayload: Record<string, any> = {
        api_key: apiKey,
        apiKey
      };
      const payload =
        tenantId && tenantId.length
          ? {
              ...basePayload,
              tenant_id: tenantId,
              tenantId
            }
          : {
              ...basePayload,
              aggregate
            };
      logExplorer('loadRoot', '请求目录', {
        tenantId: tenantId || null,
        aggregate,
        ticket,
        payloadTenant: 'tenant_id' in payload ? (payload as any).tenant_id : null
      });
      const response = await invoke<
        | { rootToken: string; entries: DriveEntry[] }
        | { aggregate: true; entries: Record<string, DriveEntry[]> }
      >('list_root_entries', payload);
      if ('aggregate' in response) {
        logExplorer('loadRoot', '聚合返回', {
          ticket,
          keys: Object.keys(response.entries)
        });
        if (ticket !== activeLoadTicket) return;
        this.currentTenantId = '';
        this.currentFolderToken = '';
        this.entries = Object.values(response.entries)
          .flat()
          .map(normalizeEntry);
        return;
      }
      const { rootToken, entries } = response;
      logExplorer('loadRoot', '单租户返回', {
        ticket,
        rootToken,
        count: entries.length
      });
      if (ticket !== activeLoadTicket) return;
      this.currentTenantId =
        tenantId ||
        ((payload as { tenant_id?: string }).tenant_id ?? this.currentTenantId);
      this.currentFolderToken = rootToken;
      this.entries = entries.map(normalizeEntry);
    },
    async loadFolder(folderToken: string) {
      const ticket = ++activeLoadTicket;
      const securityStore = useSecurityStore();
      const apiKey = await securityStore.ensureServerKey();
       logExplorer('loadFolder', '请求子目录', { folderToken, ticket });
      const entries = await invoke<DriveEntry[]>('list_folder_entries', {
        folder_token: folderToken,
        folderToken,
        api_key: apiKey
      });
      if (ticket !== activeLoadTicket) return;
      logExplorer('loadFolder', '子目录返回', { folderToken, count: entries.length, ticket });
      this.currentFolderToken = folderToken;
      this.entries = entries.map(normalizeEntry);
    },
    async reloadCurrent() {
      if (this.currentFolderToken) {
        await this.loadFolder(this.currentFolderToken);
      } else if (this.currentTenantId) {
        await this.loadRoot(this.currentTenantId);
      }
    }
  }
});

const normalizeEntry = (entry: Partial<DriveEntry> & { entry_type?: string }) => ({
  token: entry.token || '',
  name: entry.name || '',
  type: entry.type || entry.entry_type || '',
  parent_token: entry.parent_token,
  size: entry.size,
  update_time: entry.update_time
});
