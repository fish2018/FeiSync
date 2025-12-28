import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';
import { useSecurityStore } from './securityStore';

interface GroupApiRecord {
  id: string;
  name: string;
  remark?: string | null;
  tenant_ids: string[];
  api_key: string;
}

export interface GroupRecord {
  id: string;
  name: string;
  remark?: string | null;
  tenantIds: string[];
  apiKey: string;
}

const mapGroup = (item: GroupApiRecord): GroupRecord => ({
  id: item.id,
  name: item.name,
  remark: item.remark ?? '',
  tenantIds: [...item.tenant_ids],
  apiKey: item.api_key
});

export const useGroupStore = defineStore('group', {
  state: () => ({
    groups: [] as GroupRecord[],
    loading: false
  }),
  actions: {
    async ensureKey() {
      const securityStore = useSecurityStore();
      return securityStore.ensureServerKey();
    },
    async withAdminKey<T>(handler: (apiKey: string) => Promise<T>) {
      const securityStore = useSecurityStore();
      let apiKey = await securityStore.ensureServerKey();
      try {
        return await handler(apiKey);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.includes('缺少 API Key') || message.includes('API Key')) {
          apiKey = await securityStore.regenerateKey();
          return await handler(apiKey);
        }
        throw error;
      }
    },
    async fetchGroups() {
      this.loading = true;
      try {
        const list = await this.withAdminKey((apiKey) =>
          invoke<GroupApiRecord[]>('list_groups', {
            api_key: apiKey
          })
        );
        this.groups = list.map(mapGroup);
      } finally {
        this.loading = false;
      }
    },
    async addGroup(payload: { name: string; remark?: string; tenantIds: string[] }) {
      const created = await this.withAdminKey((apiKey) =>
        invoke<GroupApiRecord>('add_group', {
          api_key: apiKey,
          payload: {
            name: payload.name,
            remark: payload.remark ?? null,
            tenant_ids: payload.tenantIds
          }
        })
      );
      const mapped = mapGroup(created);
      this.groups = [...this.groups, mapped];
      return mapped;
    },
    async updateGroup(payload: { id: string; name: string; remark?: string; tenantIds: string[] }) {
      const updated = await this.withAdminKey((apiKey) =>
        invoke<GroupApiRecord>('update_group', {
          api_key: apiKey,
          payload: {
            group_id: payload.id,
            name: payload.name,
            remark: payload.remark ?? null,
            tenant_ids: payload.tenantIds
          }
        })
      );
      const mapped = mapGroup(updated);
      this.groups = this.groups.map((group) => (group.id === mapped.id ? mapped : group));
      return mapped;
    },
    async removeGroup(id: string) {
      await this.withAdminKey((apiKey) =>
        invoke('delete_group', {
          api_key: apiKey,
          payload: { group_id: id }
        })
      );
      this.groups = this.groups.filter((group) => group.id !== id);
    },
    async regenerateKey(id: string) {
      const record = await this.withAdminKey((apiKey) =>
        invoke<GroupApiRecord>('regenerate_group_key', {
          api_key: apiKey,
          groupId: id,
          group_id: id
        })
      );
      const mapped = mapGroup(record);
      this.groups = this.groups.map((group) => (group.id === mapped.id ? mapped : group));
      return mapped;
    }
  }
});
