import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';
import { useSecurityStore } from './securityStore';
import { useGroupStore } from './groupStore';

export type TenantPlatform = 'lark' | 'feishu';

export interface TenantInstance {
  id: string;
  name: string;
  app_id: string;
  app_secret?: string;
  quota_gb: number;
  used_gb: number;
  active: boolean;
  platform: TenantPlatform;
  order: number;
}

export const useTenantStore = defineStore('tenant', {
  state: () => ({
    tenants: [] as TenantInstance[],
    loading: false
  }),
  getters: {
    totalQuota: (state) => state.tenants.reduce((sum, t) => sum + t.quota_gb, 0),
    totalUsed: (state) => state.tenants.reduce((sum, t) => sum + t.used_gb, 0)
  },
  actions: {
    async withApiKey<T>(handler: (apiKey: string) => Promise<T>) {
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
    async fetchTenants() {
      this.loading = true;
      try {
        this.tenants = await this.withApiKey((apiKey) =>
          invoke<TenantInstance[]>('list_tenants', {
            api_key: apiKey
          })
        );
      } finally {
        this.loading = false;
      }
    },
    async addTenant(payload: { name: string; app_id: string; app_secret: string; quota_gb: number; platform: TenantPlatform }) {
      await this.withApiKey((apiKey) =>
        invoke('add_tenant', {
          api_key: apiKey,
          payload
        })
      );
      await this.fetchTenants();
    },
    async refreshTenant(id: string) {
      await this.withApiKey((apiKey) =>
        invoke('refresh_tenant_token', {
          api_key: apiKey,
          tenantId: id
        })
      );
      await this.fetchTenants();
    },
    async updateTenant(payload: { tenant_id: string; name?: string; app_id?: string; app_secret?: string; quota_gb?: number; active?: boolean; platform?: TenantPlatform }) {
      await this.withApiKey((apiKey) =>
        invoke('update_tenant_meta', {
          api_key: apiKey,
          payload
        })
      );
      await this.fetchTenants();
    },
    async removeTenant(id: string) {
      await this.withApiKey((apiKey) =>
        invoke('remove_tenant', {
          api_key: apiKey,
          payload: { tenant_id: id }
        })
      );
      await this.fetchTenants();
      const groupStore = useGroupStore();
      await groupStore.fetchGroups();
    },
    async reorder(items: Array<{ tenant_id: string; order: number }>) {
      await this.withApiKey((apiKey) =>
        invoke('reorder_tenants', {
          api_key: apiKey,
          payload: items
        })
      );
      const orderMap = new Map(items.map((item) => [item.tenant_id, item.order]));
      this.tenants = this.tenants
        .map((tenant) => ({
          ...tenant,
          order: orderMap.get(tenant.id) ?? tenant.order
        }))
        .sort((a, b) => a.order - b.order);
    },
    async fetchTenantDetail(id: string) {
      return this.withApiKey((apiKey) =>
        invoke<TenantInstance & { app_secret?: string }>('get_tenant_detail', {
          api_key: apiKey,
          tenantId: id
        })
      );
    }
  }
});
