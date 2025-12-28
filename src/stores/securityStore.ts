import { defineStore } from 'pinia';
import { invoke } from '@tauri-apps/api/core';

const LOCAL_KEY = 'feisync_api_key';

const randomKey = () => {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(24));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
};

export const useSecurityStore = defineStore('security', {
  state: () => ({
    apiKey: localStorage.getItem(LOCAL_KEY) || '',
    generating: null as Promise<string> | null
  }),
  actions: {
    setApiKey(key: string) {
      this.apiKey = key;
      if (key) {
        localStorage.setItem(LOCAL_KEY, key);
      } else {
        localStorage.removeItem(LOCAL_KEY);
      }
    },
    async ensureServerKey() {
      if (this.apiKey) return this.apiKey;
      if (!this.generating) {
        this.generating = (async () => {
        const existing = await invoke<string | null>('get_api_key');
          if (existing) {
            this.setApiKey(existing);
            this.generating = null;
            return existing;
          }
          const newKey = randomKey();
          await invoke('update_api_key', {
            payload: {
              currentKey: null,
              newKey
            }
          });
          this.setApiKey(newKey);
          this.generating = null;
          return newKey;
        })();
      }
      return this.generating!;
    },
    async regenerateKey() {
      const promise = (async () => {
        const newKey = randomKey();
        await invoke('update_api_key', {
          payload: {
            currentKey: this.apiKey || null,
            newKey
          }
        });
        this.setApiKey(newKey);
        return newKey;
      })();
      this.generating = promise;
      const result = await promise;
      this.generating = null;
      return result;
    }
  }
});
