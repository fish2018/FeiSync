import { defineStore } from 'pinia';

const groupStorageKey = 'feisync:home:selected-group';

export const useUiStore = defineStore('ui', {
  state: () => ({
    activeGroupId: localStorage.getItem(groupStorageKey) || ''
  }),
  actions: {
    setActiveGroup(id: string) {
      this.activeGroupId = id;
      if (id) {
        localStorage.setItem(groupStorageKey, id);
      } else {
        localStorage.removeItem(groupStorageKey);
      }
    }
  }
});
