import { defineStore } from 'pinia';

export const useTaskStore = defineStore('task', {
  state: () => ({
    running: 0,
    success: 0,
    failed: 0
  }),
  actions: {
    updateCounts(payload: { running: number; success: number; failed: number }) {
      this.running = payload.running;
      this.success = payload.success;
      this.failed = payload.failed;
    }
  }
});
