<template>
  <section class="settings-shell">
    <TenantView />
    <GroupView />
  </section>
</template>

<script setup lang="ts">
import TenantView from '@/views/TenantView.vue';
import GroupView from '@/components/GroupView.vue';
</script>

<style scoped>
.settings-shell {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  background: #fff;
  min-height: 100%;
  padding: 1.75rem 1.5rem;
}
</style>
