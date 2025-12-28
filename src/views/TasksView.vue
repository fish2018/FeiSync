<template>
  <section class="page-shell">
    <div class="card">
      <h2>任务中心</h2>
      <p>用于单/双向同步备份任务，功能建设中。</p>
    </div>
  </section>
</template>

<script setup lang="ts"></script>

<style scoped>
.page-shell {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
  background: #fff;
  min-height: 100%;
  padding: 1.75rem 1.5rem;
}

.card {
  background: white;
  border-radius: 1rem;
  padding: 1.5rem;
  border: 1px solid #e2eaf4;
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
}
</style>
