<template>
  <section class="page-shell">
    <div class="card">
      <h2>关于 FeiSync</h2>
      <p>FeiSync 是基于 Feishu/Lark 云空间 API 的统一管理工具，用于备份、同步与开放接口封装。</p>
      <ul>
        <li>版本：0.1.0</li>
        <li>技术栈：Tauri + Vue3</li>
      </ul>
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
  background: #fff;
  border: 1px solid #e2eaf4;
  border-radius: 1rem;
  padding: 1.25rem;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
}

ul {
  padding-left: 1rem;
}
</style>
