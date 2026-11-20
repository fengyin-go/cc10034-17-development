<template>
  <section class="batch-sync" data-testid="batch-sync-panel">
    <header class="batch-head">
      <h3>批量结构同步</h3>
      <p class="page-desc">
        把一批表结构变更日志按依赖逐条归并，生成批次字段映射数据集，再向目标引擎发布；
        执行结果回写 DDL 回执清单，重复提交已生效批次时目标引擎幂等跳过。
      </p>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">已登记 DDL</span>
        <strong class="stat-value">{{ receiptSummary.total }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已应用</span>
        <strong class="stat-value">{{ receiptSummary.applied }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">队列中</span>
        <strong class="stat-value">{{ receiptSummary.queued }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">目标引擎生效动作</span>
        <strong class="stat-value">{{ catalog?.apply_count ?? '—' }}</strong>
      </article>
    </div>

    <form class="batch-form" @submit.prevent="ingestDdl">
      <h4>① 登记带原始位点的 DDL 变更日志</h4>
      <div class="form-grid">
        <label><span>数据库</span><input v-model="ddlForm.database" data-testid="ddl-database" required /></label>
        <label><span>表名</span><input v-model="ddlForm.table_name" data-testid="ddl-table" required /></label>
        <label><span>Binlog 文件</span><input v-model="ddlForm.binlog_file" data-testid="ddl-binlog" required /></label>
        <label><span>位点偏移</span><input v-model.number="ddlForm.binlog_position" data-testid="ddl-position" type="number" required /></label>
        <label class="wide"><span>DDL 语句</span><input v-model="ddlForm.ddl_statement" data-testid="ddl-statement" required /></label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="ddl-submit" type="submit">登记并分析 DDL</button>
        <span v-if="lastIngested" class="hint">最近登记：{{ lastIngested.record_id }}（{{ lastIngested.column_changes.length }} 个改列动作）</span>
      </div>
    </form>

    <form class="batch-form" @submit.prevent="build">
      <h4>② 归并生成批次字段映射数据集</h4>
      <div class="form-grid">
        <label><span>源表</span><input v-model="batchForm.source_table" data-testid="batch-source" required /></label>
        <label><span>目标表</span><input v-model="batchForm.target_table" data-testid="batch-target" required /></label>
        <label><span>DDL 记录 ID（逗号分隔）</span>
          <input v-model="ddlIdsText" data-testid="batch-ddl-ids" required />
        </label>
        <label><span>类型转换策略</span>
          <select v-model="batchForm.type_strategy" data-testid="batch-strategy">
            <option value="doris">doris</option>
            <option value="hive">hive</option>
            <option value="直接映射">直接映射</option>
          </select>
        </label>
        <label class="wide"><span>初始列（column:type，逗号分隔，可空）</span>
          <input v-model="initialColumnsText" data-testid="batch-initial" placeholder="order_id:BIGINT, amount:DECIMAL(10,2)" />
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="batch-build" type="submit">归并生成批次</button>
        <button class="btn" data-testid="batch-publish" type="button" :disabled="!batch" @click="publish">发布到目标引擎</button>
        <button class="btn ghost" data-testid="batch-replay" type="button" :disabled="!batch" @click="replay">重放原批</button>
        <button class="btn ghost" data-testid="batch-refresh" type="button" @click="refreshViews">刷新回执与目标列</button>
      </div>
    </form>

    <p v-if="message" class="hint" data-testid="batch-message">{{ message }}</p>

    <div v-if="batch" class="batch-result">
      <h4>批次字段映射数据集（{{ batch.batch_id }} / {{ batch.status }}）</h4>
      <p class="hint">
        依赖组：{{ batch.dependency_groups.length }} 个 ·
        字段映射：{{ batch.mapping_dataset.length }} 条 ·
        来源位点：{{ batch.source_positions.map((p) => `${p.binlog_file}:${p.binlog_position}`).join(' → ') }}
      </p>
      <table class="data-table">
        <thead>
        <tr>
          <th>映射ID</th><th>依赖组</th><th>源列</th><th>目标列</th>
          <th>源类型</th><th>目标类型</th><th>改列动作链</th><th>来源 DDL / 位点</th>
        </tr>
        </thead>
        <tbody>
        <tr v-for="entry in batch.mapping_dataset" :key="entry.mapping_id" :data-testid="`mapping-${entry.mapping_id}`">
          <td>{{ entry.mapping_id }}</td>
          <td>{{ entry.dependency_group }}</td>
          <td>{{ entry.source_column }}</td>
          <td>{{ entry.target_column }}</td>
          <td>{{ entry.source_type || '—' }}</td>
          <td>{{ entry.target_type || '—' }}</td>
          <td>{{ entry.actions.join(' → ') }}</td>
          <td>
            <span v-for="v in entry.source_versions" :key="v.ddl_record_id" class="version-chip">
              {{ v.ddl_record_id }}（{{ v.action }} @ {{ v.binlog_file }}:{{ v.binlog_position }}）
            </span>
          </td>
        </tr>
        </tbody>
      </table>

      <div v-if="batch.publish_summary" class="publish-summary" data-testid="publish-summary">
        <h4>发布结果</h4>
        <p class="hint">
          实际生效 {{ batch.publish_summary.executed_actions }} 动作 ·
          幂等跳过 {{ batch.publish_summary.skipped_actions }} 动作 ·
          失败依赖组 {{ batch.publish_summary.failed_groups.length }}
        </p>
        <table class="data-table">
          <thead><tr><th>目标列</th><th>依赖组</th><th>状态</th><th>动作明细</th></tr></thead>
          <tbody>
          <tr v-for="r in batch.publish_summary.mapping_results" :key="String(r.target_column)">
            <td>{{ r.target_column }}</td>
            <td>{{ r.dependency_group }}</td>
            <td>{{ r.status }}</td>
            <td>
              <span v-for="(a, i) in actionDetails(r)" :key="i" class="version-chip">
                {{ a.action }}：{{ a.status }}<template v-if="a.reason">（{{ a.reason }}）</template>
              </span>
            </td>
          </tr>
          </tbody>
        </table>
      </div>

      <div v-if="replayResult" class="publish-summary" data-testid="replay-summary">
        <h4>重放结果</h4>
        <p class="hint">
          本次实际生效 {{ replayResult.replay_executed_actions }} 次 ·
          幂等跳过 {{ replayResult.replay_skipped_actions }} 次 ·
          引擎累计生效 {{ replayResult.engine_apply_count }} 次 ·
          列是否变化：{{ replayResult.columns_changed_by_replay ? '是' : '否' }}
        </p>
      </div>
    </div>

    <div class="receipt-block">
      <h4>DDL 回执清单</h4>
      <table class="data-table">
        <thead>
        <tr><th>记录ID</th><th>位点</th><th>DDL 语句</th><th>批次</th><th>应用状态</th><th>生效次数</th></tr>
        </thead>
        <tbody>
        <tr v-for="r in receiptSummary.receipts" :key="r.ddl_record_id"
            :data-testid="`receipt-${r.ddl_record_id}`" :data-status="r.apply_status">
          <td>{{ r.ddl_record_id }}</td>
          <td>{{ r.binlog_file }}:{{ r.binlog_position }}</td>
          <td>{{ r.ddl_statement }}</td>
          <td>{{ r.batch_id ?? '—' }}</td>
          <td>{{ r.apply_status }}</td>
          <td>{{ r.apply_count }}</td>
        </tr>
        </tbody>
      </table>
    </div>

    <div v-if="catalog" class="receipt-block">
      <h4>目标引擎最终列（{{ catalog.table }}）</h4>
      <table class="data-table">
        <thead><tr><th>列名</th><th>目标类型</th></tr></thead>
        <tbody>
        <tr v-for="c in catalog.columns" :key="c.column" :data-testid="`target-col-${c.column}`">
          <td>{{ c.column }}</td>
          <td>{{ c.type }}</td>
        </tr>
        </tbody>
      </table>
      <p class="hint">累计生效动作 {{ catalog.apply_count }} · 已执行语句台账 {{ catalog.executed_statement_count }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  buildBatch,
  fetchDdlReceipts,
  fetchTargetCatalog,
  ingestBatchDdl,
  publishBatch,
  replayBatch,
} from '@/api/backend-service'
import type {
  BatchDataset,
  BatchReplay,
  ReceiptSummary,
  TargetCatalog,
} from '@/api/backend-service'

const ddlForm = reactive({
  database: 'sales',
  table_name: 'orders',
  ddl_statement: '',
  binlog_file: 'mysql-bin.000001',
  binlog_position: 0,
})
const batchForm = reactive({
  source_table: 'orders',
  target_table: 'doris_sales.orders',
  type_strategy: 'doris',
})
const ddlIdsText = ref('')
const initialColumnsText = ref('order_id:BIGINT, amount:DECIMAL(10,2)')

const lastIngested = ref<{ record_id: string; column_changes: unknown[] } | null>(null)
const batch = ref<BatchDataset | null>(null)
const replayResult = ref<BatchReplay | null>(null)
const message = ref('')
const receiptSummary = ref<ReceiptSummary>({ total: 0, applied: 0, queued: 0, failed: 0, receipts: [] })
const catalog = ref<TargetCatalog | null>(null)

function actionDetails(row: Record<string, unknown>): Array<{ action: string; status: string; reason?: string }> {
  const details = row.action_results
  return Array.isArray(details) ? details as Array<{ action: string; status: string; reason?: string }> : []
}

async function refreshViews() {
  const receipts = await fetchDdlReceipts(ddlForm.table_name)
  receiptSummary.value = receipts.receipt_summary
  if (batch.value) {
    const target = await fetchTargetCatalog(batch.value.target_table)
    catalog.value = target.catalog
  }
}

async function ingestDdl() {
  message.value = ''
  try {
    const result = await ingestBatchDdl({ ...ddlForm })
    lastIngested.value = { record_id: result.record_id, column_changes: result.column_changes }
    ddlIdsText.value = ddlIdsText.value
      ? `${ddlIdsText.value},${result.record_id}`
      : result.record_id
    message.value = `DDL ${result.record_id} 已登记于 ${result.binlog_file}:${result.binlog_position}，解析出 ${result.column_changes.length} 个改列动作`
    await refreshViews()
  } catch (error) {
    message.value = error instanceof Error ? error.message : 'DDL 登记失败'
  }
}

function parseInitialColumns(): Array<{ column: string; type: string }> | undefined {
  const text = initialColumnsText.value.trim()
  if (!text) return undefined
  return text.split(',').map((part) => {
    const [column, type] = part.split(':').map((s) => s.trim())
    return { column, type }
  })
}

async function build() {
  message.value = ''
  replayResult.value = null
  try {
    const result = await buildBatch({
      source_table: batchForm.source_table,
      target_table: batchForm.target_table,
      ddl_record_ids: ddlIdsText.value.split(',').map((s) => s.trim()).filter(Boolean),
      type_strategy: batchForm.type_strategy,
      initial_columns: parseInitialColumns(),
    })
    batch.value = result.batch
    message.value = `批次 ${result.batch.batch_id} 已归并：${result.batch.mapping_dataset.length} 条字段映射，${result.batch.dependency_groups.length} 个依赖组`
    await refreshViews()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批次归并失败'
  }
}

async function publish() {
  if (!batch.value) return
  message.value = ''
  try {
    const result = await publishBatch(batch.value.batch_id)
    batch.value = result.batch
    message.value = `批次发布完成：状态 ${result.batch.status}`
    await refreshViews()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批次发布失败'
  }
}

async function replay() {
  if (!batch.value) return
  message.value = ''
  try {
    const result = await replayBatch(batch.value.batch_id)
    replayResult.value = result.replay
    message.value = `重放完成：实际生效 ${result.replay.replay_executed_actions} 次，幂等跳过 ${result.replay.replay_skipped_actions} 次`
    await refreshViews()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批次重放失败'
  }
}

onMounted(refreshViews)
</script>

<style scoped>
.batch-sync {
  margin-top: 24px;
  padding: 20px;
  border: 1px solid var(--border-color, #d9dee7);
  border-radius: 12px;
  background: var(--panel-bg, #fafbfd);
}
.batch-head h3 {
  margin: 0 0 6px;
}
.batch-form {
  margin-top: 18px;
  padding-top: 14px;
  border-top: 1px dashed var(--border-color, #d9dee7);
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px 16px;
  margin-bottom: 10px;
}
.form-grid label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 13px;
}
.form-grid label.wide {
  grid-column: 1 / -1;
}
.form-grid input,
.form-grid select {
  padding: 7px 10px;
  border: 1px solid var(--border-color, #c6ccd6);
  border-radius: 8px;
}
.hint {
  color: var(--muted, #667085);
  font-size: 13px;
}
.version-chip {
  display: inline-block;
  margin: 2px 4px 2px 0;
  padding: 2px 8px;
  border-radius: 999px;
  background: #eef2ff;
  font-size: 12px;
  color: #3538cd;
}
.batch-result,
.receipt-block,
.publish-summary {
  margin-top: 18px;
}
</style>
