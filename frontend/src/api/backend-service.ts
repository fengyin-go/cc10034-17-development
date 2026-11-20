import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 批量结构同步：跨 DDL 日志编排、批次字段映射数据集、目标引擎发布与幂等重放
// ---------------------------------------------------------------------------
const BATCH_BASE = '/api/schema-sync/batches'

export async function ingestBatchDdl(payload: {
  database: string
  table_name: string
  ddl_statement: string
  binlog_file: string
  binlog_position: number
}): Promise<{ ok: boolean; record_id: string; column_changes: Array<Record<string, string>>; binlog_file: string; binlog_position: number }> {
  const response = await request(`${BATCH_BASE}/ddl`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function buildBatch(payload: {
  source_table: string
  target_table: string
  ddl_record_ids: string[]
  type_strategy?: string
  initial_columns?: Array<{ column: string; type: string }>
  simulate_failure?: string | null
}): Promise<{ ok: boolean; batch: BatchDataset }> {
  const response = await request(`${BATCH_BASE}/build`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function publishBatch(batchId: string): Promise<{ ok: boolean; batch: BatchDataset }> {
  const response = await request(`${BATCH_BASE}/${batchId}/publish`, { method: 'POST' })
  return await response.json()
}

export async function replayBatch(batchId: string): Promise<{ ok: boolean; replay: BatchReplay }> {
  const response = await request(`${BATCH_BASE}/${batchId}/replay`, { method: 'POST' })
  return await response.json()
}

export async function fetchTargetCatalog(targetTable: string): Promise<{ ok: boolean; catalog: TargetCatalog }> {
  const response = await request(`${BATCH_BASE}/targets/${encodeURIComponent(targetTable)}`)
  return await response.json()
}

export async function fetchDdlReceipts(table?: string): Promise<{ ok: boolean; receipt_summary: ReceiptSummary }> {
  const query = table ? `?table=${encodeURIComponent(table)}` : ''
  const response = await request(`${BATCH_BASE}/ddl-receipts${query}`)
  return await response.json()
}

export type BatchMappingEntry = {
  mapping_id: string
  dependency_group: string
  source_column: string
  target_column: string
  source_type: string
  target_type: string
  actions: string[]
  source_ddl: string[]
  source_versions: Array<{ ddl_record_id: string; binlog_file: string; binlog_position: number; action: string }>
  publishable: boolean
}

export type BatchDataset = {
  batch_id: string
  batch_fingerprint: string
  source_table: string
  target_table: string
  ddl_record_ids: string[]
  source_positions: Array<{ ddl_record_id: string; binlog_file: string; binlog_position: number }>
  mapping_dataset: BatchMappingEntry[]
  dependency_groups: Array<{ group: string; target_columns: string[]; recovery_boundary: boolean }>
  status: string
  applied: boolean
  publish_summary: {
    executed_actions: number
    skipped_actions: number
    failed_groups: string[]
    mapping_results: Array<Record<string, unknown>>
  } | null
}

export type BatchReplay = {
  batch_id: string
  batch_fingerprint: string
  target_table: string
  replay_executed_actions: number
  replay_skipped_actions: number
  engine_apply_count: number
  executed_statement_count: number
  columns_changed_by_replay: boolean
  receipts: Array<Record<string, unknown>>
}

export type TargetCatalog = {
  table: string
  columns: Array<{ column: string; type: string }>
  apply_count: number
  executed_statement_count: number
}

export type DdlReceipt = {
  ddl_record_id: string
  database: string
  table: string
  ddl_statement: string
  binlog_file: string
  binlog_position: number
  batch_id: string | null
  apply_status: string
  applied: boolean
  apply_count: number
}

export type ReceiptSummary = {
  total: number
  applied: number
  queued: number
  failed: number
  receipts: DdlReceipt[]
}
