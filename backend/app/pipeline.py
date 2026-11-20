"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import copy
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)
    # 原始位点：DDL 在源端 binlog 中的位置，跨日志编排按位点排序
    binlog_file: str = ""
    binlog_position: int = 0


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }
        # 批量结构同步 — schema_sync 按批编排 DDL 变更
        self.batch_datasets: dict[str, dict[str, Any]] = {}
        # DDL 回执清单 — 按 ddl_record_id 记录每条语句的应用/队列状态
        self.ddl_receipts: dict[str, dict[str, Any]] = {}
        # 目标引擎：模拟目标库的列目录，发布在此真实生效并保证幂等
        self.target_engine: dict[str, dict[str, Any]] = {}

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def find_ddl(self, record_id: str) -> DdlRecord | None:
        """按记录 ID 查找 DDL。"""
        for ddl in self.ddl_log:
            if ddl.record_id == record_id:
                return ddl
        return None

    # ------------------------------------------------------------------
    # 目标引擎：模拟目标库列目录，承担真实发布与幂等去重
    # ------------------------------------------------------------------
    def ensure_target_table(self, table: str, initial_columns: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        # 仅在目标表首次创建时播种初始列；已存在的表（含重放场景）不回填，
        # 否则已被更名/删除的初始列会被错误地重新加回。
        if table in self.target_engine:
            return self.target_engine[table]
        catalog = self.target_engine.setdefault(
            table,
            {"columns": {}, "column_order": [], "executed_statements": [], "apply_count": 0},
        )
        for col in initial_columns or []:
            name = col["column"]
            if name not in catalog["columns"]:
                catalog["columns"][name] = {"type": col.get("type", "STRING"), "origin": col.get("origin", name)}
                catalog["column_order"].append(name)
        return catalog

    def execute_target_ddl(self, table: str, statement: str, change: dict[str, Any]) -> dict[str, Any]:
        """在目标引擎上执行一条改列动作，返回执行结果（已生效/幂等跳过/失败）。"""
        catalog = self.ensure_target_table(table)
        executed = catalog["executed_statements"]
        normalized = statement.strip()
        if normalized in executed:
            # 重复提交已生效的改列动作，目标引擎不会再次执行
            return {"executed": False, "skipped": True, "reason": "duplicate_statement"}

        action = change.get("action")
        target_col = change.get("target_column") or change.get("column")
        source_col = change.get("column")
        result: dict[str, Any] = {"executed": False, "skipped": False}
        if action == "rename":
            new_name = change.get("new_column", target_col)
            if source_col not in catalog["columns"]:
                if new_name in catalog["columns"]:
                    # 更名已生效过（含重放）：目标列已存在即视为幂等命中
                    result.update(skipped=True, reason="rename_already_applied")
                else:
                    result.update(executed=False, error=f"源列 {source_col} 不存在，依赖组恢复边界保留")
            elif new_name in catalog["columns"]:
                result.update(skipped=True, reason="target_column_exists")
            else:
                catalog["columns"][new_name] = catalog["columns"].pop(source_col)
                catalog["column_order"] = [new_name if c == source_col else c for c in catalog["column_order"]]
                result["executed"] = True
        elif action == "add":
            if target_col in catalog["columns"]:
                result.update(skipped=True, reason="column_exists")
            else:
                catalog["columns"][target_col] = {"type": change.get("target_type", "STRING"), "origin": source_col}
                catalog["column_order"].append(target_col)
                result["executed"] = True
        elif action == "modify":
            if target_col not in catalog["columns"]:
                result.update(executed=False, error=f"目标列 {target_col} 不存在，依赖组恢复边界保留")
            else:
                current_type = catalog["columns"][target_col]["type"]
                wanted_type = change.get("target_type", current_type)
                if current_type == wanted_type:
                    # 目标类型已一致：重复提交的改列动作不再执行
                    result.update(skipped=True, reason="type_already_applied")
                else:
                    catalog["columns"][target_col]["type"] = wanted_type
                    result["executed"] = True
        elif action == "drop":
            if source_col not in catalog["columns"]:
                result.update(skipped=True, reason="column_absent")
            else:
                del catalog["columns"][source_col]
                catalog["column_order"] = [c for c in catalog["column_order"] if c != source_col]
                result["executed"] = True
        else:
            result.update(executed=False, error=f"未知改列动作：{action}")

        if result.get("executed"):
            executed.append(normalized)
            catalog["apply_count"] += 1
        return result

    def target_catalog(self, table: str) -> dict[str, Any]:
        catalog = self.ensure_target_table(table)
        return {
            "table": table,
            "columns": [
                {"column": name, "type": catalog["columns"][name]["type"]}
                for name in catalog["column_order"]
            ],
            "apply_count": catalog["apply_count"],
            "executed_statement_count": len(catalog["executed_statements"]),
        }

    def snapshot_target(self, table: str) -> dict[str, Any]:
        """导出目标表当前目录的深拷贝，作为依赖组恢复边界。"""
        return copy.deepcopy(self.ensure_target_table(table))

    def restore_target(self, table: str, snapshot: dict[str, Any]) -> None:
        """把目标表恢复到快照边界（含列目录、已执行语句台账和生效计数）。"""
        self.target_engine[table] = copy.deepcopy(snapshot)

    # ------------------------------------------------------------------
    # 批量结构同步：批次字段映射数据集与 DDL 回执
    # ------------------------------------------------------------------
    def save_batch(self, batch: dict[str, Any]) -> None:
        self.batch_datasets[batch["batch_id"]] = batch

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self.batch_datasets.get(batch_id)

    def list_batches(self) -> list[dict[str, Any]]:
        return list(self.batch_datasets.values())

    def upsert_receipt(self, receipt: dict[str, Any]) -> None:
        self.ddl_receipts[receipt["ddl_record_id"]] = receipt

    def get_receipt(self, ddl_record_id: str) -> dict[str, Any] | None:
        return self.ddl_receipts.get(ddl_record_id)

    def list_receipts(self, table: str | None = None) -> list[dict[str, Any]]:
        receipts = list(self.ddl_receipts.values())
        if table:
            receipts = [r for r in receipts if r.get("table") == table]
        return sorted(receipts, key=lambda r: (r.get("binlog_file", ""), r.get("binlog_position", 0)))


    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
            "batch_count": len(self.batch_datasets),
            "ddl_receipt_count": len(self.ddl_receipts),
            "target_table_count": len(self.target_engine),
        }


pipeline_bus = PipelineBus()