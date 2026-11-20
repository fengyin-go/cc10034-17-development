"""DDL追踪领域服务：追踪和解析DDL变更对Schema的影响。

上游：从管道总线消费 DdlRecord 进行自动分析
下游：schema_sync(列级变更)、partition_sync(分区变更) 消费分析结果
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import DdlRecord, pipeline_bus


class DdlTrackerService(BaseModuleService):
    MODULE = "ddl_tracker"
    ENTITY = "DDL记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"]
    REQUIRED_FIELDS = ["记录ID", "变更数据库", "变更表名"]
    STATUS_ORDER = ["已记录", "已分析", "已通知", "已处理"]
    ACTION_RULES = {"分析变更": "已记录", "发送通知": "已分析", "完成处理": "已分析"}
    NEGATIVE_ACTIONS = []

    def analyze_ddl_impact(self, ddl: DdlRecord) -> dict[str, Any]:
        """分析DDL变更对目标表的影响范围。

        解析 DDL 语句中的列变化、分区变化、索引变化，
        产出影响报告供 schema_sync 和 partition_sync 消费。
        每条列变更都带改列动作（add/drop/rename/modify）和原始位点。
        """
        column_changes: list[dict[str, Any]] = self._parse_column_changes(ddl.ddl_statement)
        ddl.column_changes = [dict(cc) for cc in column_changes]

        partition_changes: list[dict[str, Any]] = []
        ddl_text = ddl.ddl_statement.upper()

        # 检测 ADD PARTITION
        add_part = re.findall(r'ADD\s+PARTITION', ddl_text, re.IGNORECASE)
        if add_part:
            partition_changes.append({"action": "add_partition"})
            ddl.partition_changes.append({"action": "add_partition"})

        # 检测 DROP PARTITION
        drop_part = re.findall(r'DROP\s+PARTITION\s+(\w+)', ddl_text, re.IGNORECASE)
        for part_name in drop_part:
            partition_changes.append({"action": "drop_partition", "name": part_name})
            ddl.partition_changes.append({"action": "drop_partition", "name": part_name})

        affected_cols = len(column_changes)
        rollback = self.generate_rollback(ddl.ddl_statement)

        impact = {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "column_changes": column_changes,
            "partition_changes": partition_changes,
            "affected_columns": affected_cols,
            "rollback_statement": rollback,
            "has_schema_impact": len(column_changes) > 0,
            "has_partition_impact": len(partition_changes) > 0,
            "source_position": {
                "binlog_file": ddl.binlog_file,
                "binlog_position": ddl.binlog_position,
            },
            "analyzed_at": datetime.now().isoformat(),
        }

        # 对 schema_sync 有影响的DDL，写入管道供批量结构同步按位点归并消费
        if impact["has_schema_impact"]:
            for cc in column_changes:
                pipeline_bus.schema_registry.setdefault(ddl.table_name, []).append({
                    "record_id": ddl.record_id,
                    "column": cc.get("column"),
                    "new_column": cc.get("new_column", ""),
                    "action": cc.get("action"),
                    "type": cc.get("type", ""),
                    "binlog_file": ddl.binlog_file,
                    "binlog_position": ddl.binlog_position,
                    "timestamp": datetime.now().isoformat(),
                })

        pipeline_bus.add_ddl(ddl)
        pipeline_bus.write_audit(
            "DDL分析", f"{ddl.database}.{ddl.table_name}",
            f"分析DDL影响：列变更{affected_cols}，分区变更{len(partition_changes)}，"
            f"位点 {ddl.binlog_file}:{ddl.binlog_position}",
            "ddl_tracker", "已分析"
        )
        return impact

    def _parse_column_changes(self, ddl_stmt: str) -> list[dict[str, Any]]:
        """解析一条 DDL 内的全部列变更，统一输出 add/drop/rename/modify 动作。

        - RENAME COLUMN a TO b / CHANGE a b（同类型或未给类型）→ rename
        - CHANGE a b 带新类型 → rename + modify（更名与类型调整拆开，保留交叠来源）
        - MODIFY COLUMN a T → modify
        - ADD COLUMN a T → add
        - DROP COLUMN a → drop（排除 DROP PARTITION）
        """
        changes: list[dict[str, Any]] = []
        text = ddl_stmt
        # 类型词允许括号参数与内部逗号，如 DECIMAL(12,2)、VARCHAR(32)
        type_token = r'[\w]+(?:\s*\([^)]*\))?'

        for match in re.finditer(
            rf'ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+({type_token})',
            text, re.IGNORECASE,
        ):
            changes.append({
                "action": "add",
                "column": match.group(1),
                "type": match.group(2).strip(),
            })

        for match in re.finditer(r'RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)', text, re.IGNORECASE):
            changes.append({
                "action": "rename",
                "column": match.group(1),
                "new_column": match.group(2),
            })

        for match in re.finditer(
            rf'CHANGE\s+(?:COLUMN\s+)?(\w+)\s+(\w+)(?:\s+({type_token}))?',
            text, re.IGNORECASE,
        ):
            old_name, new_name, col_type = match.group(1), match.group(2), match.group(3)
            col_type = (col_type or "").strip()
            if old_name != new_name:
                changes.append({"action": "rename", "column": old_name, "new_column": new_name})
                if col_type:
                    changes.append({
                        "action": "modify",
                        "column": new_name,
                        "new_column": new_name,
                        "type": col_type,
                    })
            elif col_type:
                changes.append({
                    "action": "modify",
                    "column": old_name,
                    "new_column": old_name,
                    "type": col_type,
                })

        for match in re.finditer(
            rf'MODIFY\s+(?:COLUMN\s+)?(\w+)\s+({type_token})',
            text, re.IGNORECASE,
        ):
            changes.append({
                "action": "modify",
                "column": match.group(1),
                "new_column": match.group(1),
                "type": match.group(2).strip(),
            })

        for match in re.finditer(r'DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(\w+)', text, re.IGNORECASE):
            changes.append({"action": "drop", "column": match.group(1)})

        return changes

    def track_temporal_ddl(self, table_name: str, from_dt: str, to_dt: str) -> list[dict[str, Any]]:
        """查询指定表在时间范围内的DDL变更序列。"""
        results: list[dict[str, Any]] = []
        recent = pipeline_bus.get_recent_ddls()
        for ddl in recent:
            if ddl.table_name == table_name and from_dt <= ddl.executed_at.isoformat() <= to_dt:
                results.append({
                    "record_id": ddl.record_id,
                    "ddl": ddl.ddl_statement[:200],
                    "executed_at": ddl.executed_at.isoformat(),
                    "affected_columns": ddl.affected_columns,
                })
        return results

    def generate_rollback(self, ddl_stmt: str) -> str:
        """根据DDL语句生成回滚语句。"""
        ddl_upper = ddl_stmt.upper()
        rename_match = re.search(r'RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)', ddl_stmt, re.IGNORECASE)
        if rename_match:
            return f"ALTER TABLE ... RENAME COLUMN {rename_match.group(2)} TO {rename_match.group(1)};"
        if "ADD COLUMN" in ddl_upper:
            match = re.search(r'ADD\s+COLUMN\s+(\w+)', ddl_stmt, re.IGNORECASE)
            if match:
                return f"ALTER TABLE ... DROP COLUMN {match.group(1)};"
        if "DROP COLUMN" in ddl_upper:
            return "-- 回滚需从备份恢复列定义"
        if "MODIFY" in ddl_upper or "CHANGE" in ddl_upper:
            return "-- 回滚需记录修改前的列类型/列名"
        if "ADD PARTITION" in ddl_upper:
            return "ALTER TABLE ... DROP PARTITION ...;"
        return "-- 自动回滚生成中"

    def record_ddl(
        self,
        database: str,
        table_name: str,
        ddl_stmt: str,
        binlog_file: str = "mysql-bin.000001",
        binlog_position: int = 0,
    ) -> DdlRecord:
        """记录并分析一条DDL，保留原始位点供跨日志归并。"""
        ddl = DdlRecord(
            record_id=f"ddl_{uuid.uuid4().hex[:8]}",
            database=database,
            table_name=table_name,
            ddl_statement=ddl_stmt,
            executed_at=datetime.now(),
            affected_columns=0,
            rollback_statement=self.generate_rollback(ddl_stmt),
            binlog_file=binlog_file,
            binlog_position=binlog_position,
        )
        self.analyze_ddl_impact(ddl)
        return ddl

    def list_ddl_receipts(self, table: str | None = None) -> dict[str, Any]:
        """DDL 回执清单：Schema同步回写后，据此区分本批已应用与队列中的语句。"""
        from app.services import SERVICES
        schema_service = SERVICES.get("schema_sync")
        if schema_service is None:
            return {"total": 0, "applied": 0, "queued": 0, "failed": 0, "receipts": []}
        return schema_service.ddl_queue_view(table)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "发送通知":
            pipeline_bus.write_audit(
                "DDL通知", f"{entry.get('变更数据库', '')}.{entry.get('变更表名', '')}",
                f"DDL记录 {entry_id} 变更通知已发送",
                "ddl_tracker", "已通知"
            )
        return entry, msg