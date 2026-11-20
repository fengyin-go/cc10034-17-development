"""Schema同步领域服务：管理源表到目标表的列级Schema同步。

上游：ddl_tracker 产出列变更分析结果 → schema_sync 消费并生成同步策略
下游：format_adapter 参考同步后的列映射进行格式适配

批量结构同步：把一批表结构变更日志按依赖逐条归并，生成批次字段映射数据集，
再向目标引擎发布；应用结果回写 DDL 回执清单，支持幂等重放与依赖组恢复边界。
"""
from __future__ import annotations

import copy
import hashlib
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class SchemaSyncService(BaseModuleService):
    MODULE = "schema_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源表", "目标表", "列映射", "类型转换", "同步模式", "冲突策略"]
    REQUIRED_FIELDS = ["任务名称", "源表", "目标表"]
    STATUS_ORDER = ["草稿", "同步中", "已完成", "同步失败"]
    ACTION_RULES = {"开始同步": "草稿", "完成同步": "同步中", "标记失败": "同步中"}
    NEGATIVE_ACTIONS = []

    def build_sync_plan(self, config: dict[str, Any]) -> dict[str, Any]:
        """根据源Schema和DDL变更结果构建同步计划。

        读取 ddl_tracker 产出的 schema_registry 中已注册的列变更，
        结合类型转换策略、冲突策略生成同步任务。
        """
        source_table = config.get("源表", "")
        target_table = config.get("目标表", "")
        type_conversion = config.get("类型转换", "直接映射")
        conflict_strategy = config.get("冲突策略", "覆盖目标")

        source_cols = pipeline_bus.schema_registry.get(source_table, [])
        col_mapping: list[dict[str, Any]] = []
        for col_info in source_cols:
            target_type = self.convert_type(col_info.get("type", "VARCHAR"), type_conversion)
            col_mapping.append({
                "source_column": col_info.get("column"),
                "source_type": col_info.get("type"),
                "target_type": target_type,
                "action": col_info.get("action"),
            })

        plan = {
            "task_name": config.get("任务名称"),
            "source_table": source_table,
            "target_table": target_table,
            "column_mapping": col_mapping,
            "mapping_count": len(col_mapping),
            "conflict_strategy": conflict_strategy,
            "generated_ddl": self.generate_sync_ddl(col_mapping, target_table),
            "estimated_impact": self.estimate_sync_impact(col_mapping),
            "generated_at": datetime.now().isoformat(),
        }
        return plan

    def convert_type(self, source_type: str, strategy: str) -> str:
        """类型转换策略执行。

        支持的转换：mysql->hive/postgresql/doris 等目标格式。
        """
        source_upper = source_type.upper()
        type_map = {
            "VARCHAR": {"直接映射": "STRING", "hive": "STRING", "doris": "VARCHAR(65533)"},
            "INT": {"直接映射": "INT", "hive": "INT", "doris": "INT"},
            "BIGINT": {"直接映射": "BIGINT", "hive": "BIGINT", "doris": "BIGINT"},
            "DECIMAL": {"直接映射": "DECIMAL(38,18)", "hive": "DECIMAL(38,18)", "doris": "DECIMAL(38,18)"},
            "DATETIME": {"直接映射": "TIMESTAMP", "hive": "TIMESTAMP", "doris": "DATETIME"},
            "TEXT": {"直接映射": "STRING", "hive": "STRING", "doris": "STRING"},
            "JSON": {"直接映射": "STRING", "hive": "STRING", "doris": "JSON"},
        }
        lower_strategy = strategy.lower()
        for base, targets in type_map.items():
            if base in source_upper:
                return targets.get(lower_strategy, targets.get("直接映射", source_type))
        return "STRING"

    def estimate_sync_impact(self, col_mapping: list[dict[str, Any]]) -> dict[str, Any]:
        """估算同步对下游的影响范围。"""
        add_count = sum(1 for c in col_mapping if c.get("action") == "add")
        drop_count = sum(1 for c in col_mapping if c.get("action") == "drop")
        modify_count = sum(1 for c in col_mapping if c.get("action") == "modify")
        risk_level = "低"
        if drop_count > 0:
            risk_level = "高"
        elif modify_count > 0:
            risk_level = "中"
        return {
            "columns_added": add_count,
            "columns_dropped": drop_count,
            "columns_modified": modify_count,
            "risk_level": risk_level,
            "breaking_change": drop_count > 0,
        }

    def generate_sync_ddl(self, col_mapping: list[dict[str, Any]], target_table: str) -> str:
        """根据列映射生成目标DDL。"""
        cols: list[str] = []
        for cm in col_mapping:
            if cm.get("action") == "drop":
                continue
            cols.append(f"  {cm['source_column']} {cm['target_type']}")
        if not cols:
            return f"-- 无新增列，目标表 {target_table} 不变"
        return f"ALTER TABLE {target_table} ADD COLUMNS (\n" + ",\n".join(cols) + "\n);"

    def detect_type_conflict(self, col_name: str, source_type: str, target_type: str) -> dict[str, Any]:
        """检测列类型冲突。"""
        has_conflict = source_type.upper() != target_type.upper()
        resolution = ""
        if has_conflict:
            if "VARCHAR" in source_type.upper() and "STRING" in target_type.upper():
                resolution = "字符串宽度调整，无数据丢失"
            elif "DECIMAL" in source_type.upper() and "BIGINT" in target_type.upper():
                resolution = "精度可能丢失，建议保留DECIMAL"
            else:
                resolution = "建议人工确认类型转换"
        return {
            "column": col_name,
            "source_type": source_type,
            "target_type": target_type,
            "conflict": has_conflict,
            "resolution": resolution,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "开始同步":
            source_table = entry.get("源表", "")
            existing = pipeline_bus.schema_registry.get(source_table)
            if existing:
                pipeline_bus.write_audit(
                    "Schema同步", source_table,
                    f"检测到 {len(existing)} 条Schema记录，生成列映射同步计划",
                    "schema_sync", "同步中"
                )
        return entry, msg

    # ==================================================================
    # 批量结构同步：跨日志编排、批次字段映射数据集、目标引擎发布
    # ==================================================================
    def build_batch_dataset(
        self,
        source_table: str,
        target_table: str,
        ddl_record_ids: list[str],
        *,
        type_strategy: str = "doris",
        initial_columns: list[dict[str, Any]] | None = None,
        simulate_failure: str | None = None,
    ) -> dict[str, Any]:
        """把一批表结构变更日志按依赖逐条归并，生成批次字段映射数据集。

        归并规则：
        - 按 DDL 原始位点（binlog 文件+偏移）排序后逐条处理；
        - 维护列名映射（旧名→当前名），列名转换后后续类型调整按新列名解析；
        - 同一目标列被多段变更交叠写入时，归并到同一映射条目并保留来源链；
        - 未涉及该列的变更落入独立依赖组，仍具备独立发布条件。
        """
        located = [ddl for rid in ddl_record_ids if (ddl := pipeline_bus.find_ddl(rid)) is not None]
        missing = [rid for rid in ddl_record_ids if pipeline_bus.find_ddl(rid) is None]
        if missing:
            raise ValueError(f"DDL记录不存在，无法编排批次：{', '.join(missing)}")
        if not located:
            raise ValueError("批次至少需要一条DDL变更日志")

        located.sort(key=lambda d: (d.binlog_file, d.binlog_position, d.executed_at.isoformat()))

        name_map: dict[str, str] = {}  # 源列旧名 -> 当前目标列名
        entries_by_target: dict[str, dict[str, Any]] = {}
        ordered_targets: list[str] = []
        next_group = 0

        def current_name(col: str) -> str:
            return name_map.get(col, col)

        for ddl in located:
            for cc in ddl.column_changes:
                action = cc["action"]
                source_col = cc["column"]
                if action == "rename":
                    target_col = cc["new_column"]
                    name_map[source_col] = target_col
                    # 已有的同名条目跟随改名，保证后续类型调整按新列名解析
                    prev = entries_by_target.pop(source_col, None)
                    if prev is not None:
                        prev["target_column"] = target_col
                        entries_by_target[target_col] = prev
                        ordered_targets = [target_col if c == source_col else c for c in ordered_targets]
                elif action == "drop":
                    target_col = current_name(source_col)
                else:  # add / modify
                    target_col = current_name(source_col)

                entry = entries_by_target.get(target_col)
                if entry is None:
                    next_group += 1
                    entry = {
                        "mapping_id": f"map_{next_group:03d}",
                        "dependency_group": f"grp_{next_group:03d}",
                        "source_column": source_col,
                        "target_column": target_col,
                        "source_type": cc.get("type", ""),
                        "target_type": self.convert_type(cc.get("type", "VARCHAR"), type_strategy)
                        if cc.get("type") else "",
                        "actions": [],
                        "source_ddl": [],
                        "source_versions": [],
                        "publishable": action != "drop",
                    }
                    entries_by_target[target_col] = entry
                    ordered_targets.append(target_col)

                entry["actions"].append(action)
                entry["source_ddl"].append(ddl.record_id)
                entry["source_versions"].append({
                    "ddl_record_id": ddl.record_id,
                    "binlog_file": ddl.binlog_file,
                    "binlog_position": ddl.binlog_position,
                    "action": action,
                })
                if action == "rename":
                    entry["source_column"] = source_col
                if action in ("add", "modify") and cc.get("type"):
                    entry["source_type"] = cc["type"]
                    entry["target_type"] = self.convert_type(cc["type"], type_strategy)
                if action == "drop":
                    entry["publishable"] = False

        mapping_dataset = [entries_by_target[name] for name in ordered_targets]

        # 依赖分组：交叠到同一目标列的变更同组，独立变更各自成组
        groups: dict[str, list[str]] = {}
        for entry in mapping_dataset:
            groups.setdefault(entry["dependency_group"], []).append(entry["target_column"])

        source_positions = [{
            "ddl_record_id": d.record_id,
            "binlog_file": d.binlog_file,
            "binlog_position": d.binlog_position,
        } for d in located]

        fingerprint_raw = "|".join(
            f"{s['ddl_record_id']}@{s['binlog_file']}:{s['binlog_position']}" for s in source_positions
        )
        batch_fingerprint = hashlib.sha256(
            f"{source_table}->{target_table}#{fingerprint_raw}".encode()
        ).hexdigest()[:16]

        batch = {
            "batch_id": f"batch_{batch_fingerprint}",
            "batch_fingerprint": batch_fingerprint,
            "source_table": source_table,
            "target_table": target_table,
            "type_strategy": type_strategy,
            "ddl_record_ids": [d.record_id for d in located],
            "source_positions": source_positions,
            "mapping_dataset": mapping_dataset,
            "dependency_groups": [
                {"group": grp, "target_columns": cols, "recovery_boundary": True}
                for grp, cols in groups.items()
            ],
            "simulate_failure": simulate_failure,
            "initial_columns": initial_columns or [],
            "status": "已编排",
            "applied": False,
            "publish_summary": None,
            "created_at": datetime.now().isoformat(),
        }
        pipeline_bus.save_batch(batch)
        pipeline_bus.write_audit(
            "批量结构同步", f"{source_table}->{target_table}",
            f"批次 {batch['batch_id']} 归并 {len(located)} 条DDL，"
            f"生成 {len(mapping_dataset)} 条字段映射，{len(groups)} 个依赖组",
            "schema_sync", "已编排"
        )
        return batch

    def _render_single_ddl(self, target_table: str, entry: dict[str, Any], action: str) -> str:
        """渲染归并链中的单个改列动作 DDL。"""
        col = entry["target_column"]
        if action == "add":
            return f"ALTER TABLE {target_table} ADD COLUMN {col} {entry['target_type']}"
        if action == "rename":
            return f"ALTER TABLE {target_table} RENAME COLUMN {entry['source_column']} TO {col}"
        if action == "modify":
            return f"ALTER TABLE {target_table} MODIFY COLUMN {col} {entry['target_type']}"
        if action == "drop":
            return f"ALTER TABLE {target_table} DROP COLUMN {entry['source_column']}"
        return f"ALTER TABLE {target_table} {action} COLUMN {col}"

    def _render_target_ddl(self, target_table: str, entry: dict[str, Any]) -> str:
        """按归并后的动作链生成目标引擎改列 DDL。"""
        return "; ".join(
            self._render_single_ddl(target_table, entry, action) for action in entry["actions"]
        )

    def publish_batch(self, batch_id: str) -> dict[str, Any]:
        """向目标引擎发布批次字段映射数据集，并把执行结果回写DDL回执清单。

        - 按依赖组逐条发布；落库失败时保留整个依赖组的恢复边界（同组回滚）；
        - 未涉及失败列的独立依赖组仍可独立发布；
        - 已生效的改列动作重放时由目标引擎幂等跳过，不重复执行。
        """
        batch = pipeline_bus.get_batch(batch_id)
        if batch is None:
            raise ValueError(f"批次不存在：{batch_id}")

        target_table = batch["target_table"]
        # 首次发布时按批次声明初始化目标引擎列目录（幂等：已存在则不回填）
        pipeline_bus.ensure_target_table(target_table, batch.get("initial_columns"))

        entries = batch["mapping_dataset"]
        group_of = {e["target_column"]: e["dependency_group"] for e in entries}
        results: list[dict[str, Any]] = []
        failed_groups: set[str] = set()
        executed_actions = 0
        skipped_actions = 0

        for entry in entries:
            group = entry["dependency_group"]
            if group in failed_groups:
                results.append({
                    "mapping_id": entry["mapping_id"], "target_column": entry["target_column"],
                    "dependency_group": group, "executed": False, "skipped": False,
                    "status": "组内阻断", "reason": "依赖组恢复边界保留",
                })
                continue

            # 依赖组级快照：组内任一改列动作落库失败，整组回滚到发布前边界
            group_snapshot = pipeline_bus.snapshot_target(target_table)
            action_results: list[dict[str, Any]] = []
            group_failed = False
            for action in entry["actions"]:
                change = {
                    "action": action,
                    "column": entry["source_column"] if action == "rename" else entry["target_column"],
                    "target_column": entry["target_column"],
                    "new_column": entry["target_column"],
                    "target_type": entry["target_type"],
                }
                statement = self._render_single_ddl(target_table, entry, action)
                if batch.get("simulate_failure") and entry["target_column"] == batch["simulate_failure"]:
                    engine_result = {"executed": False, "skipped": False,
                                     "error": f"模拟落库失败：目标列 {entry['target_column']} 的 {action} 动作"}
                else:
                    engine_result = pipeline_bus.execute_target_ddl(target_table, statement, change)

                if engine_result.get("error"):
                    # 落库失败：保留整个依赖组的恢复边界，组内已执行动作一并回滚
                    pipeline_bus.restore_target(target_table, group_snapshot)
                    failed_groups.add(group)
                    group_failed = True
                    action_results.append({
                        "action": action, "status": "失败", "reason": engine_result["error"],
                        "target_ddl": statement,
                    })
                    break
                if engine_result.get("executed"):
                    executed_actions += 1
                    action_results.append({
                        "action": action, "status": "已生效", "target_ddl": statement,
                    })
                else:
                    skipped_actions += 1
                    action_results.append({
                        "action": action, "status": "幂等跳过",
                        "reason": engine_result.get("reason", ""), "target_ddl": statement,
                    })

            if group_failed:
                for prior in results:
                    if prior.get("dependency_group") == group:
                        prior["status"] = "组内回滚"
                        prior["executed"] = False
                results.append({
                    "mapping_id": entry["mapping_id"], "target_column": entry["target_column"],
                    "dependency_group": group, "executed": False, "skipped": False,
                    "status": "失败", "reason": action_results[-1].get("reason", ""),
                    "action_results": action_results,
                })
                continue

            any_executed = any(a["status"] == "已生效" for a in action_results)
            results.append({
                "mapping_id": entry["mapping_id"], "target_column": entry["target_column"],
                "dependency_group": group,
                "executed": any_executed,
                "skipped": all(a["status"] == "幂等跳过" for a in action_results),
                "status": "已生效" if any_executed else "幂等跳过",
                "action_results": action_results,
                "target_ddl": self._render_target_ddl(target_table, entry),
            })

        # 组级快照回滚只恢复失败依赖组自身的列变更，独立依赖组的成功结果保持不变

        all_ok = not failed_groups
        batch["status"] = "已完成" if all_ok else "同步失败"
        batch["applied"] = all_ok
        batch["publish_summary"] = {
            "executed_actions": executed_actions,
            "skipped_actions": skipped_actions,
            "failed_groups": sorted(failed_groups),
            "mapping_results": results,
            "published_at": datetime.now().isoformat(),
        }

        self._write_receipts(batch, results, group_of, failed_groups)

        pipeline_bus.write_audit(
            "批量结构同步发布", target_table,
            f"批次 {batch_id} 发布：生效 {executed_actions} 动作，跳过 {skipped_actions} 动作，"
            f"失败依赖组 {len(failed_groups)}",
            "schema_sync", batch["status"]
        )
        return batch

    def _write_receipts(
        self,
        batch: dict[str, Any],
        results: list[dict[str, Any]],
        group_of: dict[str, str],
        failed_groups: set[str],
    ) -> None:
        """把每条DDL的执行结果回写到DDL回执清单，区分已应用与队列中。"""
        for ddl_id in batch["ddl_record_ids"]:
            ddl = pipeline_bus.find_ddl(ddl_id)
            if ddl is None:
                continue
            touched_groups = {
                group_of[e["target_column"]]
                for e in batch["mapping_dataset"] if ddl_id in e["source_ddl"]
            }
            group_failed = bool(touched_groups & failed_groups)
            if group_failed:
                # 失败依赖组保留恢复边界，对应语句回到队列等待重新发布
                apply_status, applied = "队列中", False
            else:
                # 同一 DDL 交叠的映射都已落库（含幂等跳过）即视为已应用
                apply_status, applied = "已应用", True

            existing = pipeline_bus.get_receipt(ddl_id) or {}
            prev_count = existing.get("apply_count", 0)
            apply_count = prev_count + 1 if applied else prev_count
            receipt = {
                "ddl_record_id": ddl_id,
                "database": ddl.database,
                "table": ddl.table_name,
                "ddl_statement": ddl.ddl_statement,
                "binlog_file": ddl.binlog_file,
                "binlog_position": ddl.binlog_position,
                "batch_id": batch["batch_id"],
                "dependency_groups": sorted(touched_groups),
                "apply_status": apply_status,
                "applied": applied,
                "apply_count": apply_count,
                "last_published_at": datetime.now().isoformat(),
            }
            pipeline_bus.upsert_receipt(receipt)

    def ddl_queue_view(self, table: str | None = None) -> dict[str, Any]:
        """DDL 回执全景：已登记但未进任何批次的语句仍处于队列中。

        DDL 追踪据此区分本批已应用和仍处于队列的语句。
        """
        all_ddls = [
            d for d in pipeline_bus.ddl_log if (table is None or d.table_name == table)
        ]
        view_rows: list[dict[str, Any]] = []
        for ddl in sorted(all_ddls, key=lambda d: (d.binlog_file, d.binlog_position)):
            receipt = pipeline_bus.get_receipt(ddl.record_id)
            if receipt is None:
                view_rows.append({
                    "ddl_record_id": ddl.record_id,
                    "database": ddl.database,
                    "table": ddl.table_name,
                    "ddl_statement": ddl.ddl_statement,
                    "binlog_file": ddl.binlog_file,
                    "binlog_position": ddl.binlog_position,
                    "batch_id": None,
                    "apply_status": "队列中",
                    "applied": False,
                    "apply_count": 0,
                })
            else:
                view_rows.append(receipt)
        return {
            "total": len(view_rows),
            "applied": sum(1 for r in view_rows if r["apply_status"] == "已应用"),
            "queued": sum(1 for r in view_rows if r["apply_status"] == "队列中"),
            "failed": sum(1 for r in view_rows if r["apply_status"] == "失败"),
            "receipts": view_rows,
        }

    def replay_batch(self, batch_id: str) -> dict[str, Any]:
        """重放一个已生效的批次，返回实际生效次数（目标引擎保证不重复执行）。"""
        before = copy.deepcopy(pipeline_bus.target_engine)
        batch = self.publish_batch(batch_id)
        after = pipeline_bus.target_engine
        summary = batch["publish_summary"]
        catalog = after[batch["target_table"]]
        changed = {
            t: (before.get(t, {}).get("columns"), c.get("columns"))
            for t, c in after.items()
        }
        return {
            "batch_id": batch_id,
            "batch_fingerprint": batch["batch_fingerprint"],
            "target_table": batch["target_table"],
            "replay_executed_actions": summary["executed_actions"],
            "replay_skipped_actions": summary["skipped_actions"],
            "engine_apply_count": catalog["apply_count"],
            "executed_statement_count": len(catalog["executed_statements"]),
            "columns_changed_by_replay": any(a != b for a, b in changed.values()),
            "receipts": [
                pipeline_bus.get_receipt(rid) for rid in batch["ddl_record_ids"]
            ],
        }

    def target_catalog(self, target_table: str) -> dict[str, Any]:
        """查看目标引擎最终列目录。"""
        return pipeline_bus.target_catalog(target_table)
