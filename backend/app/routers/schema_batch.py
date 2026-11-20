"""批量结构同步接口：跨DDL日志编排、批次字段映射数据集、目标引擎发布与幂等重放。

路由层只做参数转发，状态流转全部落在 schema_sync / ddl_tracker 领域服务内。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/schema-sync/batches", tags=["批量结构同步"])


class BatchBuildPayload(BaseModel):
    source_table: str
    target_table: str
    ddl_record_ids: list[str] = Field(default_factory=list)
    type_strategy: str = "doris"
    initial_columns: list[dict[str, Any]] | None = None
    simulate_failure: str | None = None


class DdlIngestPayload(BaseModel):
    database: str
    table_name: str
    ddl_statement: str
    binlog_file: str = "mysql-bin.000001"
    binlog_position: int = 0


def _schema_service():
    return service_for("schema_sync")


def _ddl_service():
    return service_for("ddl_tracker")


@router.post("/ddl")
def ingest_ddl(payload: DdlIngestPayload) -> dict[str, Any]:
    """登记并分析一条带原始位点的 DDL（DDL追踪交付改列动作和原始位点）。"""
    ddl = _ddl_service().record_ddl(
        payload.database,
        payload.table_name,
        payload.ddl_statement,
        binlog_file=payload.binlog_file,
        binlog_position=payload.binlog_position,
    )
    return {
        "ok": True,
        "record_id": ddl.record_id,
        "column_changes": ddl.column_changes,
        "binlog_file": ddl.binlog_file,
        "binlog_position": ddl.binlog_position,
    }


@router.post("/build")
def build_batch(payload: BatchBuildPayload) -> dict[str, Any]:
    """把一批表结构变更日志按依赖归并，生成批次字段映射数据集（不发布）。"""
    try:
        batch = _schema_service().build_batch_dataset(
            payload.source_table,
            payload.target_table,
            payload.ddl_record_ids,
            type_strategy=payload.type_strategy,
            initial_columns=payload.initial_columns,
            simulate_failure=payload.simulate_failure,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "batch": batch}


@router.post("/{batch_id}/publish")
def publish_batch(batch_id: str) -> dict[str, Any]:
    """向目标引擎发布批次字段映射数据集，结果回写 DDL 回执清单。"""
    try:
        batch = _schema_service().publish_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "batch": batch}


@router.post("/{batch_id}/replay")
def replay_batch(batch_id: str) -> dict[str, Any]:
    """重放原批次，返回实际生效次数（已生效动作由目标引擎幂等跳过）。"""
    try:
        result = _schema_service().replay_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "replay": result}


@router.get("")
def list_batches() -> dict[str, Any]:
    from app.pipeline import pipeline_bus
    return {"ok": True, "items": pipeline_bus.list_batches()}


@router.get("/ddl-receipts")
def ddl_receipts(table: str | None = None) -> dict[str, Any]:
    """查看 DDL 回执清单：区分本批已应用与仍处于队列的语句。"""
    return {"ok": True, "receipt_summary": _schema_service().ddl_queue_view(table)}


@router.get("/targets/{target_table:path}")
def target_catalog(target_table: str) -> dict[str, Any]:
    """查看目标引擎的最终列目录。"""
    return {"ok": True, "catalog": _schema_service().target_catalog(target_table)}
