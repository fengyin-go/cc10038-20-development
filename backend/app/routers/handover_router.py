"""交接班补做路由：领取凭证、归属核验补做、跨班移交和交接清单核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.shift_handover import shift_handover

router = APIRouter(prefix="/api/handover", tags=["交接班补做"])


class ClaimPayload(BaseModel):
    shift: str = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)
    record_ids: list[int] | None = None


class RedoPayload(BaseModel):
    credential: str = Field(..., min_length=1)
    record_ids: list[int] | None = None


class TransferPayload(BaseModel):
    credential: str = Field(..., min_length=1)
    to_shift: str = Field(..., min_length=1)
    record_ids: list[int] | None = None


@router.post("/claim")
def handover_claim(payload: ClaimPayload) -> dict[str, Any]:
    """班组按来源领取待重试记录，签发带行范围的领取凭证。"""
    return shift_handover.claim_retries(payload.shift, payload.source_id, payload.record_ids)


@router.post("/redo")
def handover_redo(payload: RedoPayload) -> dict[str, Any]:
    """补做入口：归属核验后读取原文交给解析，处置结果回写交接清单。"""
    return shift_handover.redo_with_credential(payload.credential, payload.record_ids)


@router.post("/transfer")
def handover_transfer(payload: TransferPayload) -> dict[str, Any]:
    """跨班移交：验证权限和行范围，超出凭证的记录留在本来所在的批次。"""
    return shift_handover.transfer_credential(
        payload.credential, payload.to_shift, payload.record_ids
    )


@router.get("/manifest")
def handover_manifest(
    shift: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
    result: str | None = Query(default=None),
) -> dict[str, Any]:
    """交接清单：核对哪一班、哪张领取凭证生成了哪些标准化明细。"""
    return shift_handover.manifest(shift=shift, source_id=source_id, result=result)


@router.get("/credentials")
def handover_credentials(
    shift: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """领取凭证列表：交班后仍可凭摘要核验归属。"""
    return shift_handover.list_credentials(shift=shift, status=status)
