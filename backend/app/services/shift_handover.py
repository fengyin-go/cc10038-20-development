"""重试队列交接班补做编排：按来源核对归属、生成归属核验后的补做范围、
读取原始正文交给解析，并把处置结果回写交接清单。

背景：重试队列本身不随来源交接限制补做权限，领取过的待重试日志可能还
在上一班手里。这里引入「领取凭证」（handover_credential）与「交接清单」
（handover_manifest）两张表：

- 领取：班组按来源领取待重试记录，拿到带行范围和可核验摘要的凭证；
  交接并发时允许新旧班组各持一份凭证（冲突在补做确认时裁决）。
- 补做：按凭证核对来源归属与行范围，生成归属核验后的补做范围；只有
  进入补做范围的原始日志才会被读取正文并交给解析。同一原始记录只确认
  一份有效补做结果，冲突双方的归属凭证都保留在交接清单里可供核验。
- 移交：跨班移交要验证权限和行范围，超出凭证的记录留在本来所在的批次，
  其他来源的待重试行继续照原处置。
- 核对：交班以后通过交接清单关联实际整理过的原始日志与产出的标准化
  明细，核对哪一班、哪张领取凭证生成了哪些明细。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services.pipeline import pipeline
from app.store import store

CREDENTIAL_TABLE = "handover_credential"
MANIFEST_TABLE = "handover_manifest"

# 处置结果
RESULT_CONFIRMED = "确认"
RESULT_CONFLICT = "冲突未确认"
RESULT_REJECTED = "越权驳回"

# 凭证状态
CRED_ACTIVE = "有效"
CRED_SETTLED = "已结算"
CRED_TRANSFERRED = "已移交"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _credential_token(code: str, shift: str, source_id: str, record_ids: list[int]) -> str:
    """生成可核验的归属凭证摘要：班组、来源、行范围任一不符都验不出来。"""
    payload = json.dumps(
        {"code": code, "shift": shift, "source": source_id, "rows": sorted(record_ids)},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _normalize_ids(record_ids: list[Any] | None) -> list[int] | None:
    if record_ids is None:
        return None
    normalized: list[int] = []
    for rid in record_ids:
        try:
            normalized.append(int(rid))
        except (TypeError, ValueError):
            continue
    return sorted(set(normalized))


class ShiftHandoverService:
    """重试补做的交接班编排：领取、补做、移交、核对。"""

    # ---------- 归属解析 ----------
    def _source_of_raw(self, raw: dict[str, Any] | None) -> str | None:
        """解析原始日志记录归属的数据源。"""
        if raw is None:
            return None
        source_id = raw.get("source_id")
        if source_id:
            return str(source_id)
        batch_no = raw.get("批次编号")
        if batch_no:
            for batch in store.rows("ingest_batch"):
                if batch.get("批次编号") == batch_no:
                    return str(batch.get("所属数据源") or "") or None
        return None

    def _retry_source(self, retry_entry: dict[str, Any]) -> str | None:
        """解析重试任务归属的数据源：先按失败记录，再按批次兜底。"""
        raw_id = retry_entry.get("失败记录ID")
        raw = self._find_raw(raw_id)
        source_id = self._source_of_raw(raw)
        if source_id:
            return source_id
        batch_no = retry_entry.get("batch_no")
        if batch_no:
            for batch in store.rows("ingest_batch"):
                if batch.get("批次编号") == batch_no:
                    return str(batch.get("所属数据源") or "") or None
        return None

    @staticmethod
    def _find_raw(raw_id: Any) -> dict[str, Any] | None:
        try:
            rid = int(raw_id)
        except (TypeError, ValueError):
            return None
        return store.find("raw_records", rid)

    def _pending_retries_for_source(self, source_id: str) -> list[dict[str, Any]]:
        """该来源当前仍处于待重试的重试任务；其他来源的行不在此列。"""
        return [
            entry
            for entry in store.rows("retry_queue")
            if entry.get("status") == "待重试"
            and self._retry_source(entry) == source_id
        ]

    def _confirmed_manifest_entry(self, raw_id: int) -> dict[str, Any] | None:
        """该原始记录是否已有被确认的有效补做结果。"""
        for entry in store.rows(MANIFEST_TABLE):
            if (
                entry.get("处置结果") == RESULT_CONFIRMED
                and entry.get("失败记录ID") == str(raw_id)
            ):
                return entry
        return None

    def _find_credential(self, code: str) -> dict[str, Any] | None:
        for cred in store.rows(CREDENTIAL_TABLE):
            if cred.get("凭证编号") == code:
                return cred
        return None

    def _published_rule(self) -> dict[str, Any] | None:
        for rule in store.rows("field_parser"):
            if rule.get("status") == "已发布":
                return rule
        return None

    # ---------- 领取 ----------
    def claim_retries(
        self,
        shift: str,
        source_id: str,
        record_ids: list[Any] | None = None,
    ) -> dict[str, Any]:
        """班组按来源领取待重试记录，签发带行范围的领取凭证。

        不阻塞其他班组的在持凭证（交接并发允许重叠领取），但会在响应里
        列出同源仍在有效期内的凭证，提醒存在补做冲突风险。
        """
        shift = (shift or "").strip()
        source_id = (source_id or "").strip()
        if not shift:
            return {"ok": False, "message": "缺少班组标识"}
        sources = store.rows("source_registry")
        if not any(s.get("来源标识") == source_id for s in sources):
            return {"ok": False, "message": f"数据源 {source_id} 未登记，无法核对归属"}

        pending = self._pending_retries_for_source(source_id)
        claimable = {int(e["失败记录ID"]): e for e in pending if str(e.get("失败记录ID", "")).isdigit()}

        rejected: list[dict[str, Any]] = []
        wanted = _normalize_ids(record_ids)
        if wanted is None:
            scope_ids = sorted(claimable)
        else:
            scope_ids = []
            for rid in wanted:
                if rid in claimable:
                    scope_ids.append(rid)
                else:
                    rejected.append({
                        "失败记录ID": str(rid),
                        "原因": "不属于该来源的待重试记录，留在本来所在的批次",
                    })

        if not scope_ids:
            return {
                "ok": False,
                "message": f"数据源 {source_id} 没有可领取的待重试记录",
                "rejected": rejected,
            }

        code = f"CLM-{_next_id(CREDENTIAL_TABLE):04d}"
        credential = {
            "id": _next_id(CREDENTIAL_TABLE),
            "凭证编号": code,
            "班组": shift,
            "来源标识": source_id,
            "行范围": sorted(scope_ids),
            "行数": len(scope_ids),
            "凭证摘要": _credential_token(code, shift, source_id, sorted(scope_ids)),
            "移交自": "",
            "status": CRED_ACTIVE,
            "pending": True,
            "abnormal": False,
            "issued_at": _now_iso(),
            "settled_at": "",
        }
        store.rows(CREDENTIAL_TABLE).append(credential)

        # 非破坏式登记领取轨迹：同一条记录可被交接双方先后领取
        for rid in scope_ids:
            entry = claimable.get(rid)
            if entry is not None:
                history = entry.setdefault("claim_history", [])
                history.append(code)

        overlapping = [
            {
                "凭证编号": c.get("凭证编号"),
                "班组": c.get("班组"),
                "行范围": c.get("行范围"),
            }
            for c in store.rows(CREDENTIAL_TABLE)
            if c.get("来源标识") == source_id
            and c.get("status") == CRED_ACTIVE
            and c.get("凭证编号") != code
        ]
        return {
            "ok": True,
            "message": f"{shift}已领取数据源 {source_id} 的 {len(scope_ids)} 条待重试记录",
            "credential": credential,
            "overlapping_credentials": overlapping,
            "rejected": rejected,
        }

    # ---------- 补做 ----------
    def redo_with_credential(
        self,
        credential_code: str,
        record_ids: list[Any] | None = None,
    ) -> dict[str, Any]:
        """补做入口：按来源核对归属并读取原文，生成归属核验后的补做范围。

        只有同时通过来源归属核验与凭证行范围核验的记录才进入补做范围；
        超出凭证的记录留在本来所在的批次，其他来源的待重试行继续照原处置。
        同一原始记录在交接并发时只确认一份有效补做结果。
        """
        credential = self._find_credential((credential_code or "").strip())
        if credential is None:
            return {"ok": False, "message": f"领取凭证 {credential_code} 不存在"}
        if credential.get("status") != CRED_ACTIVE:
            return {
                "ok": False,
                "message": f"领取凭证 {credential_code} 已{credential.get('status')}，无权补做",
            }

        rule = self._published_rule()
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        scope = [int(r) for r in credential.get("行范围", [])]
        wanted = _normalize_ids(record_ids)
        if wanted is None:
            wanted = list(scope)

        confirmed: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        verified_scope: list[int] = []

        for rid in wanted:
            raw = self._find_raw(rid)
            # 归属核验：记录必须真实存在且属于凭证登记的数据源
            owner = self._source_of_raw(raw)
            if raw is None or owner != credential.get("来源标识"):
                rejected.append({
                    "失败记录ID": str(rid),
                    "原因": "来源归属核验未通过，记录留在本来所在的批次",
                })
                self._write_manifest(
                    credential, rid, raw, RESULT_REJECTED,
                    note="来源归属核验未通过",
                )
                continue
            # 行范围核验：超出凭证行范围的记录不在本次补做权限内
            if rid not in scope:
                rejected.append({
                    "失败记录ID": str(rid),
                    "原因": "超出凭证行范围，记录留在本来所在的批次",
                })
                self._write_manifest(
                    credential, rid, raw, RESULT_REJECTED,
                    note="超出凭证行范围",
                )
                continue
            verified_scope.append(rid)

            # 冲突裁决：交接并发时只确认一份有效的补做结果
            winner = self._confirmed_manifest_entry(rid)
            if winner is not None:
                conflicts.append({
                    "失败记录ID": str(rid),
                    "冲突凭证": winner.get("凭证编号"),
                    "冲突班组": winner.get("班组"),
                })
                self._write_manifest(
                    credential, rid, raw, RESULT_CONFLICT,
                    note="该记录已被另一张凭证确认补做",
                    peer_credential=winner,
                )
                continue

            # 读取原始正文并交给解析
            entity = self._parse_and_register(raw, credential, rule)
            self._mark_retry_resolved(rid, credential)
            confirmed.append({
                "失败记录ID": str(rid),
                "产出明细ID": str(entity["id"]),
            })
            self._write_manifest(
                credential, rid, raw, RESULT_CONFIRMED,
                note="补做确认",
                produced_entity_id=entity["id"],
            )

        # 凭证行范围内的记录全部有了确认结果（不论出自哪张凭证）即结算
        if scope and all(self._confirmed_manifest_entry(rid) for rid in scope):
            credential["status"] = CRED_SETTLED
            credential["pending"] = False
            credential["settled_at"] = _now_iso()

        return {
            "ok": True,
            "message": (
                f"凭证 {credential['凭证编号']} 补做完成："
                f"确认 {len(confirmed)} 条，冲突 {len(conflicts)} 条，驳回 {len(rejected)} 条"
            ),
            "credential": credential["凭证编号"],
            "补做范围": sorted(verified_scope),
            "confirmed": confirmed,
            "conflicts": conflicts,
            "rejected": rejected,
        }

    def _parse_and_register(
        self,
        raw: dict[str, Any],
        credential: dict[str, Any],
        rule: dict[str, Any],
    ) -> dict[str, Any]:
        """读取原始正文交给解析，产出带来源凭证标记的标准化明细。"""
        body = raw.get("原始正文", "")
        fields = pipeline._parse_body(body, rule)
        schema_ver = next(
            (sv for sv in store.rows("schema_version") if sv.get("status") == "已发布"),
            None,
        )
        entity = {
            "id": _next_id("parsed_entity"),
            "来源批次": raw.get("批次编号", ""),
            "原始记录ID": str(raw["id"]),
            "解析规则版本": rule.get("规则名称", "unknown"),
            "标准化字段集": json.dumps(fields, ensure_ascii=False),
            "校验状态": "",
            "status": "待校验",
            "pending": True,
            "abnormal": False,
            "parser_rule": rule.get("规则名称"),
            "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
            "parsed_at": _now_iso(),
            "领取凭证": credential.get("凭证编号"),
            "补做班组": credential.get("班组"),
            "补做来源": credential.get("来源标识"),
        }
        store.rows("parsed_entity").append(entity)
        raw["status"] = "已解析"
        raw["解析状态"] = "已解析"
        raw["pending"] = False
        raw["abnormal"] = False
        return entity

    def _mark_retry_resolved(self, raw_id: int, credential: dict[str, Any]) -> None:
        """把对应的待重试任务标记为已被该凭证处置。"""
        for entry in store.rows("retry_queue"):
            if entry.get("失败记录ID") != str(raw_id):
                continue
            if entry.get("status") not in ("待重试", "重试中"):
                continue
            try:
                count = int(entry.get("重试次数", "0"))
            except (TypeError, ValueError):
                count = 0
            entry["重试次数"] = str(count + 1)
            entry["status"] = "重试中"
            entry["pending"] = False
            entry["下次重试时间"] = _now_iso()
            entry["resolved_by"] = credential.get("凭证编号")
            entry["resolved_at"] = _now_iso()

    def _write_manifest(
        self,
        credential: dict[str, Any],
        raw_id: int,
        raw: dict[str, Any] | None,
        result: str,
        *,
        note: str = "",
        produced_entity_id: int | None = None,
        peer_credential: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """处置结果回写交接清单：冲突双方的归属凭证都留档可核验。"""
        entry = {
            "id": _next_id(MANIFEST_TABLE),
            "凭证编号": credential.get("凭证编号"),
            "凭证摘要": credential.get("凭证摘要"),
            "班组": credential.get("班组"),
            "来源标识": credential.get("来源标识"),
            "失败记录ID": str(raw_id),
            "批次编号": (raw or {}).get("批次编号", ""),
            "原始行号": (raw or {}).get("原始行号", ""),
            "处置结果": result,
            "处置说明": note,
            "产出明细ID": str(produced_entity_id) if produced_entity_id else "",
            "冲突凭证": (peer_credential or {}).get("凭证编号", ""),
            "冲突凭证摘要": (peer_credential or {}).get("凭证摘要", ""),
            "处置时间": _now_iso(),
            "status": result,
            "pending": result == RESULT_REJECTED,
            "abnormal": result != RESULT_CONFIRMED,
        }
        store.rows(MANIFEST_TABLE).append(entry)
        return entry

    # ---------- 跨班移交 ----------
    def transfer_credential(
        self,
        credential_code: str,
        to_shift: str,
        record_ids: list[Any] | None = None,
    ) -> dict[str, Any]:
        """跨班移交：验证权限和行范围后，把凭证内记录移交给新班组。

        超出凭证行范围的记录留在本来所在的批次；已被确认补做的记录不再
        移交。原凭证保留可核验的归属摘要，行范围相应收缩。
        """
        credential = self._find_credential((credential_code or "").strip())
        if credential is None:
            return {"ok": False, "message": f"领取凭证 {credential_code} 不存在"}
        if credential.get("status") != CRED_ACTIVE:
            return {
                "ok": False,
                "message": f"领取凭证 {credential_code} 已{credential.get('status')}，不能移交",
            }
        to_shift = (to_shift or "").strip()
        if not to_shift:
            return {"ok": False, "message": "缺少接收班组标识"}

        scope = [int(r) for r in credential.get("行范围", [])]
        wanted = _normalize_ids(record_ids)
        if wanted is None:
            wanted = list(scope)

        transferable: list[int] = []
        rejected: list[dict[str, Any]] = []
        for rid in wanted:
            if rid not in scope:
                rejected.append({
                    "失败记录ID": str(rid),
                    "原因": "超出凭证行范围，记录留在本来所在的批次",
                })
                continue
            if self._confirmed_manifest_entry(rid) is not None:
                rejected.append({
                    "失败记录ID": str(rid),
                    "原因": "已有确认的补做结果，不再移交",
                })
                continue
            transferable.append(rid)

        if not transferable:
            return {
                "ok": False,
                "message": "没有可移交的记录",
                "rejected": rejected,
            }

        source_id = credential.get("来源标识", "")
        code = f"CLM-{_next_id(CREDENTIAL_TABLE):04d}"
        new_credential = {
            "id": _next_id(CREDENTIAL_TABLE),
            "凭证编号": code,
            "班组": to_shift,
            "来源标识": source_id,
            "行范围": sorted(transferable),
            "行数": len(transferable),
            "凭证摘要": _credential_token(code, to_shift, source_id, sorted(transferable)),
            "移交自": credential.get("凭证编号"),
            "status": CRED_ACTIVE,
            "pending": True,
            "abnormal": False,
            "issued_at": _now_iso(),
            "settled_at": "",
        }
        store.rows(CREDENTIAL_TABLE).append(new_credential)

        remaining = [rid for rid in scope if rid not in transferable]
        credential["行范围"] = sorted(remaining)
        credential["行数"] = len(remaining)
        if not remaining:
            credential["status"] = CRED_TRANSFERRED
            credential["pending"] = False
            credential["settled_at"] = _now_iso()

        return {
            "ok": True,
            "message": (
                f"凭证 {credential.get('凭证编号')} 的 {len(transferable)} 条记录"
                f"已移交给{to_shift}（新凭证 {code}）"
            ),
            "credential": new_credential,
            "transferred": sorted(transferable),
            "rejected": rejected,
        }

    # ---------- 核对 ----------
    def manifest(
        self,
        shift: str | None = None,
        source_id: str | None = None,
        result: str | None = None,
    ) -> dict[str, Any]:
        """交接清单查询：关联实际整理过的原始日志，核对哪一班生成了哪些明细。"""
        entries = list(store.rows(MANIFEST_TABLE))
        if shift:
            entries = [e for e in entries if e.get("班组") == shift]
        if source_id:
            entries = [e for e in entries if e.get("来源标识") == source_id]
        if result:
            entries = [e for e in entries if e.get("处置结果") == result]

        by_shift: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            if entry.get("处置结果") != RESULT_CONFIRMED:
                continue
            by_shift.setdefault(entry.get("班组", ""), []).append({
                "凭证编号": entry.get("凭证编号"),
                "失败记录ID": entry.get("失败记录ID"),
                "批次编号": entry.get("批次编号"),
                "原始行号": entry.get("原始行号"),
                "产出明细ID": entry.get("产出明细ID"),
            })
        return {
            "ok": True,
            "items": entries,
            "total": len(entries),
            "confirmed_by_shift": by_shift,
        }

    def list_credentials(
        self,
        shift: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        """领取凭证查询：交班后仍可凭摘要核验归属。"""
        entries = list(store.rows(CREDENTIAL_TABLE))
        if shift:
            entries = [e for e in entries if e.get("班组") == shift]
        if status:
            entries = [e for e in entries if e.get("status") == status]
        return {"ok": True, "items": entries, "total": len(entries)}


shift_handover = ShiftHandoverService()
