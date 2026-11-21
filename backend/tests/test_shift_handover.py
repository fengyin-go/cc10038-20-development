"""重试队列交接班补做测试：按来源核对归属、归属核验后的补做范围、
交接并发只确认一份有效结果、跨班移交验证权限和行范围，以及交班后
通过交接清单核对哪一班、哪张领取凭证生成了哪些标准化明细。"""
from __future__ import annotations

import pytest

from app.services.shift_handover import (
    CRED_ACTIVE,
    CRED_SETTLED,
    CRED_TRANSFERRED,
    RESULT_CONFIRMED,
    RESULT_CONFLICT,
    RESULT_REJECTED,
    shift_handover,
)
from app.store import store


def _make_failed_records(pipeline, source_id: str, bodies: list[str]):
    """模拟交换机断流：采集到的行解析失败，进入重试队列等待补做。"""
    pipeline.register_source(source_id, "syslog", "idc-1")
    pipeline.validate_source(source_id)
    batch = pipeline.start_ingest_batch(
        source_id, "2026-11-21", [{"body": body} for body in bodies]
    )
    assert batch["ok"] is True
    batch_no = batch["batch"]["批次编号"]
    raw_ids: list[int] = []
    for raw in store.rows("raw_records"):
        if raw.get("批次编号") != batch_no:
            continue
        raw["status"] = "解析失败"
        raw["解析状态"] = "解析失败"
        raw["abnormal"] = True
        raw_ids.append(int(raw["id"]))
        store.rows("retry_queue").append({
            "id": max((int(r.get("id", 0)) for r in store.rows("retry_queue")), default=0) + 1,
            "失败记录ID": str(raw["id"]),
            "失败原因": "交换机断流，解析中断",
            "重试次数": "0",
            "最大重试": "3",
            "下次重试时间": "2026-11-21T08:00:00",
            "status": "待重试",
            "pending": True,
            "abnormal": False,
            "batch_no": batch_no,
        })
    return batch_no, sorted(raw_ids)


def _parsed_entities_for(batch_no: str):
    return [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]


def _retry_entry(raw_id: int):
    for entry in store.rows("retry_queue"):
        if entry.get("失败记录ID") == str(raw_id):
            return entry
    return None


@pytest.fixture
def switch_outage(pipeline):
    """交换机断流留下的两行日志 + 别处（另一来源）的一行。"""
    batch_no, raw_ids = _make_failed_records(
        pipeline,
        "switch-syslog",
        [
            "2026-11-21T08:00:03|WARN|Gi0/1 link-down",
            "2026-11-21T08:00:04|ERROR|uplink lost",
        ],
    )
    other_batch, other_ids = _make_failed_records(
        pipeline,
        "core-router",
        ["2026-11-21T08:01:00|INFO|bgp keepalive"],
    )
    return {
        "batch_no": batch_no,
        "raw_ids": raw_ids,
        "other_batch": other_batch,
        "other_ids": other_ids,
    }


class TestClaim:
    """按来源领取待重试记录，签发带行范围的领取凭证。"""

    def test_claim_generates_scoped_credential(self, pipeline, switch_outage):
        result = shift_handover.claim_retries("白班", "switch-syslog")
        assert result["ok"] is True
        cred = result["credential"]
        assert cred["班组"] == "白班"
        assert cred["来源标识"] == "switch-syslog"
        assert cred["行范围"] == switch_outage["raw_ids"]
        assert cred["status"] == CRED_ACTIVE
        assert cred["凭证摘要"]

    def test_claim_does_not_touch_other_sources(self, pipeline, switch_outage):
        shift_handover.claim_retries("白班", "switch-syslog")
        other_retry = _retry_entry(switch_outage["other_ids"][0])
        assert other_retry["status"] == "待重试"
        assert "claim_history" not in other_retry

    def test_claim_unregistered_source_rejected(self, pipeline):
        result = shift_handover.claim_retries("白班", "ghost-source")
        assert result["ok"] is False
        assert "未登记" in result["message"]

    def test_claim_without_pending_retries_rejected(self, pipeline):
        pipeline.register_source("quiet-src", "http", "default")
        result = shift_handover.claim_retries("白班", "quiet-src")
        assert result["ok"] is False

    def test_claim_with_explicit_ids_filters_foreign_rows(self, pipeline, switch_outage):
        wanted = [switch_outage["raw_ids"][0], switch_outage["other_ids"][0]]
        result = shift_handover.claim_retries("白班", "switch-syslog", wanted)
        assert result["ok"] is True
        assert result["credential"]["行范围"] == [switch_outage["raw_ids"][0]]
        assert [r["失败记录ID"] for r in result["rejected"]] == [
            str(switch_outage["other_ids"][0])
        ]

    def test_handover_allows_overlapping_claims(self, pipeline, switch_outage):
        """换班：旧班组的凭证不回收，新班组可领取同一条，响应提示重叠。"""
        first = shift_handover.claim_retries("白班", "switch-syslog")
        second = shift_handover.claim_retries("夜班", "switch-syslog")
        assert second["ok"] is True
        assert second["credential"]["行范围"] == first["credential"]["行范围"]
        assert [c["凭证编号"] for c in second["overlapping_credentials"]] == [
            first["credential"]["凭证编号"]
        ]


class TestRedo:
    """归属核验后的补做范围：读取原文交给解析，结果回写交接清单。"""

    def test_redo_reads_body_and_parses(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        result = shift_handover.redo_with_credential(cred["凭证编号"])
        assert result["ok"] is True
        assert result["补做范围"] == switch_outage["raw_ids"]
        assert len(result["confirmed"]) == 2

        entities = _parsed_entities_for(switch_outage["batch_no"])
        assert len(entities) == 2
        for entity in entities:
            assert entity["领取凭证"] == cred["凭证编号"]
            assert entity["补做班组"] == "白班"
        first = sorted(entities, key=lambda e: e["原始记录ID"])[0]
        assert "Gi0/1 link-down" in first["标准化字段集"]

    def test_redo_marks_retry_resolved(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        shift_handover.redo_with_credential(cred["凭证编号"])
        retry = _retry_entry(switch_outage["raw_ids"][0])
        assert retry["重试次数"] == "1"
        assert retry["pending"] is False
        assert retry["resolved_by"] == cred["凭证编号"]

    def test_redo_writes_manifest(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        shift_handover.redo_with_credential(cred["凭证编号"])
        manifest = shift_handover.manifest(source_id="switch-syslog")
        assert manifest["total"] == 2
        for entry in manifest["items"]:
            assert entry["处置结果"] == RESULT_CONFIRMED
            assert entry["凭证编号"] == cred["凭证编号"]
            assert entry["批次编号"] == switch_outage["batch_no"]
            assert entry["产出明细ID"]

    def test_redo_rejects_foreign_row_and_keeps_it_pending(self, pipeline, switch_outage):
        """夹带别处的一行：归属核验未通过，留在本来所在的批次照原处置。"""
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        foreign = switch_outage["other_ids"][0]
        result = shift_handover.redo_with_credential(
            cred["凭证编号"], [switch_outage["raw_ids"][0], foreign]
        )
        assert result["ok"] is True
        assert result["补做范围"] == [switch_outage["raw_ids"][0]]
        assert [r["失败记录ID"] for r in result["rejected"]] == [str(foreign)]
        # 别处的行继续待在原批次、原重试状态
        foreign_retry = _retry_entry(foreign)
        assert foreign_retry["status"] == "待重试"
        assert foreign_retry["pending"] is True
        assert _parsed_entities_for(switch_outage["other_batch"]) == []

    def test_redo_rejects_rows_outside_credential_scope(self, pipeline, switch_outage):
        first = switch_outage["raw_ids"][0]
        cred = shift_handover.claim_retries("白班", "switch-syslog", [first])["credential"]
        result = shift_handover.redo_with_credential(
            cred["凭证编号"], switch_outage["raw_ids"]
        )
        assert result["补做范围"] == [first]
        assert [r["失败记录ID"] for r in result["rejected"]] == [
            str(switch_outage["raw_ids"][1])
        ]

    def test_redo_unknown_credential(self, pipeline, switch_outage):
        result = shift_handover.redo_with_credential("CLM-9999")
        assert result["ok"] is False

    def test_redo_settles_credential_when_scope_confirmed(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        shift_handover.redo_with_credential(cred["凭证编号"])
        stored = shift_handover.list_credentials(status=CRED_SETTLED)
        assert [c["凭证编号"] for c in stored["items"]] == [cred["凭证编号"]]
        again = shift_handover.redo_with_credential(cred["凭证编号"])
        assert again["ok"] is False
        assert "无权补做" in again["message"]


class TestShiftChangeConflict:
    """换班场景：白班、夜班交错回填同一行，只确认一份有效补做结果。"""

    def test_interleaved_redo_confirms_only_one_result(self, pipeline, switch_outage):
        line1, line2 = switch_outage["raw_ids"]
        foreign = switch_outage["other_ids"][0]

        # 换班：白班先领取，夜班随后领取同一来源的同一批行
        day_cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        night_cred = shift_handover.claim_retries("夜班", "switch-syslog")["credential"]
        assert day_cred["行范围"] == night_cred["行范围"] == [line1, line2]

        # 白班先补做第一行 → 确认
        day_first = shift_handover.redo_with_credential(day_cred["凭证编号"], [line1])
        assert day_first["confirmed"] == [
            {"失败记录ID": str(line1), "产出明细ID": day_first["confirmed"][0]["产出明细ID"]}
        ]

        # 夜班交错回填同一行 → 冲突未确认，不产生新明细
        night_same = shift_handover.redo_with_credential(night_cred["凭证编号"], [line1])
        assert night_same["confirmed"] == []
        assert night_same["conflicts"] == [
            {
                "失败记录ID": str(line1),
                "冲突凭证": day_cred["凭证编号"],
                "冲突班组": "白班",
            }
        ]

        # 夜班补做第二行，并夹带别处的一行 → 第二行确认，夹带行驳回
        night_second = shift_handover.redo_with_credential(
            night_cred["凭证编号"], [line2, foreign]
        )
        assert night_second["补做范围"] == [line2]
        assert len(night_second["confirmed"]) == 1
        assert [r["失败记录ID"] for r in night_second["rejected"]] == [str(foreign)]

        # 白班再回填第二行 → 同样只落得下一个冲突记录
        day_second = shift_handover.redo_with_credential(day_cred["凭证编号"], [line2])
        assert day_second["confirmed"] == []
        assert day_second["conflicts"][0]["冲突凭证"] == night_cred["凭证编号"]

        # 同一行只确认一份有效补做结果：交换机批次只落下两条标准化明细
        entities = _parsed_entities_for(switch_outage["batch_no"])
        assert len(entities) == 2
        by_raw = {e["原始记录ID"]: e for e in entities}
        assert by_raw[str(line1)]["领取凭证"] == day_cred["凭证编号"]
        assert by_raw[str(line1)]["补做班组"] == "白班"
        assert by_raw[str(line2)]["领取凭证"] == night_cred["凭证编号"]
        assert by_raw[str(line2)]["补做班组"] == "夜班"

        # 交接清单保留两边能核验的归属凭证
        manifest = shift_handover.manifest(source_id="switch-syslog")
        conflicts = [e for e in manifest["items"] if e["处置结果"] == RESULT_CONFLICT]
        assert len(conflicts) == 2
        conflict_line1 = next(e for e in conflicts if e["失败记录ID"] == str(line1))
        assert conflict_line1["凭证编号"] == night_cred["凭证编号"]
        assert conflict_line1["凭证摘要"] == night_cred["凭证摘要"]
        assert conflict_line1["冲突凭证"] == day_cred["凭证编号"]
        assert conflict_line1["冲突凭证摘要"] == day_cred["凭证摘要"]

        # 交班以后核对：哪一班、哪张领取凭证生成了哪些明细
        confirmed = manifest["confirmed_by_shift"]
        assert [e["产出明细ID"] for e in confirmed["白班"]] == [
            by_raw[str(line1)]["id"] and str(by_raw[str(line1)]["id"])
        ]
        assert [e["产出明细ID"] for e in confirmed["夜班"]] == [
            str(by_raw[str(line2)]["id"])
        ]
        assert confirmed["白班"][0]["凭证编号"] == day_cred["凭证编号"]
        assert confirmed["夜班"][0]["凭证编号"] == night_cred["凭证编号"]

        # 夹带行仍在本来所在的批次等待原处置
        assert _retry_entry(foreign)["status"] == "待重试"
        assert _parsed_entities_for(switch_outage["other_batch"]) == []


class TestTransfer:
    """跨班移交：验证权限和行范围，超出凭证的记录留在本来所在的批次。"""

    def test_transfer_moves_scope_to_new_shift(self, pipeline, switch_outage):
        line1, line2 = switch_outage["raw_ids"]
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        result = shift_handover.transfer_credential(cred["凭证编号"], "夜班", [line1])
        assert result["ok"] is True
        new_cred = result["credential"]
        assert new_cred["班组"] == "夜班"
        assert new_cred["行范围"] == [line1]
        assert new_cred["移交自"] == cred["凭证编号"]
        # 原凭证行范围收缩，归属摘要仍可核验
        old = shift_handover.list_credentials(status=CRED_ACTIVE)["items"]
        old = next(c for c in old if c["凭证编号"] == cred["凭证编号"])
        assert old["行范围"] == [line2]

        redo = shift_handover.redo_with_credential(new_cred["凭证编号"])
        assert redo["补做范围"] == [line1]

    def test_transfer_rejects_out_of_scope_rows(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        foreign = switch_outage["other_ids"][0]
        result = shift_handover.transfer_credential(
            cred["凭证编号"], "夜班", [foreign]
        )
        assert result["ok"] is False
        assert [r["失败记录ID"] for r in result["rejected"]] == [str(foreign)]
        # 超出凭证的记录留在本来所在的批次
        assert _retry_entry(foreign)["status"] == "待重试"
        assert shift_handover.list_credentials()["total"] == 1

    def test_transfer_full_scope_retires_old_credential(self, pipeline, switch_outage):
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        result = shift_handover.transfer_credential(cred["凭证编号"], "夜班")
        assert result["ok"] is True
        retired = shift_handover.list_credentials(status=CRED_TRANSFERRED)
        assert [c["凭证编号"] for c in retired["items"]] == [cred["凭证编号"]]
        redo = shift_handover.redo_with_credential(cred["凭证编号"])
        assert redo["ok"] is False
        assert "无权补做" in redo["message"]

    def test_transfer_skips_already_confirmed_rows(self, pipeline, switch_outage):
        line1, line2 = switch_outage["raw_ids"]
        cred = shift_handover.claim_retries("白班", "switch-syslog")["credential"]
        shift_handover.redo_with_credential(cred["凭证编号"], [line1])
        result = shift_handover.transfer_credential(cred["凭证编号"], "夜班")
        assert result["ok"] is True
        assert result["transferred"] == [line2]
        assert [r["失败记录ID"] for r in result["rejected"]] == [str(line1)]
