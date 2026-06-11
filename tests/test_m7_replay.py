"""里程碑 P4 验收: 迟到反馈正确关联, 隐私闸 100%。"""
import json

import pytest

from common.config import resolve
from common.lake import Lake
from online.m7_flink import job
from online.m7_flink.pii import scrub


@pytest.fixture(scope="module")
def turns(cfg):
    import subprocess, sys
    subprocess.run([sys.executable, str(resolve(cfg, "replay-data/generate.py"))], check=True)
    job.run(cfg)
    return Lake(cfg).read("turn_candidate")


def test_pii_scrub():
    assert scrub("电话13912345678 邮箱a@b.com") == "电话<PHONE> 邮箱<EMAIL>"


def test_consent_gate(turns, cfg):
    consent = {json.loads(l)["user_id"]: json.loads(l)["consent_ok"]
               for l in resolve(cfg, "replay-data/consent.jsonl").read_text().splitlines()}
    msgs = [json.loads(l) for l in resolve(cfg, "replay-data/message_events.jsonl").read_text().splitlines()]
    expect = {m["conversation_id"] for m in msgs if consent[m["user_id"]]}
    assert {t["conversation_id"] for t in turns} == expect


def test_no_pii_in_output(turns):
    for t in turns:
        assert "13912345678" not in (t["response"] or "")
        assert "a@b.com" not in (t["user_edit"] or "")


def test_late_within_window_joined(turns):
    # i%10==3: 反馈延迟 8min > watermark 5min, 但 < lateness 30min → 合入且 late=True
    late = [t for t in turns if t["late"]]
    assert late and all(t["thumbs"] != 0 or t["user_edit"] or t["regenerated"]
                        or t["stopped"] or t["followup_correction"] for t in late)


def test_too_late_dropped(turns):
    # i%10==7 (信号 regenerate, i 奇数 i=7,17,...) 延迟 40min > 30min → 信号丢弃
    t7 = [t for t in turns if int(t["conversation_id"][4:]) % 10 == 7]
    assert t7 and all(t["thumbs"] == 0 and not t["regenerated"] and not t["user_edit"]
                      and not t["stopped"] and not t["followup_correction"] for t in t7)
