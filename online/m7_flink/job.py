"""M7 signal-ingest (README §5.1)。

mini profile: 文件 replay 驱动的事件时间模拟, 语义与 Flink keyed state 作业一致:
- conversation_id keyBy, keyed state + timer
- watermark 5min, allowed lateness 30min (此窗口内的迟到反馈仍合入并标 late)
- state TTL 24h
- consent 维表 lookup join, consent_ok=false 丢弃; PII 清洗 UDF
cluster profile 用同等语义的 Flink SQL + DataStream 作业。
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from common.config import resolve
from common.io import write_table
from schemas.tables import TURN_CANDIDATE
from .pii import scrub


def _read_jsonl(p: Path):
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def run(cfg: dict) -> Path:
    m = cfg["m7_signal_ingest"]
    consent = {c["user_id"]: c["consent_ok"] for c in _read_jsonl(resolve(cfg, m["consent_table"]))}
    events = sorted(
        [e | {"_topic": "message"} for e in _read_jsonl(resolve(cfg, m["message_events"]))] +
        [e | {"_topic": "feedback"} for e in _read_jsonl(resolve(cfg, m["feedback_events"]))],
        key=lambda e: e["ts"])  # replay 按到达序 = ts 序; 事件内 event_ts 可乱序

    state: dict[str, dict] = {}   # key: conv:turn → turn 聚合状态
    fired: dict[str, dict] = {}
    watermark = 0
    for ev in events:
        watermark = max(watermark, ev["ts"] - m["watermark_ms"])
        key = f"{ev['conversation_id']}:{ev['turn_id']}"
        if not consent.get(ev["user_id"], False):
            continue
        if ev["_topic"] == "message":
            state[key] = {
                "conversation_id": ev["conversation_id"], "turn_id": ev["turn_id"],
                "model_version": ev["model_version"],
                "prompt": scrub(ev["prompt"]), "response": scrub(ev["response"]),
                "user_edit": None, "thumbs": 0, "regenerated": False, "stopped": False,
                "followup_correction": False, "late": False,
                "event_ts": ev["event_ts"], "timer": ev["event_ts"] + m["watermark_ms"],
                "expire": ev["ts"] + m["state_ttl_ms"],
            }
        else:
            tgt = state.get(key) or fired.get(key)
            if tgt is None or ev["ts"] > tgt["expire"]:
                continue  # 无 state / TTL 过期
            if ev["ts"] > tgt["timer"] + m["allowed_lateness_ms"]:
                continue  # 超出 allowed lateness, 丢弃 (无论 timer 是否已触发)
            # timer 在 watermark = ts - 5min 越过时已触发 → 此反馈算迟到
            late = ev["ts"] - m["watermark_ms"] >= tgt["timer"]
            sig = ev["signal"]
            if sig == "thumbs_up":
                tgt["thumbs"] = 1
            elif sig == "thumbs_down":
                tgt["thumbs"] = -1
            elif sig == "regenerate":
                tgt["regenerated"] = True
            elif sig == "stop":
                tgt["stopped"] = True
            elif sig == "edit":
                tgt["user_edit"] = scrub(ev.get("edit_text"))
            elif sig == "followup_correction":
                tgt["followup_correction"] = True
            if late:
                tgt["late"] = True
        for k in [k for k, s in state.items() if s["timer"] <= watermark]:
            fired[k] = state.pop(k)  # timer 触发, 进入 lateness 窗口
    fired.update(state)  # 流结束冲刷

    rows = [{k: v for k, v in s.items() if k not in ("timer", "expire")} |
            {"dt": datetime.fromtimestamp(s["event_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")}
            for s in fired.values()]
    out = resolve(cfg, cfg["data_root"]) / "turn_candidate"
    write_table(rows, TURN_CANDIDATE, out, partition="dt")
    return out
