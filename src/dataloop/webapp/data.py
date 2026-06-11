"""可视化数据层: 纯函数, 读真实 mini-run 产物 (Lance 表 + SQLite registry)。

被 server.py(http.server)与 explorer.py(Streamlit)共用, 互不耦合具体前端。
默认读 configs/mini.yaml; 传 cfg 即可指向集群 profile。
"""
import datetime as _dt
import json

from dataloop.common.config import load_profile
from dataloop.common.lake import Lake
from dataloop.registry.db import Registry

DOC_TABLES = ["doc_raw", "doc_filtered", "doc_dedup", "doc_scored", "turn_candidate",
              "preference_pair", "human_queue", "rlhf_example"]


def default_cfg() -> dict:
    return load_profile("configs/mini.yaml")


def _trunc(v):
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.hex()[:32]
    if isinstance(v, str) and len(v) > 120:
        return v[:120] + "…"
    if isinstance(v, list) and len(v) > 6:
        return v[:6] + ["…"]
    return v


def pipeline(cfg: dict) -> dict:
    """九模块拓扑 + 各阶段真实指标 (缺失的表计 0, 提示先跑 run_mini)。"""
    lake = Lake(cfg)
    rows = {n: lake.read(n) for n in DOC_TABLES}
    filtered = rows["doc_filtered"]
    kept = sum(1 for d in filtered if d["kept"])
    turns = rows["turn_candidate"]
    reg = Registry.from_cfg(cfg)
    manifests = {r[0]: json.loads(r[3]) for r in
                 reg.query("SELECT version_id, kind, created_ts, manifest FROM dataset_version")
                 if r[1] == "corpus_manifest"}
    ablation = reg.query("SELECT decision_id, score_a, score_b, conclusion FROM ablation_report")

    def vers(name):
        return lake.latest_version(name)

    nodes = [
        {"id": "M1", "name": "warc-ingest", "lane": "offline", "table": "doc_raw",
         "stack": "Daft · warcio · trafilatura", "metric": len(rows["doc_raw"]), "unit": "文档",
         "version": vers("doc_raw")},
        {"id": "M2", "name": "filter", "lane": "offline", "table": "doc_filtered",
         "stack": "Daft · 规则集 · langid", "metric": kept, "unit": f"保留 / {len(filtered)} 总",
         "version": vers("doc_filtered")},
        {"id": "M3", "name": "dedup", "lane": "offline", "table": "doc_dedup",
         "stack": "Daft · MinHash LSH", "metric": len(rows["doc_dedup"]), "unit": "去重后",
         "version": vers("doc_dedup")},
        {"id": "M4", "name": "quality", "lane": "offline", "table": "doc_scored",
         "stack": "教师打标 · 回归头", "metric": len(rows["doc_scored"]), "unit": "带质量分",
         "version": vers("doc_scored")},
        {"id": "M5", "name": "corpus-build", "lane": "offline", "table": "corpus",
         "stack": "tokenize · 双阈值分层",
         "metric": sum(m.get("doc_count", 0) for m in manifests.values()), "unit": "stable+anneal 文档",
         "version": None,
         "detail": {k: {"doc_count": v.get("doc_count"), "token_count": v.get("token_count")}
                    for k, v in manifests.items()}},
        {"id": "M6", "name": "ablation", "lane": "loop", "table": None,
         "stack": "代理评估 · 显著性", "metric": len(ablation), "unit": "决策报告",
         "version": None,
         "detail": [{"decision": a[0], "score_a": round(a[1], 4), "score_b": round(a[2], 4),
                     "conclusion": a[3]} for a in ablation]},
        {"id": "M7", "name": "signal-ingest", "lane": "online", "table": "turn_candidate",
         "stack": "Flink · keyed state · watermark",
         "metric": len(turns), "unit": "TurnCandidate",
         "version": vers("turn_candidate"),
         "detail": {"late": sum(1 for t in turns if t.get("late")),
                    "with_signal": sum(1 for t in turns if t.get("thumbs") or t.get("user_edit")
                                       or t.get("regenerated") or t.get("stopped")
                                       or t.get("followup_correction"))}},
        {"id": "M8", "name": "pref-build", "lane": "online", "table": "preference_pair",
         "stack": "score · sample · generate · judge · qc",
         "metric": len(rows["preference_pair"]), "unit": "偏好对(auto)",
         "version": vers("preference_pair"),
         "detail": {"auto": len(rows["preference_pair"]), "human_queue": len(rows["human_queue"]),
                    "rlhf_example": len(rows["rlhf_example"])}},
        {"id": "M9", "name": "registry", "lane": "loop", "table": None,
         "stack": "版本 · 血缘 · 去污染 · 快照绑定",
         "metric": len(reg.query("SELECT 1 FROM dataset_version")), "unit": "已注册版本",
         "version": None},
    ]
    edges = [
        ("M1", "M2"), ("M2", "M3"), ("M3", "M4"), ("M4", "M5"), ("M5", "M6"),
        ("M7", "M8"), ("M6", "M9"), ("M5", "M9"), ("M8", "M9"),
        ("M6", "M2"), ("M8", "M7"),  # 闭环反馈
    ]
    return {"nodes": nodes, "edges": [{"from": a, "to": b} for a, b in edges]}


def table(cfg: dict, name: str, limit: int = 8) -> dict:
    lake = Lake(cfg)
    rows = lake.read(name)
    schema = list(rows[0].keys()) if rows else []
    sample = [{k: _trunc(v) for k, v in r.items()} for r in rows[:limit]]
    return {"name": name, "schema": schema, "count": len(rows),
            "version": lake.latest_version(name), "uri": lake.uri(name), "sample": sample}


def registry(cfg: dict) -> dict:
    reg = Registry.from_cfg(cfg)
    versions = []
    for vid, kind, ts, manifest in reg.query(
            "SELECT version_id, kind, created_ts, manifest FROM dataset_version"):
        versions.append({"version_id": vid, "kind": kind, "created_ts": ts,
                         "manifest": json.loads(manifest), "snapshots": reg.snapshots(vid)})
    ablation = [{"decision_id": a[0], "config_diff": a[1], "score_a": round(a[2], 4),
                 "score_b": round(a[3], 4), "significant": bool(a[4]), "conclusion": a[5],
                 "snapshots": reg.snapshots(a[0])}
                for a in reg.query("SELECT decision_id, config_diff, score_a, score_b, "
                                   "significant, conclusion FROM ablation_report")]
    decontam = reg.query("SELECT version_id, COUNT(*) FROM decontam_log GROUP BY version_id")
    return {"versions": versions, "ablation": ablation,
            "decontam": [{"version_id": d[0], "records": d[1]} for d in decontam]}


def lineage(cfg: dict, kind: str, ident: str) -> dict:
    reg = Registry.from_cfg(cfg)
    return {"root": {"kind": kind, "id": ident},
            "edges": [{"parent_kind": k, "parent_id": i} for k, i in reg.trace(kind, ident)]}


def sample_ids(cfg: dict) -> dict:
    """血缘探索起点: 一条偏好对与一篇高分文档。"""
    lake = Lake(cfg)
    pairs = lake.read("preference_pair")
    docs = lake.read("doc_scored")
    out = {}
    if pairs:
        out["pair"] = pairs[0]["pair_id"]
    if docs:
        out["doc"] = max(docs, key=lambda d: d["quality_score"])["doc_id"]
    return out
