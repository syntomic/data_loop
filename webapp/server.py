"""数据闭环可视化 Demo 后端 (纯标准库 http.server, 无新依赖)。

读取真实 mini-run 产物 (Lance 表 + SQLite registry), 以 JSON 暴露给前端:
  GET /api/pipeline            九模块拓扑 + 各阶段真实行数/指标
  GET /api/table/<name>        某表的 schema + 抽样行 + Lance 物理 version
  GET /api/registry            数据集版本 / 快照绑定 / 消融报告
  GET /api/lineage/<kind>/<id> 血缘反查
  GET /                        前端单页

启动: uv run python -m webapp.server   (默认 http://127.0.0.1:8000)
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import load_profile
from common.lake import Lake
from registry.db import Registry

STATIC = Path(__file__).resolve().parent / "static"
_CFG = load_profile("configs/mini.yaml")


def _lake() -> Lake:
    return Lake(_CFG)


def _reg() -> Registry:
    return Registry.from_cfg(_CFG)


def _count(lake: Lake, name: str) -> int:
    return len(lake.read(name))


def pipeline() -> dict:
    """九模块拓扑 + 各阶段真实指标 (缺失的表计 0, 提示先跑 run_mini)。"""
    lake = _lake()
    rows = {n: lake.read(n) for n in
            ["doc_raw", "doc_filtered", "doc_dedup", "doc_scored", "turn_candidate",
             "preference_pair", "human_queue", "rlhf_example"]}
    filtered = rows["doc_filtered"]
    kept = sum(1 for d in filtered if d["kept"])
    scored = rows["doc_scored"]
    turns = rows["turn_candidate"]
    reg = _reg()
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
         "stack": "教师打标 · 回归头", "metric": len(scored), "unit": "带质量分",
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


def table(name: str) -> dict:
    lake = _lake()
    rows = lake.read(name)
    schema = list(rows[0].keys()) if rows else []
    sample = []
    for r in rows[:8]:
        sample.append({k: _trunc(v) for k, v in r.items()})
    return {"name": name, "schema": schema, "count": len(rows),
            "version": lake.latest_version(name), "uri": lake.uri(name), "sample": sample}


def _trunc(v):
    import datetime as _dt
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.hex()[:32]
    if isinstance(v, str) and len(v) > 120:
        return v[:120] + "…"
    if isinstance(v, list) and len(v) > 6:
        return v[:6] + ["…"]
    return v


def registry() -> dict:
    reg = _reg()
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


def lineage(kind: str, ident: str) -> dict:
    reg = _reg()
    return {"root": {"kind": kind, "id": ident},
            "edges": [{"parent_kind": k, "parent_id": i} for k, i in reg.trace(kind, ident)]}


def sample_ids() -> dict:
    """给前端血缘探索一个起点: 取一条偏好对与一篇高分文档。"""
    lake = _lake()
    pairs = lake.read("preference_pair")
    docs = lake.read("doc_scored")
    out = {}
    if pairs:
        out["pair"] = pairs[0]["pair_id"]
    if docs:
        out["doc"] = max(docs, key=lambda d: d["quality_score"])["doc_id"]
    return out


class Handler(BaseHTTPRequestHandler):
    def _send(self, obj, ctype="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(
            obj, ensure_ascii=False, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if "json" in ctype or "html" in ctype else ""))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps({"error": msg}, ensure_ascii=False).encode())

    def do_GET(self):
        path = unquote(urlparse(self.path).path)
        try:
            if path in ("/", "/index.html"):
                return self._send((STATIC / "index.html").read_bytes(), "text/html")
            if path == "/api/pipeline":
                return self._send(pipeline())
            if path == "/api/registry":
                return self._send(registry())
            if path == "/api/sample-ids":
                return self._send(sample_ids())
            if path.startswith("/api/table/"):
                return self._send(table(path[len("/api/table/"):]))
            if path.startswith("/api/lineage/"):
                rest = path[len("/api/lineage/"):]
                if "/" not in rest:
                    return self._err(400, "用法: /api/lineage/<kind>/<id>")
                kind, ident = rest.split("/", 1)
                return self._send(lineage(kind, ident))
            return self._err(404, f"no route: {path}")
        except FileNotFoundError as e:
            return self._err(404, str(e))
        except Exception as e:  # noqa
            return self._err(500, f"{type(e).__name__}: {e}")

    def log_message(self, *a):  # 静默
        pass


def main(host: str = "127.0.0.1", port: int = 8000):
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"data-loop 可视化: http://{host}:{port}  (Ctrl-C 退出)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main(port=int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
