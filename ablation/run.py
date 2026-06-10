"""M6 消融最小闭环 (README §4, 里程碑 P3): 一对 manifest 差异 → 报告入 registry。
默认决策: per-dump vs global 去重。退火评估法在 mini 中以低成本代理预筛体现。"""
from common.config import resolve
from common.io import read_table
from offline.m3_dedup import dedup
from offline.m4_quality import score as quality
from registry.db import Registry
from .eval_harness import proxy_eval, significant


def run(cfg: dict) -> dict:
    seed = cfg["m6_ablation"]["seed"]
    arms = {}
    for arm, scope in [("a", "per_dump"), ("b", "global")]:
        dedup.run(cfg, scope=scope, out_name=f"doc_dedup_{arm}")
        quality.run(cfg, in_name=f"doc_dedup_{arm}", out_name=f"doc_scored_{arm}")
        docs = read_table(resolve(cfg, cfg["data_root"]) / f"doc_scored_{arm}")
        arms[arm] = proxy_eval(docs, seed)
    sig = significant(arms["a"], arms["b"])
    conclusion = ("per_dump 与 global 无显著差异, 维持默认 per_dump" if not sig
                  else ("采用 per_dump" if arms["a"] >= arms["b"] else "采用 global"))
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    reg.save_ablation("dedup_scope-v1", "m3_dedup.scope: per_dump vs global",
                      arms["a"], arms["b"], sig, conclusion,
                      "doc_scored_a", "doc_scored_b")
    return {"decision_id": "dedup_scope-v1", "score_a": arms["a"], "score_b": arms["b"],
            "significant": sig, "conclusion": conclusion}
