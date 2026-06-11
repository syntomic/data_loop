"""全链路 M1→M9 端到端 (README §7)。

默认 mini profile (Daft 单机 + Lance + Flink MiniCluster)。
切集群只改配置: python scripts/run_mini.py configs/cluster.yaml
"""
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import load_profile, resolve
from common.lake import Lake, _is_uri


def main(profile: str = "configs/mini.yaml"):
    cfg = load_profile(profile)
    lake = Lake(cfg)
    data_root = resolve(cfg, cfg["data_root"])
    if not _is_uri(str(cfg["data_root"])):
        shutil.rmtree(data_root, ignore_errors=True)

    import subprocess
    subprocess.run([sys.executable, str(resolve(cfg, "replay-data/generate.py"))], check=True)

    from offline.m1_warc_ingest import ingest
    from offline.m2_filter import filter as m2
    from offline.m3_dedup import dedup
    from offline.m4_quality import score as m4
    from offline.m5_corpus import build as m5
    from ablation import run as m6
    if cfg["m7_signal_ingest"].get("backend") == "flink":
        from online.m7_flink import job_flink as m7
    else:
        from online.m7_flink import job as m7
    from online.m8_pref import pipeline as m8

    ingest.run(cfg)
    print("M1 doc_raw:", len(lake.read("doc_raw")))
    m2.run(cfg)
    print("M2 kept:", sum(d["kept"] for d in lake.read("doc_filtered")))
    dedup.run(cfg)
    print("M3 doc_dedup:", len(lake.read("doc_dedup")))
    m4.run(cfg)
    print("M4 doc_scored:", len(lake.read("doc_scored")))
    manifests = m5.run(cfg)
    for layer, mf in manifests.items():
        print(f"M5 {layer}: docs={mf['doc_count']} tokens={mf['token_count']}")
    report = m6.run(cfg)
    print("M6 ablation:", report["conclusion"])
    m7.run(cfg)
    print("M7 turn_candidate:", len(lake.read("turn_candidate")))
    stats = m8.run(cfg)
    print("M8:", stats)

    from registry.db import Registry
    reg = Registry.from_cfg(cfg)
    pair = lake.read("preference_pair")[0]
    print("lineage(pair):", reg.trace("pair", pair["pair_id"]))
    doc = max(lake.read("doc_scored"), key=lambda d: d["quality_score"])
    print("lineage(doc):", reg.trace("doc", doc["doc_id"]))
    print("OK: 全链 9 模块端到端跑通")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "configs/mini.yaml")
