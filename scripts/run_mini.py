"""mini profile 全链路: M1→M9 端到端 (README §7: 50 WARC → 千条偏好对的缩样版)。"""
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import load_profile, resolve
from common.io import read_table


def main():
    cfg = load_profile("configs/mini.yaml")
    data_root = resolve(cfg, cfg["data_root"])
    shutil.rmtree(data_root, ignore_errors=True)

    import subprocess
    subprocess.run([sys.executable, str(resolve(cfg, "replay-data/generate.py"))], check=True)

    from offline.m1_warc_ingest import ingest
    from offline.m2_filter import filter as m2
    from offline.m3_dedup import dedup
    from offline.m4_quality import score as m4
    from offline.m5_corpus import build as m5
    from ablation import run as m6
    from online.m7_flink import job as m7
    from online.m8_pref import pipeline as m8

    ingest.run(cfg)
    print("M1 doc_raw:", len(read_table(data_root / "doc_raw")))
    m2.run(cfg)
    kept = [d for d in read_table(data_root / "doc_filtered") if d["kept"]]
    print("M2 kept:", len(kept))
    dedup.run(cfg)
    print("M3 doc_dedup:", len(read_table(data_root / "doc_dedup")))
    m4.run(cfg)
    print("M4 doc_scored:", len(read_table(data_root / "doc_scored")))
    manifests = m5.run(cfg)
    for layer, mf in manifests.items():
        print(f"M5 {layer}: docs={mf['doc_count']} tokens={mf['token_count']}")
    report = m6.run(cfg)
    print("M6 ablation:", report["conclusion"])
    m7.run(cfg)
    print("M7 turn_candidate:", len(read_table(data_root / "turn_candidate")))
    stats = m8.run(cfg)
    print("M8:", stats)

    from registry.db import Registry
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    pair = read_table(data_root / "preference_pair")[0]
    print("lineage(pair):", reg.trace("pair", pair["pair_id"]))
    doc = max(read_table(data_root / "doc_scored"), key=lambda d: d["quality_score"])
    print("lineage(doc):", reg.trace("doc", doc["doc_id"]))
    print("OK: 全链 9 模块端到端跑通")


if __name__ == "__main__":
    main()
