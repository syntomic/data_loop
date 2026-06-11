"""端到端 mini run (README §7 验收: 全链 9 模块跑通; 里程碑 P5/P6)。"""
import shutil
import subprocess
import sys

import pytest

from common.config import resolve
from common.io import read_table
from registry.db import Registry


@pytest.fixture(scope="module")
def pipeline(cfg):
    shutil.rmtree(resolve(cfg, cfg["data_root"]), ignore_errors=True)
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
    ingest.run(cfg), m2.run(cfg), dedup.run(cfg), m4.run(cfg)
    manifests = m5.run(cfg)
    report = m6.run(cfg)
    m7.run(cfg)
    stats = m8.run(cfg)
    return cfg, manifests, report, stats


def test_layers(pipeline):
    _, manifests, _, _ = pipeline
    assert manifests["stable"]["doc_count"] >= manifests["anneal"]["doc_count"] > 0


def test_dedup_removed_near_dups(pipeline):
    cfg = pipeline[0]
    kept = [d for d in read_table(resolve(cfg, cfg["data_root"]) / "doc_filtered") if d["kept"]]
    deduped = read_table(resolve(cfg, cfg["data_root"]) / "doc_dedup")
    assert len(deduped) < len(kept)


def test_ablation_report_in_registry(pipeline):
    cfg, _, report, _ = pipeline
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    row = reg.conn.execute("SELECT conclusion FROM ablation_report WHERE decision_id=?",
                           (report["decision_id"],)).fetchone()
    assert row and row[0] == report["conclusion"]


def test_pref_dataset_registered_with_lineage(pipeline):
    cfg, _, _, stats = pipeline
    assert stats["auto"] > 0
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    pair = read_table(resolve(cfg, cfg["data_root"]) / "preference_pair")[0]
    trace = reg.trace("pair", pair["pair_id"])
    assert trace and trace[0][0] == "turn"


def test_doc_lineage_to_warc(pipeline):
    cfg = pipeline[0]
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    doc = max(read_table(resolve(cfg, cfg["data_root"]) / "doc_scored"),
              key=lambda d: d["quality_score"])
    assert any(k == "warc" and ".warc.gz+" in v for k, v in reg.trace("doc", doc["doc_id"]))
