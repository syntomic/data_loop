"""Lance 版本 ↔ registry 快照绑定 (docs/design_lance_registry.md)。"""
import lance
import pyarrow as pa
import pytest

from common.lake import Lake, Snapshot, sanitize_tag
from registry.db import Registry

SCHEMA = pa.schema([("a", pa.int64()), ("b", pa.string())])


def _lake(tmp_path):
    return Lake({"data_root": str(tmp_path), "_root": str(tmp_path)})


def test_write_returns_incrementing_version(tmp_path):
    lake = _lake(tmp_path)
    v1 = lake.write("t", [{"a": 1, "b": "x"}], SCHEMA)
    v2 = lake.write("t", [{"a": 2, "b": "y"}], SCHEMA)
    assert (v1, v2) == (1, 2)
    assert lake.latest_version("t") == 2
    assert lake.latest_version("missing") is None


def test_time_travel_read(tmp_path):
    lake = _lake(tmp_path)
    lake.write("t", [{"a": 1, "b": "x"}], SCHEMA)
    lake.write("t", [{"a": 2, "b": "y"}], SCHEMA)
    assert lake.read("t", version=1) == [{"a": 1, "b": "x"}]
    assert lake.read("t") == [{"a": 2, "b": "y"}]


def test_register_binds_and_lists_snapshots(tmp_path):
    lake = _lake(tmp_path)
    v = lake.write("rlhf_example", [{"a": 1, "b": "x"}], SCHEMA)
    reg = Registry(tmp_path / "r.sqlite")
    reg.log_decontam("pref-v1", "_checked", "pass")
    reg.register("pref-v1", "rlhf_example", {"pairs": 1},
                 snapshots=[lake.snapshot("rlhf_example", v, "output")])
    snaps = reg.snapshots("pref-v1")
    assert snaps == [{"role": "output", "table_name": "rlhf_example",
                      "table_uri": lake.uri("rlhf_example"), "lance_version": v}]


def test_reproducible_after_upstream_rewrite(tmp_path):
    """G1: 上游表被重写产生新 version 后, registry 记录的旧 version 仍能还原注册当时数据。"""
    lake = _lake(tmp_path)
    v_old = lake.write("doc_scored", [{"a": 1, "b": "old"}], SCHEMA)
    reg = Registry(tmp_path / "r.sqlite")
    reg.log_decontam("corpus-stable-v1", "_checked", "pass")
    reg.register("corpus-stable-v1", "corpus_manifest", {},
                 snapshots=[lake.snapshot("doc_scored", v_old, "input")])
    # 重跑上游, 数据变了
    lake.write("doc_scored", [{"a": 2, "b": "new"}], SCHEMA)
    assert lake.read("doc_scored") == [{"a": 2, "b": "new"}]
    # 但按 registry 记录的快照仍能还原
    snap = reg.snapshots("corpus-stable-v1")[0]
    assert lake.read(snap["table_name"], version=snap["lance_version"]) == [{"a": 1, "b": "old"}]


def test_ablation_binds_both_arms(tmp_path):
    lake = _lake(tmp_path)
    va = lake.write("doc_scored_a", [{"a": 1, "b": "x"}], SCHEMA)
    vb = lake.write("doc_scored_b", [{"a": 2, "b": "y"}], SCHEMA)
    reg = Registry(tmp_path / "r.sqlite")
    reg.save_ablation("dec-1", "diff", 0.5, 0.6, False, "tie", "doc_scored_a", "doc_scored_b",
                      snap_a=lake.snapshot("doc_scored_a", va, "arm_a"),
                      snap_b=lake.snapshot("doc_scored_b", vb, "arm_b"))
    roles = {s["role"]: s["lance_version"] for s in reg.snapshots("dec-1")}
    assert roles == {"arm_a": va, "arm_b": vb}


def test_lance_tag_mirror(tmp_path):
    lake = _lake(tmp_path)
    v = lake.write("rlhf_example", [{"a": 1, "b": "x"}], SCHEMA)
    lake.tag("rlhf_example", "pref-v1", v)
    assert "pref-v1" in lance.dataset(lake.uri("rlhf_example")).tags.list()


def test_sanitize_tag():
    assert sanitize_tag("corpus/stable:v1") == "corpus-stable-v1"


def test_legacy_version_without_snapshot(tmp_path):
    reg = Registry(tmp_path / "r.sqlite")
    reg.log_decontam("old", "_checked", "pass")
    reg.register("old", "corpus_manifest", {})  # 无 snapshots
    assert reg.snapshots("old") == []
