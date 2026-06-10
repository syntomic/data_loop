import pytest

from registry.db import Registry
from registry.decontam import is_contaminated


def test_register_requires_decontam(tmp_path):
    reg = Registry(tmp_path / "r.sqlite")
    with pytest.raises(PermissionError):
        reg.register("v1", "corpus_manifest", {})
    reg.log_decontam("v1", "_checked", "pass")
    reg.register("v1", "corpus_manifest", {"docs": 1})


def test_lineage_trace(tmp_path):
    reg = Registry(tmp_path / "r.sqlite")
    reg.add_lineage("pair", "p1", "turn", "c1+t0")
    reg.add_lineage("pair", "p1", "turn", "c1+t0")  # 幂等
    assert reg.trace("pair", "p1") == [("turn", "c1+t0")]


def test_decontam_13gram():
    item = "下列哪一项是导致温室效应的主要气体二氧化碳的来源"
    assert is_contaminated("课文提到" + item + "等问题", [item])
    assert not is_contaminated("完全无关的中文文本" * 10, [item])
