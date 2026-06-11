"""验证「切分布式只改配置」: 同一份模块代码按 cfg 选后端。

用假后端 (monkeypatch) 替掉真实 Paimon/Postgres/vLLM, 证明 cluster 配置
路由到分布式实现, 无需真实集群/GPU。
"""
import sys
import types

import numpy as np
import pyarrow as pa
import pytest

from common.config import load_profile
from common.lake import Lake
from registry.db import registry_dsn


def test_cluster_profile_loads_and_selects_backends():
    cfg = load_profile("configs/cluster.yaml")   # runner=ray 缺失时被静默忽略
    assert cfg["storage"] == "paimon"
    assert Lake(cfg).uri("doc_raw") == "s3://data-loop/lake/doc_raw"
    assert cfg["m7_signal_ingest"]["source"] == "kafka"
    assert cfg["m8_pref"]["judge"] == "vllm" and cfg["m8_pref"]["generator"] == "vllm"


def test_registry_dsn_dispatch(tmp_path):
    mini = {"registry_db": "data/registry.sqlite", "_root": str(tmp_path)}
    assert registry_dsn(mini).endswith("registry.sqlite") and "://" not in registry_dsn(mini)
    cluster = {"registry_db": "postgresql://registry:5432/data_loop"}
    assert registry_dsn(cluster).startswith("postgresql://")


def test_storage_paimon_roundtrip_via_fake(monkeypatch):
    """storage=paimon 时 Lake 走 daft.write_paimon/read_paimon (此处用假后端)。"""
    store = {}

    def fake_paimon_table(cfg, name, schema=None, partition=None, create=False):
        return name

    class FakeDF:
        def __init__(self, table): self.table = table
        def write_paimon(self, table, mode="append"): store[table] = self.table

    fake_daft = types.ModuleType("daft")
    fake_daft.from_arrow = lambda t: FakeDF(t)
    fake_daft.read_paimon = lambda name: types.SimpleNamespace(to_pylist=lambda: store[name].to_pylist())
    monkeypatch.setitem(sys.modules, "daft", fake_daft)
    monkeypatch.setitem(sys.modules, "common.paimon", types.SimpleNamespace(paimon_table=fake_paimon_table))

    cfg = {"storage": "paimon", "data_root": "s3://bucket/lake", "_root": "."}
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    Lake(cfg).write("t", rows, schema)
    assert Lake(cfg).read("t") == rows


def test_m8_generator_dispatch(monkeypatch):
    """generator=vllm 时 make_llm 构造 vLLM 客户端 (用假 openai)。"""
    captured = {}

    class FakeOpenAI:
        def __init__(self, base_url=None, api_key=None): captured["base_url"] = base_url
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))

    from online.m8_pref.generate import MockLLM, VLLMClient, make_llm
    assert isinstance(make_llm({"m8_pref": {"generator": "mock"}}, "x"), MockLLM)
    cfg = {"m8_pref": {"generator": "vllm", "generator_endpoint": "http://vllm/v1", "models": {"current_model": "c"}}}
    llm = make_llm(cfg, "current_model")
    assert isinstance(llm, VLLMClient) and captured["base_url"] == "http://vllm/v1"


def test_m4_embedding_dispatch(monkeypatch):
    """embedding=bge-zh 时走 API embeddings (用假 openai)。"""
    class FakeEmb:
        def create(self, model, input):
            data = [types.SimpleNamespace(embedding=[float(len(t)), 1.0]) for t in input]
            return types.SimpleNamespace(data=data)

    class FakeOpenAI:
        def __init__(self, base_url=None, api_key=None): self.embeddings = FakeEmb()
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))

    from offline.m4_quality.embedding import embed_texts
    cfg = {"m4_quality": {"embedding": "bge-zh", "embedding_endpoint": "http://bge/v1", "embedding_dim": 2}}
    out = embed_texts(cfg, ["ab", "abcd"])
    assert out.shape == (2, 2) and np.allclose(np.linalg.norm(out, axis=1), 1.0)
