"""验证「切分布式只改配置」: 同一份模块代码按 cfg 选后端。

用假后端 (monkeypatch) 替掉真实 Postgres/vLLM, 证明 cluster 配置
路由到分布式实现, 无需真实集群/GPU。
"""
import sys
import types

import numpy as np
import pyarrow as pa
import pytest

from dataloop.common.config import load_profile
from dataloop.common.lake import Lake
from dataloop.registry.db import registry_dsn


def test_cluster_profile_loads_and_selects_backends():
    cfg = load_profile("configs/cluster.yaml")   # runner=ray 缺失时被静默忽略
    assert cfg["storage"] == "lance"             # 本地/分布式统一 Lance, 仅 data_root 不同
    assert Lake(cfg).uri("doc_raw") == "s3://data-loop/lake/doc_raw"
    assert cfg["m7_signal_ingest"]["source"] == "kafka"
    assert cfg["m8_pref"]["judge"] == "vllm" and cfg["m8_pref"]["generator"] == "vllm"


def test_registry_dsn_dispatch(tmp_path):
    mini = {"registry_db": "data/registry.sqlite", "_root": str(tmp_path)}
    assert registry_dsn(mini).endswith("registry.sqlite") and "://" not in registry_dsn(mini)
    cluster = {"registry_db": "postgresql://registry:5432/data_loop"}
    assert registry_dsn(cluster).startswith("postgresql://")


def test_storage_options_threaded_to_lance(monkeypatch):
    """data_root 为对象存储 URI 时, Lake 把 storage_options 透传给 Lance。"""
    captured = {}

    def fake_write(rows, schema, uri, storage_options=None):
        captured["uri"], captured["opts"] = uri, storage_options
    monkeypatch.setattr("dataloop.common.io.write_table", fake_write)

    cfg = {"data_root": "s3://bucket/lake", "storage_options": {"region": "cn-hangzhou"}}
    schema = pa.schema([("a", pa.int64())])
    Lake(cfg).write("doc_raw", [{"a": 1}], schema)
    assert captured["uri"] == "s3://bucket/lake/doc_raw"
    assert captured["opts"] == {"region": "cn-hangzhou"}


def test_lance_local_roundtrip(tmp_path):
    """同一 Lance 代码路径用于本地目录。"""
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    cfg = {"data_root": str(tmp_path), "_root": str(tmp_path)}
    Lake(cfg).write("t", rows, schema)
    assert Lake(cfg).read("t") == rows
    assert Lake(cfg).read("missing") == []


def test_m8_generator_dispatch(monkeypatch):
    """generator=vllm 时 make_llm 构造 vLLM 客户端 (用假 openai)。"""
    captured = {}

    class FakeOpenAI:
        def __init__(self, base_url=None, api_key=None): captured["base_url"] = base_url
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))

    from dataloop.online.m8_pref.generate import MockLLM, VLLMClient, make_llm
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

    from dataloop.offline.m4_quality.embedding import embed_texts
    cfg = {"m4_quality": {"embedding": "bge-zh", "embedding_endpoint": "http://bge/v1", "embedding_dim": 2}}
    out = embed_texts(cfg, ["ab", "abcd"])
    assert out.shape == (2, 2) and np.allclose(np.linalg.norm(out, axis=1), 1.0)
