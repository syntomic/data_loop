"""文档嵌入适配层 (M4 学生分类器输入)。

按 cfg["m4_quality"]["embedding"] 分发:
- hashing (mini): char-ngram 散列, 纯本地确定性, 无外部依赖。
- bge-zh  (cluster): BGE-zh 句向量, 经 OpenAI 兼容 embeddings API 批量取回
  (vLLM/TEI 等服务), endpoint/model/key 全部来自 configs。
两后端都返回 L2 归一化的 np.float32 矩阵 [n, dim]。
"""
import numpy as np

from .classifier import embed as _hashing  # char-ngram 散列 (本地确定性)


def _bge_api(cfg: dict, texts: list[str]) -> np.ndarray:
    m = cfg["m4_quality"]
    from openai import OpenAI
    client = OpenAI(base_url=m.get("embedding_endpoint"), api_key=m.get("api_key", "EMPTY"))
    out = []
    batch = m.get("embedding_batch", 64)
    for i in range(0, len(texts), batch):
        resp = client.embeddings.create(model=m.get("embedding_model", "bge-zh"),
                                        input=[t[:m.get("max_chars", 4000)] for t in texts[i:i + batch]])
        out.extend(d.embedding for d in resp.data)
    arr = np.asarray(out, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    return arr / np.where(norms == 0, 1, norms)


def embed_texts(cfg: dict, texts: list[str]) -> np.ndarray:
    m = cfg["m4_quality"]
    backend = m.get("embedding", "hashing")
    if backend in ("bge-zh", "api"):
        return _bge_api(cfg, texts)
    dim = m["embedding_dim"]
    if not texts:
        return np.zeros((0, dim), dtype=np.float32)
    return np.stack([_hashing(t, dim) for t in texts])
