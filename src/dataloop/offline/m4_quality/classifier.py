"""学生分类器: embedding + 线性回归头, 输出连续分 [0,5]。
cluster 用 BGE-zh; mini 用 char-ngram hashing embedding, 头部用 ridge 拟合教师分。"""
import hashlib

import numpy as np


def embed(text: str, dim: int) -> np.ndarray:
    v = np.zeros(dim, dtype=np.float32)
    for i in range(len(text) - 2):
        h = int.from_bytes(hashlib.md5(text[i:i + 3].encode()).digest()[:4], "big")
        v[h % dim] += 1.0
    n = np.linalg.norm(v)
    return v / n if n else v


class Regressor:
    def __init__(self, dim: int):
        self.dim, self.w, self.b = dim, np.zeros(dim, dtype=np.float32), 0.0

    def fit(self, X: np.ndarray, y: np.ndarray, l2: float = 0.05):
        Xb = np.hstack([X, np.ones((len(X), 1), dtype=np.float32)])
        wb = np.linalg.solve(Xb.T @ Xb + l2 * np.eye(self.dim + 1), Xb.T @ y)
        self.w, self.b = wb[:-1].astype(np.float32), float(wb[-1])

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.clip(X @ self.w + self.b, 0.0, 5.0)
