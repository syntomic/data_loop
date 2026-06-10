"""M4 quality: 两段式 (教师抽样打标 → 学生分类器全量推理), README §3.4。"""
import random
from pathlib import Path

import numpy as np

from common.config import resolve
from common.io import read_table, write_table
from schemas.tables import DOC_SCORED
from . import teacher
from .classifier import Regressor, embed


def run(cfg: dict, in_name: str = "doc_dedup", out_name: str = "doc_scored") -> Path:
    m = cfg["m4_quality"]
    docs = read_table(resolve(cfg, cfg["data_root"]) / in_name)
    dim = m["embedding_dim"]

    sample = random.Random(0).sample(docs, min(m["teacher_sample"], len(docs)))
    X = np.stack([embed(d["text"], dim) for d in sample])
    y = np.array([teacher.score(d["text"], m["teacher"]) for d in sample], dtype=np.float32)
    reg = Regressor(dim)
    reg.fit(X, y)

    preds = reg.predict(np.stack([embed(d["text"], dim) for d in docs])) if docs else []
    rows = [d | {"quality_score": float(p), "classifier_version": m["classifier_version"]}
            for d, p in zip(docs, preds)]
    out = resolve(cfg, cfg["data_root"]) / out_name
    write_table(rows, DOC_SCORED, out, partition="dump_id")
    return out
