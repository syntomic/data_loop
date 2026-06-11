"""M4 quality: 两段式 (教师抽样打标 → 学生分类器全量推理), README §3.4。"""
import random

import numpy as np

from dataloop.common.lake import Lake
from dataloop.schemas.tables import DOC_SCORED
from . import teacher
from .classifier import Regressor
from .embedding import embed_texts


def run(cfg: dict, in_name: str = "doc_dedup", out_name: str = "doc_scored") -> str:
    m = cfg["m4_quality"]
    lake = Lake(cfg)
    docs = lake.read(in_name)
    dim = m["embedding_dim"]

    sample = random.Random(0).sample(docs, min(m["teacher_sample"], len(docs)))
    X = embed_texts(cfg, [d["text"] for d in sample])
    y = np.array([teacher.score(d["text"], m["teacher"], cfg) for d in sample], dtype=np.float32)
    reg = Regressor(X.shape[1] if len(X) else dim)
    reg.fit(X, y)

    preds = reg.predict(embed_texts(cfg, [d["text"] for d in docs])) if docs else []
    rows = [d | {"quality_score": float(p), "classifier_version": m["classifier_version"]}
            for d, p in zip(docs, preds)]
    lake.write(out_name, rows, DOC_SCORED, partition="dump_id")
    return out_name
