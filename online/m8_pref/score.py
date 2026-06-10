"""M8.score: reward + safety + embedding, 然后 cluster + novelty。
cluster profile 用真实 reward model / BGE; mini 用信号驱动启发式 + hashing embedding。"""
import numpy as np

from offline.m4_quality.classifier import embed


def score_turn(t: dict, dim: int = 64) -> dict:
    reward = 0.5 + 0.4 * t["thumbs"] - (0.3 if t["regenerated"] else 0) \
        - (0.2 if t["stopped"] else 0) - (0.2 if t["followup_correction"] else 0) \
        + (0.1 if t["user_edit"] else 0)
    safety = 0.2 if "暴力" in (t["response"] or "") else 0.95
    return t | {"reward": float(np.clip(reward, 0, 1)), "safety": safety,
                "embedding": embed(t["prompt"], dim).tolist()}


def cluster_and_novelty(turns: list[dict], threshold: float = 0.7) -> list[dict]:
    centroids: list[np.ndarray] = []
    out = []
    for t in turns:
        v = np.array(t["embedding"], dtype=np.float32)
        sims = [float(v @ c) for c in centroids]
        best = int(np.argmax(sims)) if sims else -1
        if best >= 0 and sims[best] >= threshold:
            t = t | {"cluster": best, "novelty": 1 - sims[best]}
        else:
            centroids.append(v)
            t = t | {"cluster": len(centroids) - 1, "novelty": 1.0}
        out.append(t)
    return out
