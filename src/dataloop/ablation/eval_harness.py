"""消融评估 harness。
cluster: 0.2B 模型 5B token 真训 + CMMLU/C-Eval/CLUE 子集。
mini: 不真训, 用语料统计代理分 (多样性 / 平均质量 / token 量), 接口与真评估对齐。"""
import math
import random


def proxy_eval(docs: list[dict], seed: int = 42) -> float:
    if not docs:
        return 0.0
    rnd = random.Random(seed)
    mean_q = sum(d["quality_score"] for d in docs) / len(docs)
    diversity = len({d["doc_id"] for d in docs}) / len(docs)
    volume = math.log1p(sum(len(d["text"]) for d in docs)) / 15
    return mean_q / 5 * 0.5 + diversity * 0.3 + volume * 0.2 + rnd.gauss(0, 0.005)


def significant(a: float, b: float, noise: float = 0.01) -> bool:
    return abs(a - b) > 2 * noise
