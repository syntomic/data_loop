"""去污染: eval 套件 13-gram 精确匹配 + MinHash 近重 (README §6)。"""
from dataloop.common.config import resolve
from dataloop.offline.m3_dedup.minhash import _params, shingles, signature

_PARAMS = _params(112)


def load_eval_items(cfg: dict) -> list[str]:
    lines = resolve(cfg, cfg["decontam"]["eval_file"]).read_text().splitlines()
    return [l.strip() for l in lines if l.strip() and not l.startswith("#")]


def _ngrams(text: str, n: int) -> set[str]:
    t = "".join(text.split())
    return {t[i:i + n] for i in range(max(0, len(t) - n + 1))}


def is_contaminated(text: str, eval_items: list[str], ngram: int = 13, jaccard: float = 0.8) -> bool:
    grams = _ngrams(text, ngram)
    sig = None
    for item in eval_items:
        if _ngrams(item, ngram) & grams:
            return True
        if sig is None:
            sig = signature(shingles(text), _PARAMS)
        isig = signature(shingles(item), _PARAMS)
        if sum(a == b for a, b in zip(sig, isig)) / len(sig) >= jaccard:
            return True
    return False
