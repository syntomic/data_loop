"""教师打标适配层 (rubric: configs/rubric_zh.md)。
cluster 走外部强模型 API; mini 用与 rubric 对齐的启发式教师。"""
import re
from functools import lru_cache
from pathlib import Path

_EDU = ["教材", "公式", "定理", "推导", "例如", "首先", "其次", "原理", "定义", "性质", "证明", "教程", "因此"]
_SPAM = ["点击", "优惠", "促销", "广告", "免费领取", "加微信"]


def _heuristic(text: str) -> float:
    s = 1.5
    s += min(2.0, sum(text.count(k) for k in _EDU) * 0.35)
    s += min(1.0, len(re.findall(r"[一二三四五六七八九1-9][、.)]", text)) * 0.25)
    s += min(0.8, len(text) / 3000)
    s -= min(2.0, sum(text.count(k) for k in _SPAM) * 0.6)
    return max(0.0, min(5.0, s))


@lru_cache(maxsize=1)
def _rubric(root: str) -> str:
    return (Path(root) / "configs" / "rubric_zh.md").read_text()


def _api(text: str, cfg: dict) -> float:
    """中文强模型按 rubric 打 0–5 educational value 分, OpenAI 兼容 chat API。"""
    m = cfg["m4_quality"]
    from openai import OpenAI
    client = OpenAI(base_url=m.get("teacher_endpoint"), api_key=m.get("api_key", "EMPTY"))
    rubric = _rubric(cfg.get("_root", "."))
    resp = client.chat.completions.create(
        model=m.get("teacher_model", "strong-zh"), temperature=0,
        messages=[{"role": "system", "content": rubric + "\n只输出一个 0-5 的小数。"},
                  {"role": "user", "content": text[:m.get("max_chars", 4000)]}])
    try:
        return max(0.0, min(5.0, float(resp.choices[0].message.content.strip().split()[0])))
    except (ValueError, IndexError):
        return 0.0


def score(text: str, backend: str = "heuristic", cfg: dict | None = None) -> float:
    if backend == "api":
        return _api(text, cfg or {})
    return _heuristic(text)
