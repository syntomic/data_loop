"""教师打标适配层 (rubric: configs/rubric_zh.md)。
cluster 走外部强模型 API; mini 用与 rubric 对齐的启发式教师。"""
import re

_EDU = ["教材", "公式", "定理", "推导", "例如", "首先", "其次", "原理", "定义", "性质", "证明", "教程", "因此"]
_SPAM = ["点击", "优惠", "促销", "广告", "免费领取", "加微信"]


def score(text: str, backend: str = "heuristic") -> float:
    if backend == "api":
        raise NotImplementedError("cluster profile: 外部 API 适配层")
    s = 1.5
    s += min(2.0, sum(text.count(k) for k in _EDU) * 0.35)
    s += min(1.0, len(re.findall(r"[一二三四五六七八九1-9][、.)]", text)) * 0.25)
    s += min(0.8, len(text) / 3000)
    s -= min(2.0, sum(text.count(k) for k in _SPAM) * 0.6)
    return max(0.0, min(5.0, s))
