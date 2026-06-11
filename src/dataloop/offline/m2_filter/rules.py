"""M2 启发式规则集 (Gopher/C4 中文标定, README §3.2)。每条返回独立 flag。"""
import re
from collections import Counter

from .langid import is_zh_char

_SYMBOL = re.compile(r"[#$%^&*=+|\\<>~`@_{}\[\]]")


def evaluate(text: str, rules: dict, stopwords: set[str]) -> dict[str, bool]:
    n = len(text)
    visible = [c for c in text if not c.isspace()]
    nv = max(1, len(visible))
    lines = [l for l in text.splitlines() if l.strip()]
    grams = [text[i:i + 2] for i in range(len(text) - 1)] or [""]
    gc = Counter(grams)
    flags = {
        "len_ok": rules["min_chars"] <= n <= rules["max_chars"],
        "symbol_ok": len(_SYMBOL.findall(text)) / nv < rules["max_symbol_ratio"],
        "digit_ok": sum(c.isdigit() for c in visible) / nv < rules["max_digit_ratio"],
        "dup_line_ok": (1 - len(set(lines)) / max(1, len(lines))) < rules["max_dup_line_ratio"],
        "dup_2gram_ok": (1 - len(gc) / len(grams)) < rules["max_dup_2gram_ratio"],
        "zh_ratio_ok": sum(is_zh_char(c) for c in visible) / nv >= rules["min_zh_char_ratio"],
        "stopword_ok": len({w for w in stopwords if w in text}) >= rules["min_stopword_kinds"],
    }
    return flags
