"""PII 清洗 UDF (可插拔): 正则基线; cluster 可叠加 NER。"""
import re

_PATTERNS = [
    (re.compile(r"(?<!\d)\d{17}[\dXx](?![\dXx])"), "<ID>"),
    (re.compile(r"(?<!\d)\d{16,19}(?!\d)"), "<CARD>"),
    (re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)"), "<PHONE>"),
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9.]+"), "<EMAIL>"),
]


def scrub(text: str | None) -> str | None:
    if text is None:
        return None
    for pat, repl in _PATTERNS:
        text = pat.sub(repl, text)
    return text
