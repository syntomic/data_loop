"""正文抽取适配层: cluster 用 trafilatura(favor_precision)+resiliparse fallback,
mini 用内置正则抽取器, 接口一致, 返回 (text, extractor_name)。"""
import re

_TAG = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_HTML = re.compile(r"<[^>]+>")


def _simple(html: str):
    text = _HTML.sub("\n", _TAG.sub("", html))
    text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    return (text, "simple") if text else (None, "simple")


def _trafilatura(html: str):
    import trafilatura
    text = trafilatura.extract(html, favor_precision=True)
    if text:
        return text, "trafilatura"
    from resiliparse.extract.html2text import extract_plain_text
    text = extract_plain_text(html)
    return (text, "resiliparse") if text else (None, "resiliparse")


def extract(html: str, backend: str = "simple"):
    return _trafilatura(html) if backend == "trafilatura" else _simple(html)
