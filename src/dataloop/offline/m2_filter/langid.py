"""langid 适配层: cluster 用 fastText lid.176, mini 用 CJK 占比启发式。"""


def is_zh_char(c: str) -> bool:
    return "一" <= c <= "鿿"


def detect(text: str, backend: str = "heuristic") -> tuple[str, float]:
    if backend == "fasttext":
        import fasttext
        model = fasttext.load_model("lid.176.bin")
        labels, confs = model.predict(text.replace("\n", " "))
        return labels[0].replace("__label__", ""), float(confs[0])
    visible = [c for c in text if not c.isspace()]
    if not visible:
        return "und", 0.0
    ratio = sum(is_zh_char(c) for c in visible) / len(visible)
    return ("zh", min(1.0, ratio + 0.3)) if ratio > 0.2 else ("other", 1.0 - ratio)
