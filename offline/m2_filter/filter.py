"""M2 filter: doc_raw → doc_filtered。被滤文档保留 flags 不删行 (消融需要)。"""
from pathlib import Path
from urllib.parse import urlparse

import yaml

from common.config import resolve
from common.io import read_table, write_table
from schemas.tables import DOC_FILTERED
from .langid import detect
from .rules import evaluate


def run(cfg: dict) -> Path:
    m = cfg["m2_filter"]
    rules = yaml.safe_load(resolve(cfg, m["rules_file"]).read_text())
    blacklist = {l.strip() for l in resolve(cfg, m["blacklist_file"]).read_text().splitlines()
                 if l.strip() and not l.startswith("#")}
    stopwords = {l.strip() for l in resolve(cfg, m["stopwords_file"]).read_text().splitlines() if l.strip()}

    rows = []
    for doc in read_table(resolve(cfg, cfg["data_root"]) / "doc_raw"):
        lang, conf = detect(doc["text"], m["langid"])
        flags = {"lang_ok": lang == "zh" and conf >= m["zh_conf_min"],
                 "domain_ok": urlparse(doc["url"]).hostname not in blacklist}
        flags.update(evaluate(doc["text"], rules, stopwords))
        rows.append(doc | {"lang": lang, "lang_conf": conf,
                           "filter_flags": flags, "kept": all(flags.values())})
    out = resolve(cfg, cfg["data_root"]) / "doc_filtered"
    write_table(rows, DOC_FILTERED, out, partition="dump_id")
    return out
