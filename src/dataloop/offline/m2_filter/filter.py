"""M2 filter: doc_raw → doc_filtered。被滤文档保留 flags 不删行 (消融需要)。"""
from pathlib import Path
from urllib.parse import urlparse

import yaml

from dataloop.common.config import resolve
from dataloop.common.lake import Lake
from dataloop.schemas.tables import DOC_FILTERED
from .langid import detect
from .rules import evaluate


def run(cfg: dict) -> str:
    m = cfg["m2_filter"]
    lake = Lake(cfg)
    rules = yaml.safe_load(resolve(cfg, m["rules_file"]).read_text())
    blacklist = {l.strip() for l in resolve(cfg, m["blacklist_file"]).read_text().splitlines()
                 if l.strip() and not l.startswith("#")}
    stopwords = {l.strip() for l in resolve(cfg, m["stopwords_file"]).read_text().splitlines() if l.strip()}

    rows = []
    for doc in lake.read("doc_raw"):
        lang, conf = detect(doc["text"], m["langid"])
        flags = {"lang_ok": lang == "zh" and conf >= m["zh_conf_min"],
                 "domain_ok": urlparse(doc["url"]).hostname not in blacklist}
        flags.update(evaluate(doc["text"], rules, stopwords))
        rows.append(doc | {"lang": lang, "lang_conf": conf,
                           "filter_flags": flags, "kept": all(flags.values())})
    lake.write("doc_filtered", rows, DOC_FILTERED, partition="dump_id")
    return "doc_filtered"
