"""M5 corpus-build: 双阈值切 stable/anneal, tokenize 落 shard,
写 corpus_manifest 进 registry 并登记文档级血缘 (README §3.5, §6)。"""
import json
from collections import Counter
from pathlib import Path

from common.config import resolve
from common.io import read_table
from registry.db import Registry
from registry.decontam import is_contaminated, load_eval_items


def _tokenize(text: str) -> list[str]:
    return list("".join(text.split()))  # mini: 字符级


def run(cfg: dict, in_name: str = "doc_scored", manifest_suffix: str = "v1") -> dict:
    m = cfg["m5_corpus"]
    docs = read_table(resolve(cfg, cfg["data_root"]) / in_name)
    eval_items = load_eval_items(cfg)
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    out_root = resolve(cfg, cfg["data_root"]) / "corpus"
    manifests = {}

    for layer, threshold in [("stable", m["stable_threshold"]), ("anneal", m["anneal_threshold"])]:
        mid = f"corpus-{layer}-{manifest_suffix}"
        kept, tokens, dumps = [], 0, Counter()
        for d in docs:
            if d["quality_score"] < threshold:
                continue
            if is_contaminated(d["text"], eval_items, cfg["decontam"]["ngram"]):
                reg.log_decontam(mid, d["doc_id"], "13gram_or_minhash")
                continue
            kept.append(d)
            tokens += len(_tokenize(d["text"]))
            dumps[d["dump_id"]] += 1
        reg.log_decontam(mid, "_checked", "decontam_pass")
        shard_dir = out_root / layer
        shard_dir.mkdir(parents=True, exist_ok=True)
        (shard_dir / "shard-0.txt").write_text("\n".join("".join(_tokenize(d["text"])) for d in kept))
        manifest = {"manifest_id": mid, "layer": layer, "doc_count": len(kept), "token_count": tokens,
                    "dump_distribution": dict(dumps), "classifier_version": cfg["m4_quality"]["classifier_version"],
                    "rules_version": "filter_rules-v1"}
        reg.register(mid, "corpus_manifest", manifest)
        for d in kept:
            reg.add_lineage("corpus_manifest", mid, "doc", d["doc_id"])
            reg.add_lineage("doc", d["doc_id"], "warc", f"{d['warc_file']}+{d['warc_offset']}")
        manifests[layer] = manifest
    (out_root / "manifest.json").write_text(json.dumps(manifests, ensure_ascii=False, indent=2))
    return manifests
