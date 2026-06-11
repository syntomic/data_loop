"""M3 dedup: per-dump MinHash LSH (默认); global 留作消融项。
簇内保留质量启发分最高的一篇, 其余记 dup_of。"""
from dataloop.common.lake import Lake
from dataloop.schemas.tables import DOC_DEDUP
from .minhash import _params, shingles, signature, lsh_bands


def _heuristic_quality(text: str) -> float:
    return len(set(text)) / max(1, len(text)) * min(len(text), 5000)


def _cluster(docs: list[dict], cfg_m: dict) -> dict[str, str]:
    params = _params(cfg_m["num_perm"])
    parent = {d["doc_id"]: d["doc_id"] for d in docs}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    buckets: dict[tuple, str] = {}
    for d in docs:
        sig = signature(shingles(d["text"], cfg_m["ngram"]), params)
        for key in lsh_bands(sig, cfg_m["bands"], cfg_m["rows"]):
            if key in buckets:
                parent[find(d["doc_id"])] = find(buckets[key])
            else:
                buckets[key] = d["doc_id"]
    return {d["doc_id"]: find(d["doc_id"]) for d in docs}


def run(cfg: dict, scope: str | None = None, out_name: str = "doc_dedup") -> str:
    m = cfg["m3_dedup"]
    lake = Lake(cfg)
    scope = scope or m["scope"]
    docs = [d for d in lake.read("doc_filtered") if d["kept"]]

    groups = {}
    for d in docs:
        groups.setdefault(d["dump_id"] if scope == "per_dump" else "_global", []).append(d)

    rows = []
    for group in groups.values():
        roots = _cluster(group, m)
        clusters: dict[str, list[dict]] = {}
        for d in group:
            clusters.setdefault(roots[d["doc_id"]], []).append(d)
        for cid, members in clusters.items():
            best = max(members, key=lambda d: _heuristic_quality(d["text"]))
            for d in members:
                if d["doc_id"] == best["doc_id"]:
                    rows.append(d | {"cluster_id": cid, "cluster_size": len(members), "dup_of": None})
    lake.write(out_name, rows, DOC_DEDUP, partition="dump_id")
    return out_name
