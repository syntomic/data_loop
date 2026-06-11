"""M8.qc: 去重、去污染、领域(簇)平衡。"""
from dataloop.registry.decontam import is_contaminated


def run_qc(pairs: list[dict], eval_items: list[str], reg, version_id: str, quota: int) -> list[dict]:
    seen, per_cluster, out = set(), {}, []
    for p in pairs:
        key = (p["prompt"], p["chosen"])
        if key in seen:
            continue
        seen.add(key)
        if is_contaminated(p["prompt"] + p["chosen"], eval_items):
            reg.log_decontam(version_id, p["pair_id"], "13gram_or_minhash")
            continue
        c = p.pop("_cluster")
        if per_cluster.get(c, 0) >= quota:
            continue
        per_cluster[c] = per_cluster.get(c, 0) + 1
        out.append(p)
    return out
