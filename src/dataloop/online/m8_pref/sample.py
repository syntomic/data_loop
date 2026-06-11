"""M8.active_sampling: 负反馈 | 高分歧 | 低置信 | 新簇, 簇配额封顶。"""


def select(turns: list[dict], quota: int) -> list[dict]:
    out, per_cluster = [], {}
    for t in turns:
        reason = None
        if t["thumbs"] < 0 or t["regenerated"] or t["followup_correction"]:
            reason = "negative"
        elif t["novelty"] >= 0.99:
            reason = "new_cluster"
        elif 0.4 <= t["reward"] <= 0.6:
            reason = "low_conf"
        elif t["user_edit"]:
            reason = "divergence"
        if reason and per_cluster.get(t["cluster"], 0) < quota:
            per_cluster[t["cluster"]] = per_cluster.get(t["cluster"], 0) + 1
            out.append({"conversation_id": t["conversation_id"], "turn_id": t["turn_id"],
                        "prompt": t["prompt"], "reason": reason, "cluster": t["cluster"],
                        "_turn": t})
    return out
