"""M8 pref-build 固定流水 (README §5.2):
score → cluster+novelty → active_sampling → generate → judge → qc
→ PreferencePair → RLHFExample 版本注册 (血缘到 conversation_id+turn_id)。"""
import hashlib

from common.lake import Lake
from registry.db import Registry
from registry.decontam import load_eval_items
from schemas.tables import PREFERENCE_PAIR, RLHF_EXAMPLE
from .generate import candidates, make_llm
from .judge import judge_pair
from .qc import run_qc
from .sample import select
from .score import cluster_and_novelty, score_turn


def run(cfg: dict, version_id: str = "pref-v1") -> dict:
    m = cfg["m8_pref"]
    lake = Lake(cfg)
    turns = lake.read("turn_candidate")
    scored = cluster_and_novelty([score_turn(t) for t in turns])
    selected = select(scored, m["cluster_quota"])

    current, strong = make_llm(cfg, "current_model"), make_llm(cfg, "strong_model")
    pairs = []
    for sel in selected:
        cands = candidates(sel, current, strong)
        best, worst, conf = cands[0], cands[1], 0.0
        for c in cands[1:]:
            chosen, rejected, conf = judge_pair(sel["prompt"], best, c, cfg)
            best, worst = chosen, rejected
        pairs.append({
            "pair_id": hashlib.sha1(f"{sel['turn_id']}{best['response']}".encode()).hexdigest()[:16],
            "conversation_id": sel["conversation_id"], "turn_id": sel["turn_id"],
            "prompt": sel["prompt"], "chosen": best["response"], "rejected": worst["response"],
            "chosen_source": best["source"], "rejected_source": worst["source"],
            "judge_conf": conf,
            "judge_route": "auto" if conf >= m["judge_conf_auto"] else "human",
            "_cluster": sel["cluster"],
        })

    reg = Registry.from_cfg(cfg)
    auto = [p for p in pairs if p["judge_route"] == "auto"]
    human = [dict(p) for p in pairs if p["judge_route"] == "human"]
    for p in human:
        p.pop("_cluster")
    final = run_qc(auto, load_eval_items(cfg), reg, version_id, m["cluster_quota"])
    reg.log_decontam(version_id, "_checked", "decontam_pass")

    lake.write("preference_pair", final, PREFERENCE_PAIR)
    lake.write("human_queue", human, PREFERENCE_PAIR)
    rlhf = [p | {"dataset_version": version_id} for p in final]
    lake.write("rlhf_example", rlhf, RLHF_EXAMPLE)
    reg.register(version_id, "rlhf_example", {"pairs": len(final), "human_queue": len(human)})
    for p in final:
        reg.add_lineage("rlhf_example", version_id, "pair", p["pair_id"])
        reg.add_lineage("pair", p["pair_id"], "turn", f"{p['conversation_id']}+{p['turn_id']}")
    return {"selected": len(selected), "pairs": len(pairs), "auto": len(final), "human": len(human)}
