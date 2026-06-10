"""M8.generate: 候选来源混合 (current_model 多温度 / strong_model / user_edit / constitutional)。
mini 用与 vLLM generate 接口对齐的 mock 适配层 (README §10 本地推理瓶颈对策)。"""
import hashlib


class MockLLM:
    """与 vLLM 对齐的接口: generate(prompts, temperature) → list[str]。"""

    def __init__(self, name: str):
        self.name = name

    def generate(self, prompts: list[str], temperature: float = 0.7) -> list[str]:
        return [f"[{self.name}|t={temperature}] 针对「{p[:20]}」的回答 "
                f"{hashlib.md5(f'{self.name}{p}{temperature}'.encode()).hexdigest()[:8]}"
                for p in prompts]


def candidates(sel: dict, current: MockLLM, strong: MockLLM) -> list[dict]:
    p = sel["prompt"]
    cands = [{"turn_id": sel["turn_id"], "prompt": p, "source": "current_model",
              "response": current.generate([p], t)[0]} for t in (0.3, 1.0)]
    cands.append({"turn_id": sel["turn_id"], "prompt": p, "source": "strong_model",
                  "response": strong.generate([p])[0]})
    if sel["_turn"]["user_edit"]:
        cands.append({"turn_id": sel["turn_id"], "prompt": p, "source": "user_edit",
                      "response": sel["_turn"]["user_edit"]})
    cands.append({"turn_id": sel["turn_id"], "prompt": p, "source": "constitutional",
                  "response": strong.generate([f"按宪法原则改写: {p}"])[0]})
    return cands
