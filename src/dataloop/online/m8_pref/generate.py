"""M8.generate: 候选来源混合 (current_model 多温度 / strong_model / user_edit / constitutional)。
generator=mock 用确定性 mock 适配层; generator=vllm 走 OpenAI 兼容服务,
两者同接口 generate(prompts, temperature) → list[str] (README §10 推理适配)。"""
import hashlib


class MockLLM:
    """与 vLLM 对齐的接口: generate(prompts, temperature) → list[str]。"""

    def __init__(self, name: str):
        self.name = name

    def generate(self, prompts: list[str], temperature: float = 0.7) -> list[str]:
        return [f"[{self.name}|t={temperature}] 针对「{p[:20]}」的回答 "
                f"{hashlib.md5(f'{self.name}{p}{temperature}'.encode()).hexdigest()[:8]}"
                for p in prompts]


class VLLMClient:
    """vLLM (OpenAI 兼容) 客户端; role→model 映射与 endpoint 来自 configs。"""

    def __init__(self, cfg: dict, role: str):
        m = cfg["m8_pref"]
        self.model = (m.get("models") or {}).get(role, m.get("model", role))
        self.max_tokens = m.get("max_tokens", 1024)
        from openai import OpenAI
        self.client = OpenAI(base_url=m.get("generator_endpoint"), api_key=m.get("api_key", "EMPTY"))

    def generate(self, prompts: list[str], temperature: float = 0.7) -> list[str]:
        out = []
        for p in prompts:
            resp = self.client.chat.completions.create(
                model=self.model, temperature=temperature, max_tokens=self.max_tokens,
                messages=[{"role": "user", "content": p}])
            out.append(resp.choices[0].message.content)
        return out


def make_llm(cfg: dict, role: str):
    if cfg["m8_pref"].get("generator", "mock") == "vllm":
        return VLLMClient(cfg, role)
    return MockLLM(role)


def candidates(sel: dict, current, strong) -> list[dict]:
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
