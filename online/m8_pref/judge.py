"""M8.judge: rubric 4 维 (准确/有用/安全/简洁)。conf ≥ 0.8 自动通过, 否则路由人工队列。
judge=mock 用确定性 mock; judge=vllm 用 vLLM 裁判 (OpenAI 兼容)。"""
import hashlib
import json

_SOURCE_PRIOR = {"strong_model": 0.85, "user_edit": 0.8, "constitutional": 0.7, "current_model": 0.5}

_JUDGE_SYS = ("你是严格的中文偏好裁判。比较两个回答, 从准确/有用/安全/简洁四维打分, "
              "只输出 JSON: {\"winner\": \"A\"|\"B\", \"conf\": 0~1}。")


def _mock(prompt: str, a: dict, b: dict) -> tuple[dict, dict, float]:
    def s(c):
        h = int(hashlib.md5((prompt + c["response"]).encode()).hexdigest()[:6], 16) / 0xFFFFFF
        return _SOURCE_PRIOR[c["source"]] * 0.7 + h * 0.3
    sa, sb = s(a), s(b)
    chosen, rejected = (a, b) if sa >= sb else (b, a)
    return chosen, rejected, min(0.99, 0.5 + abs(sa - sb) * 2)


def _vllm(prompt: str, a: dict, b: dict, cfg: dict) -> tuple[dict, dict, float]:
    m = cfg["m8_pref"]
    from openai import OpenAI
    client = OpenAI(base_url=m.get("judge_endpoint"), api_key=m.get("api_key", "EMPTY"))
    user = f"问题:\n{prompt}\n\n回答A:\n{a['response']}\n\n回答B:\n{b['response']}"
    resp = client.chat.completions.create(
        model=m.get("judge_model", "judge-zh"), temperature=0,
        messages=[{"role": "system", "content": _JUDGE_SYS}, {"role": "user", "content": user}])
    try:
        verdict = json.loads(resp.choices[0].message.content)
        chosen, rejected = (a, b) if verdict["winner"].upper() == "A" else (b, a)
        return chosen, rejected, float(verdict["conf"])
    except (json.JSONDecodeError, KeyError, ValueError):
        return a, b, 0.0  # 解析失败 → 低置信, 路由人工


def judge_pair(prompt: str, a: dict, b: dict, cfg: dict | None = None) -> tuple[dict, dict, float]:
    if cfg and cfg.get("m8_pref", {}).get("judge") == "vllm":
        return _vllm(prompt, a, b, cfg)
    return _mock(prompt, a, b)
