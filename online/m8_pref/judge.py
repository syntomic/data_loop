"""M8.judge: rubric 4 维 (准确/有用/安全/简洁)。conf ≥ 0.8 自动通过, 否则路由人工队列。
mini 用确定性 mock; cluster 用 vLLM judge。"""
import hashlib

_SOURCE_PRIOR = {"strong_model": 0.85, "user_edit": 0.8, "constitutional": 0.7, "current_model": 0.5}


def judge_pair(prompt: str, a: dict, b: dict) -> tuple[dict, dict, float]:
    def s(c):
        h = int(hashlib.md5((prompt + c["response"]).encode()).hexdigest()[:6], 16) / 0xFFFFFF
        return _SOURCE_PRIOR[c["source"]] * 0.7 + h * 0.3  # 4 维 rubric 折合单分

    sa, sb = s(a), s(b)
    chosen, rejected = (a, b) if sa >= sb else (b, a)
    conf = min(0.99, 0.5 + abs(sa - sb) * 2)
    return chosen, rejected, conf
