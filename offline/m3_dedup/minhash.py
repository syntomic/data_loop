"""MinHash LSH (字粒度 n-gram), num_perm=112, b=14, r=8 ≈ 0.75 Jaccard 阈值。"""
import hashlib
import struct

_PRIME = (1 << 61) - 1


def _params(num_perm: int, seed: int = 1):
    import random
    rnd = random.Random(seed)
    return [(rnd.randrange(1, _PRIME), rnd.randrange(0, _PRIME)) for _ in range(num_perm)]


def shingles(text: str, n: int = 5) -> set[int]:
    t = "".join(text.split())
    return {int.from_bytes(hashlib.md5(t[i:i + n].encode()).digest()[:8], "big")
            for i in range(max(1, len(t) - n + 1))}


def signature(sh: set[int], params) -> list[int]:
    return [min(((a * x + b) % _PRIME) for x in sh) for a, b in params]


def lsh_bands(sig: list[int], bands: int, rows: int):
    for b in range(bands):
        chunk = sig[b * rows:(b + 1) * rows]
        yield b, hashlib.md5(struct.pack(f">{rows}q", *(v & 0x7FFFFFFFFFFFFFFF for v in chunk))).hexdigest()
