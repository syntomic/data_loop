from dataloop.offline.m3_dedup.minhash import _params, lsh_bands, shingles, signature

A = "等差数列的求和方法。首先定义首项与公差,其次推导前n项和的公式,例如首项一公差二时计算前十项的和。" * 4
B = A + "(转载)"
C = "光合作用是植物把光能转化为化学能的过程,叶绿体吸收光能并固定二氧化碳。" * 5


def _bucket_overlap(x, y):
    p = _params(112)
    bx = set(lsh_bands(signature(shingles(x), p), 14, 8))
    by = set(lsh_bands(signature(shingles(y), p), 14, 8))
    return bool(bx & by)


def test_near_duplicate_collides():
    assert _bucket_overlap(A, B)


def test_distinct_no_collision():
    assert not _bucket_overlap(A, C)
