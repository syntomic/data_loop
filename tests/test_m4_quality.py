import numpy as np

from offline.m4_quality import teacher
from offline.m4_quality.classifier import Regressor, embed


def test_teacher_orders_quality():
    edu = "教材中推导公式,例如首先定义,其次证明定理,因此可得性质。" * 5
    spam = "点击优惠促销广告免费领取加微信。" * 10
    assert teacher.score(edu) > teacher.score(spam)


def test_regressor_fits():
    texts = [f"文档{i}" + "教材推导例如" * (i + 1) for i in range(8)]
    X = np.stack([embed(t, 64) for t in texts])
    y = np.linspace(1, 5, 8).astype(np.float32)
    r = Regressor(64)
    r.fit(X, y)
    pred = r.predict(X)
    assert pred[-1] > pred[0]
    assert (pred >= 0).all() and (pred <= 5).all()
