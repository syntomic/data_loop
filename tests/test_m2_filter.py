from offline.m2_filter.langid import detect
from offline.m2_filter.rules import evaluate

RULES = {"min_chars": 200, "max_chars": 100000, "max_symbol_ratio": 0.25,
         "max_digit_ratio": 0.3, "max_dup_line_ratio": 0.3, "max_dup_2gram_ratio": 0.2,
         "min_zh_char_ratio": 0.6, "min_stopword_kinds": 2}
STOP = {"的", "了", "是", "在", "和"}

GOOD = ("等差数列的求和方法是数学中重要的内容。在学习时我们先定义首项和公差,然后推导公式,"
        "再通过例题加以巩固。这一思想方法在后续学习中也有广泛应用,值得反复体会和练习。"
        "类似地,等比数列可以用错位相减求和,数列极限则刻画了无穷多项相加的趋势,"
        "这些内容共同构成高中代数的核心章节,考试中占据相当比例,需要熟练掌握。"
        "建议同学整理出推导脉络:先观察、再猜想,然后做严格证明,最后再回到题目检验思路,长期坚持就能形成稳定方法。")
SPAM = "点击优惠促销广告" * 50
EN = "This is an English page only. " * 30


def test_langid():
    assert detect(GOOD)[0] == "zh"
    assert detect(EN)[0] == "other"


def test_rules_good_passes():
    assert all(evaluate(GOOD, RULES, STOP).values())


def test_spam_fails_dup_gram():
    assert not evaluate(SPAM, RULES, STOP)["dup_2gram_ok"]


def test_short_fails_len():
    assert not evaluate("短文。", RULES, STOP)["len_ok"]
