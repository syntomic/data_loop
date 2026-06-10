"""生成 mini profile 的模拟输入: WARC 分片 + 生产事件流 replay (README §1.3 非目标: 生产信号用 replay 代替)。
确定性 (seed=7), 可重复生成。运行: python replay-data/generate.py
"""
import io
import json
import random
from pathlib import Path

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

HERE = Path(__file__).parent
rnd = random.Random(7)

GOOD = [
    "等差数列{i}的求和方法。一、定义首项与公差,这是数列的基本性质;二、推导前n项和的公式;三、用具体例子验证。"
    "例如,当首项是一,公差是二的时候,可以计算前十项的和。这种推导也适用于更一般的情形,因此教材中通常先讲定义再讲性质与证明。"
    "学习时建议从具体数列入手,把每一项写出来观察规律,再尝试给出一般的证明。这种由特殊到一般的归纳方法,是中学数学最重要的思想之一,值得反复体会与练习。",
    "光合作用是植物把光能转化为化学能的过程{i}。一、叶绿体吸收光能;二、把水分解;三、固定二氧化碳合成有机物。"
    "原理上,这是一个氧化还原过程。例如,在强光照与适宜温度下,光合速率明显升高。也因此植物在生态系统中扮演生产者的角色。"
    "农业生产中常通过延长光照时间与提高二氧化碳浓度来增产,这正是对该原理的应用。课堂演示中常用水绵和好氧细菌的实验来证明氧气由叶绿体释放。",
    "造纸术与印刷术的传播史{i}。一、造纸术的出现降低了书写成本;二、印刷术使知识被大规模复制;三、教材随之普及。"
    "例如,宋代的活字印刷推动了书院教育的发展。随着技术沿丝绸之路向西传播,欧洲的文艺复兴也间接受益于此。"
    "研究传播史时,应当注意技术、制度与文化三者的互动。学者们普遍认为,载体革新与知识扩散互为因果,这一框架也可解释近代报刊的兴起。",
    "牛顿第二定律{i}指出,物体加速度与合外力成正比,与质量成反比。一、定义惯性参考系;二、给出公式;三、用斜面实验验证。"
    "例如,推同一辆小车,力越大加速度越大。教材中通常配合打点计时器实验来测量加速度,并要求学生绘制图像分析比例关系。"
    "理解这一定律是学习动力学的基础,后续的动量定理与功能关系都建立在它之上,因此务必通过练习巩固对受力分析的掌握。",
    "唐诗的格律{i}包含平仄、对仗与押韵三要素。一、平仄交替形成节奏;二、颔联颈联讲究对仗;三、偶数句必须押韵。"
    "例如,五言律诗共八句四十字,首联可对可不对。教材常以杜甫的作品作为范例,因为其格律最为严谨。"
    "学习格律的意义并非束缚创作,而是理解汉语声调之美。掌握了格律,再去读宋词的词牌,就能体会到声律传统的延续与变化。",
    "细胞分裂{i}分为有丝分裂与减数分裂两类。一、有丝分裂保证体细胞遗传物质恒定;二、减数分裂使配子染色体减半;三、受精恢复倍数。"
    "例如,人的体细胞有四十六条染色体,精卵各二十三条。教材中以洋葱根尖装片观察分裂各期,是经典实验。"
    "理解分裂过程是学习遗传规律的前提,孟德尔定律的细胞学基础正在于此,因此各期染色体行为务必画图掌握。",
    "二分查找{i}是在有序数组上的高效检索算法。一、取中点比较;二、按大小关系折半;三、重复直到命中或区间为空。"
    "例如,在一千个元素中查找,最多只需十次比较。教材通常给出循环与递归两种实现,并要求证明循环不变式。"
    "其复杂度为对数级,这正是有序结构换来的收益。理解二分思想后,平衡树与跳表等结构都可视为它的推广与工程化。",
    "宋代经济{i}的繁荣体现在商业、货币与城市三方面。一、坊市制度打破;二、交子作为纸币出现;三、市镇兴起带动手工业。"
    "例如,汴京人口逾百万,夜市通宵不绝。教材以清明上河图为材料,引导学生从图像证史,分析市井结构。"
    "理解宋代经济,有助于把握中国古代经济重心南移的脉络,也为讨论近世化问题提供基础,是历史学习的重要章节。",
    "酸碱中和反应{i}的实质是氢离子与氢氧根结合成水。一、写出离子方程式;二、用指示剂判断终点;三、计算浓度。"
    "例如,盐酸滴定氢氧化钠,酚酞由红变无色即为终点。教材要求掌握滴定管读数与误差分析,这是定量实验的基本功。"
    "中和反应的原理还应用于土壤改良与胃酸治疗。由此可见,化学定律既是公式与计算,也是解决实际问题的性质工具。",
    "勾股定理{i}给出直角三角形三边的关系。一、作正方形面积证明;二、给出公式;三、用三四五验证。例如,边长三与四的直角边对应斜边五。"
    "教材中通常给出赵爽弦图证明,体现数形结合思想。这一定理也是平面几何向解析几何过渡的桥梁,距离公式正源于此。"
    "掌握定理之后,可以解决测量、定位等许多实际问题,也为学习三角函数与向量的性质打下基础,务必通过练习熟练运用。",
    "经济学的供求模型{i}解释价格形成。一、需求曲线向下;二、供给曲线向上;三、交点决定均衡价格与数量。"
    "例如,旱灾使粮食供给减少,均衡价格上升。教材会让学生用图像推导税收与补贴对均衡的影响,这是比较静态分析的基础。"
    "理解供求模型,就能解释市场是如何配置资源的,也能看出价格管制为何会带来短缺,因此被称为最有力的工具之一。",
    "数据结构中的哈希表{i}通过散列函数定位元素。一、计算键的散列值;二、定位桶;三、冲突时链表或开放寻址。"
    "例如,字典在平均情形下查找只需常数时间。教材会比较拉链法与线性探测的性质,并分析负载因子对性能的影响。"
    "哈希思想还用于去重、缓存与签名,前述去重正用到它。理解散列分布与冲突,才能正确设计键并避免退化成线性查找。",
]
SPAM = "点击优惠促销广告点击优惠促销广告点击优惠促销广告免费领取加微信" * 12
EN = "This is an English page with no Chinese content. " * 20


def gen_warc():
    out = HERE / "warc"
    out.mkdir(exist_ok=True)
    for shard in range(3):
        with open(out / f"shard-{shard}.warc.gz", "wb") as fh:
            w = WARCWriter(fh, gzip=True)
            for i in range(10):
                tpl = GOOD[(shard * 10 + i) % len(GOOD)]
                # 注入: 近重复对, 黑名单域, 垃圾文本, 英文页
                if i == 7:
                    text, host = SPAM, "example.com"
                elif i == 8:
                    text, host = tpl.format(i="(黑名单镜像)"), "seo-farm.example.com"
                elif i == 9:
                    text, host = EN, "example.com"
                elif i == 6:  # 与 i==0 模板的近重复版本
                    text, host = GOOD[(shard * 10) % len(GOOD)].format(i=f"重{shard}") + "(转载)", "example.com"
                else:
                    text, host = tpl.format(i=f"页{shard}{i}"), "example.com"
                if i not in (7, 9):
                    text += (f"本文编号{shard}{i},供教学示例使用,转载请注明出处,欢迎指正其中的错误。"
                             "建议读者在课后结合教材的对应章节做练习,并把疑问记录下来与老师讨论。")
                html = f"<html><body><p>{text}</p></body></html>"
                rec = w.create_warc_record(
                    f"https://{host}/p/{shard}/{i}", "response",
                    payload=io.BytesIO(html.encode()),
                    http_headers=StatusAndHeaders("200 OK", [("Content-Type", "text/html; charset=utf-8")], "HTTP/1.1"))
                w.write_record(rec)


def gen_events():
    t0 = 1750000000000
    msgs, fbs = [], []
    prompts = ["怎么推导等差数列求和公式", "解释光合作用的原理", "印刷术的历史影响", "写一段周报"]
    for i in range(60):
        conv, turn, user = f"conv{i:03d}", "t0", f"u{i % 6}"
        ts = t0 + i * 60000
        msgs.append({"conversation_id": conv, "turn_id": turn, "user_id": user,
                     "model_version": "m-2026-05", "prompt": prompts[i % 4] + f" #{i}",
                     "response": f"回答{i}: 这里是模型回复, 联系 13912345678", "ts": ts, "event_ts": ts})
        sig = ["thumbs_up", "thumbs_down", "regenerate", "edit", "followup_correction", "stop"][i % 6]
        # timer 在 event_ts+5min 由 watermark(再滞后5min)触发 ≈ +10min; lateness 30min → 窗口至 +40min
        delay = 12 * 60000 if i % 10 == 3 else (45 * 60000 if i % 10 == 7 else 60000)
        fb = {"conversation_id": conv, "turn_id": turn, "user_id": user, "signal": sig,
              "ts": ts + delay, "event_ts": ts + 30000}
        if sig == "edit":
            fb["edit_text"] = f"用户改写后的更好回答 {i}, 邮箱 a@b.com"
        fbs.append(fb)
    (HERE / "message_events.jsonl").write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in msgs))
    (HERE / "feedback_events.jsonl").write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in fbs))
    consent = [{"user_id": f"u{i}", "consent_ok": i != 5} for i in range(6)]
    (HERE / "consent.jsonl").write_text("\n".join(json.dumps(c) for c in consent))


if __name__ == "__main__":
    gen_warc()
    gen_events()
    print("replay-data generated")
