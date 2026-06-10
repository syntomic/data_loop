# LLM 训练数据闭环平台 · 项目设计文档

**项目代号**: `data-loop`
**版本**: v0.1 (设计稿)
**日期**: 2026-06-10
**关联项目**: Mini-FineWeb-ZH (离线管线在其基础上扩展)

---

## 1. 项目概述

### 1.1 目标

构建一套覆盖大模型训练数据全生命周期的闭环平台,包含三个子系统:

1. **离线数据管线 (offline pipeline)**: 从 Common Crawl WARC 到分层预训练语料(stable 层 + anneal 层),以中文为主要目标语言。
2. **在线数据管线 (online pipeline)**: 从生产反馈信号到定向偏好数据集(preference dataset),Flink 流式前端 + Daft/vLLM 批式后端。
3. **闭环基础设施 (loop infra)**: 消融评估、数据集版本化、血缘追踪、去污染,把前两条管线缝成可迭代闭环。

### 1.2 设计原则

- **评估优先**: 任何过滤/配比决策必须可被消融实验或退火评估验证,杜绝拍脑袋规则。
- **分层产出**: 质量分类输出连续分数,同一管线用双阈值切出 stable / anneal 两层,不做单一 pass/fail。
- **流批分离**: 有状态的事件关联归 Flink,GPU 密集批推理归 Daft+vLLM,湖表(Paimon/Iceberg)解耦,互不阻塞。
- **血缘贯穿**: 每条样本可反查来源(WARC 偏移 / conversation_id),每个数据集版本可复现。
- **本地可跑**: 全链路提供 mini profile,可在单机(64GB 内存,Apple Silicon)上以缩样数据端到端跑通。

### 1.3 非目标

- 不实现训练框架本身(预训练 / SFT / RLHF 训练器不在范围内,只产出其输入数据)。
- 不实现生产推理服务,生产信号以模拟事件流(replay 数据集)代替。
- 不做多模态(图/音/视频)管线,接口预留。

---

## 2. 总体架构

```
                ┌──────────────── 消融反馈(离线闭环) ─────────────────┐
                ▼                                                      │
  CC WARC ─→ offline pipeline ─→ corpus(stable/anneal) ─→ pretrain ─→ eval suite
                ▲                                                      │
                │                                                  deploy(模拟)
                │                                                      │
  preference dataset ←─ Daft+vLLM batch ←─ candidate lake ←─ Flink ←─ feedback events
                └──────────────── 生产回流(在线闭环) ─────────────────┘
```

模块视图:

| 模块 | 名称 | 技术栈 | 产出 |
|---|---|---|---|
| M1 | warc-ingest | Daft, warcio, trafilatura | 抽取后文档表 |
| M2 | filter | Daft, fastText, 规则集 | 过滤后文档表 |
| M3 | dedup | Daft, MinHash LSH | 去重后文档表 |
| M4 | quality | Daft, BGE-zh + 回归头 | 带质量分文档表 |
| M5 | corpus-build | Daft, tokenizer | stable/anneal token 集 |
| M6 | ablation | 小模型训练脚本 + eval harness | 决策报告 |
| M7 | signal-ingest | Flink (SQL + DataStream) | TurnCandidate 湖表 |
| M8 | pref-build | Daft, vLLM, judge | PreferencePair 数据集 |
| M9 | registry | 元数据库 (SQLite/PG) | 版本、血缘、去污染记录 |

数据层统一为 Lakehouse(本地 mini profile 用 Parquet 目录模拟分区表;集群 profile 用 Paimon)。

---
## 3. 离线管线设计 (M1–M5)

### 3.1 M1 warc-ingest

输入: CC dump 的 WARC 分片清单 (mini profile: 单 dump 抽样 ~50 个 WARC 文件)。

处理: warcio 流式解析 → 仅保留 `response` 记录与 `text/html` → trafilatura 抽正文(`favor_precision=True`) → 失败 fallback resiliparse,记录抽取器来源。

输出表 `doc_raw`:

```
doc_raw {
  doc_id          string   // sha1(url + warc_offset)
  url             string
  warc_file       string
  warc_offset     long
  dump_id         string   // 如 CC-MAIN-2026-18
  text            string
  extractor       string   // trafilatura | resiliparse
  fetch_ts        timestamp
}
分区: dump_id
```

### 3.2 M2 filter

顺序执行,每条规则记录命中标记,便于消融时单独开关:

1. **langid**: fastText lid.176,`zh` 置信 ≥ 0.65 保留。
2. **垃圾站黑名单**: 域名级黑名单(内容农场/SEO 站),外部可配置文件。
3. **启发式规则集** (Gopher/C4 改造,中文标定):
   - 长度: 字符数 ∈ [200, 100k]
   - 符号比 < 0.25;数字比 < 0.3
   - 重复行比例 < 0.3;重复 2-gram 比例(字粒度) < 0.2
   - 中文字符占比 ≥ 0.6
   - 停用词(中文虚词表)命中 ≥ 2 种

输出表 `doc_filtered` = `doc_raw` + `lang`, `lang_conf`, `filter_flags map<string,bool>`, `kept bool`。**被滤文档保留 flags 不删行**,消融需要。

### 3.3 M3 dedup

- 粒度: per-dump MinHash LSH(FineWeb 结论,全局去重不做默认,留作消融项)。
- 中文 n-gram: 字粒度 5-gram;num_perm=112,b=14, r=8(≈0.75 Jaccard)。
- 簇内保留质量启发分最高的一篇;其余记 `dup_of`。

输出表 `doc_dedup` = 保留行 + `cluster_id`, `cluster_size`。

### 3.4 M4 quality

两段式(FineWeb-Edu 复刻,中文化):

1. **教师打标(一次性)**: 中文强模型按本地化 rubric(教育/信息价值 0–5)给 ~50 万抽样打分,产出标注集 `teacher_labels` v1。rubric 单独维护于 `configs/rubric_zh.md`。
2. **学生分类器**: BGE-zh embedding + 线性回归头。全量推理输出连续分 `quality_score ∈ [0,5]`。

输出表 `doc_scored` = `doc_dedup` + `quality_score`, `classifier_version`。

### 3.5 M5 corpus-build (分层)

| 层 | 条件(默认,消融可调) | 用途 |
|---|---|---|
| stable | quality_score ≥ 3.0 | 预训练稳定期 |
| anneal | quality_score ≥ 4.2,叠加领域上采样(教材/代码/QA 外部精选源) | 退火期 |

tokenize 后落 token shard;同时写 `corpus_manifest`(各层文档数、token 数、来源 dump 分布、分类器与规则版本)进 registry。

---

## 4. 消融评估子系统 (M6)

- **决策粒度**: 每个待验证决策 = 一对 manifest 差异(如 dedup-global vs per-dump)→ 各训 ablation 模型(默认 0.2B,5B token)。
- **基准选择**: 早期低方差中文基准(CMMLU 子集、C-Eval 子集、CLUE 常识题)+ ppl 监控;基准集禁入训练数据(见 §7 去污染)。
- **退火评估法**: 共享 checkpoint + 候选集小段退火,作为低成本预筛,显著决策再上完整消融。
- **产出**: `ablation_report`(决策 id、配置 diff、分数、显著性、结论),写入 registry,关联到 manifest 版本。

---

## 5. 在线管线设计 (M7–M8)

### 5.1 M7 signal-ingest (Flink)

输入: Kafka 两个 topic(`message_events`, `feedback_events`)。本地 mini profile 用文件 replay。

- **关联**: 按 conversation_id keyBy;keyed state + timer;watermark 5min,allowed lateness 30min;state TTL 24h。
- **隐私闸**: consent 维表 lookup join,`consent_ok=false` 丢弃;正则 + NER PII 清洗 UDF(可插拔)。
- **会话级聚合**: thumbs/regenerate/edit/stop/followup-correction 信号聚合到 turn。
- **输出**: TurnCandidate 表(schema 同前述讨论),按 dt/model_version 分区写湖表。

### 5.2 M8 pref-build (Daft + vLLM)

按日批读 TurnCandidate,固定流水:

```
score(reward + safety + embedding)
 → cluster + novelty
 → active_sampling(负反馈 | 高分歧 | 低置信 | 新簇,簇配额封顶)
 → generate(vLLM, 候选来源混合: current_model 多温度 / strong_model / user_edit / constitutional_revision)
 → judge(rubric 4 维,conf ≥ 0.8 自动通过,< 0.8 路由人工队列)
 → qc(去重、去污染、领域平衡)
 → 写 PreferencePair → RLHFExample 数据集版本
```

数据 schema 沿用既定的 TurnCandidate / ScoredCandidate / SelectedPrompt / ResponseCandidates / PreferencePair / RLHFExample 六张表,详见 schemas 目录。

---

## 6. 注册与血缘 (M9 registry)

- 表: `dataset_version`、`lineage_edge`、`decontam_log`、`ablation_report`。
- 任何写入下游训练目录的数据集必须先注册版本并通过去污染检查。
- 血缘边: 文档级到 `warc_file+offset`;偏好级到 `conversation_id+turn_id`。

去污染基线: eval 套件全部条目做 13-gram 精确匹配 + MinHash 近重,命中即剔除并写 `decontam_log`。anneal 层 + 偏好集执行更高阈值。

---

## 7. 技术选型与运行 profile

| 维度 | mini profile(本地,默认) | cluster profile |
|---|---|---|
| 计算 | Daft 单机 (native runner) | Daft Ray runner |
| 流 | Flink MiniCluster + 文件 replay | Flink on K8s + Kafka |
| 存储 | 本地 Parquet 分区目录 | Paimon on OSS/S3 |
| 推理 | vLLM(Metal 受限,小模型或外部 API)| vLLM GPU 节点 |
| registry | SQLite | Postgres |

mini profile 必须保证全链 9 个模块端到端跑通: 50 WARC → 千条偏好对。

## 8. 仓库结构(代码生成依据)

```
data-loop/
├── configs/                # profiles(mini/cluster)、rubrics、过滤规则、黑名单
├── schemas/                # 各表 PyArrow/Avro 定义,集中维护
├── offline/
│   ├── m1_warc_ingest/  m2_filter/  m3_dedup/  m4_quality/  m5_corpus/
├── ablation/               # m6: 训练脚本、eval harness、退火评估
├── online/
│   ├── m7_flink/           # SQL + DataStream Java/Python 作业
│   └── m8_pref/            # Daft pipeline: score/sample/generate/judge/qc
├── registry/                # m9: 版本/血缘/去污染 API + CLI
├── replay-data/             # 模拟生产事件流样例
└── tests/                   # 每模块 fixture + 端到端 mini run
```

## 9. 实施里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| P1 | M1–M3 + schemas + registry 骨架 | 50 WARC → 去重表,血缘可查 |
| P2 | M4–M5 + 中文 rubric 与分类器 | 双层 corpus + manifest 注册 |
| P3 | M6 消融最小闭环 | 一项决策(全局 vs per-dump 去重)出报告 |
| P4 | M7 Flink 摄入(replay) | 迟到反馈正确关联,隐私闸 100% |
| P5 | M8 偏好流水 + 去污染 | replay → pref-v1 数据集 |
| P6 | 联调:失败簇驱动定向回流 | lineage 全链反查通过 |

## 10. 风险与对策

- **教师打标质量(中文 rubric 偏置)** → 双教师交叉抽检 + 人审 5% 校准。
- **裁判漂移** → 每版数据抽 2% 人审,Cohen's κ < 0.7 触发裁判回滚。
- **去污染遗漏** → 注册前强制检查,无 decontam 记录拒绝注册。
- **本地推理瓶颈** → judge/strong_model 在 mini profile 走外部 API 适配层,接口与 vLLM 对齐。

---

## 11. 快速开始 (mini profile)

```bash
uv sync                       # 创建 .venv 并按 uv.lock 安装 (dev 组默认包含)

# 全链 9 模块端到端 (自动生成模拟 WARC + 事件流)
uv run python scripts/run_mini.py

# 单元 + 端到端测试
uv run pytest -q

# M7 用 PyFlink MiniCluster 跑 (默认纯 Python replay, 两后端输出逐行一致)
uv sync --extra flink         # 需 Java 17+
# configs/mini.yaml: m7_signal_ingest.backend: flink
uv run pytest -q tests/test_m7_flink.py   # 一致性校验

# registry 查询
uv run python -m registry.cli versions
uv run python -m registry.cli ablations
uv run python -m registry.cli trace --kind pair --id <pair_id>
```

mini profile 的外部模型组件(fastText langid / trafilatura / BGE-zh / vLLM /
教师与裁判模型)全部走可插拔适配层,默认用确定性的纯 Python mock,接口与
cluster profile 对齐;切到 cluster 只需替换 `configs/cluster.yaml` 中的后端
并安装重型依赖 `uv pip install daft trafilatura fasttext-wheel vllm`(不入 lock)。
