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
- **流批分离**: 有状态的事件关联归 Flink,GPU 密集批推理归 Daft+vLLM,湖表(Lance)解耦,互不阻塞。
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
| M1 | warc-ingest | Daft, warcio, trafilatura + resiliparse | 抽取后文档表 |
| M2 | filter | Daft, fastText, 规则集 | 过滤后文档表 |
| M3 | dedup | Daft, MinHash LSH | 去重后文档表 |
| M4 | quality | Daft, BGE-zh + 回归头 | 带质量分文档表 |
| M5 | corpus-build | Daft, tokenizer | stable/anneal token 集 |
| M6 | ablation | 小模型训练脚本 + eval harness | 决策报告 |
| M7 | signal-ingest | Flink (SQL + DataStream) | TurnCandidate 湖表 |
| M8 | pref-build | Daft, vLLM, judge | PreferencePair 数据集 |
| M9 | registry | 元数据库 (SQLite/PG) | 版本、血缘、去污染记录 |

数据层统一为 Lakehouse:本地与集群都用 Lance 数据集,只是 `data_root` 从本地目录换成对象存储 URI(如 `s3://`)。

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

- 表: `dataset_version`、`lineage_edge`、`decontam_log`、`ablation_report`、`dataset_snapshot`。
- 任何写入下游训练目录的数据集必须先注册版本并通过去污染检查。
- 血缘边: 文档级到 `warc_file+offset`;偏好级到 `conversation_id+turn_id`。

去污染基线: eval 套件全部条目做 13-gram 精确匹配 + MinHash 近重,命中即剔除并写 `decontam_log`。anneal 层 + 偏好集执行更高阈值。

### 6.1 版本可复现:registry 绑定 Lance 物理快照

逻辑版本(`corpus-stable-v1`、`pref-v1`)钉到它消费/产出的 Lance 物理 `version`,
两者通过 `dataset_snapshot` 表桥接,真正兑现"每个数据集版本可复现"。设计细节见
[`docs/design_lance_registry.md`](docs/design_lance_registry.md)。

- 写入层 `Lake.write` 返回提交的 Lance version;`Lake.read(name, version=N)` 时间旅行读历史快照。
- `register(..., snapshots=[...])` 把逻辑版本绑定到 output/input 物理快照;
  `save_ablation(..., snap_a, snap_b)` 把 A/B 两臂绑定到各自不可变快照。
- Lance overwrite 仍保留历史 version,故重跑上游后,旧逻辑版本按记录的 version 仍能精确还原。
- registry 是唯一权威;同时在 Lance 上打 `tag=版本号` 作冗余镜像,便于 `lance` 直接浏览。
- 原子顺序:先写 Lance(拿到 version)再 registry 记录,registry 绝不指向未提交数据。

```bash
uv run python -m registry.cli snapshots --id pref-v1      # 列出绑定的物理快照
uv run python -m registry.cli reproduce  --id pref-v1 --load  # 按快照还原并验证行数
uv run python -m registry.cli cleanup    --keep-days 7    # 回收未被引用的超龄旧 version
```

---

## 7. 技术选型与运行 profile

两套 profile 共用同一份模块代码、表 schema 与存储/计算抽象,差异仅在后端实现:
mini 与 cluster 的计算引擎都是 Daft、流都是 Flink、抽取都是 trafilatura+resiliparse,
只是单机 vs 分布式的部署不同。

| 维度 | mini profile(本地,默认) | cluster profile |
|---|---|---|
| 计算 | Daft 单机 (native runner) | Daft Ray runner |
| 流 | Flink MiniCluster + 文件 replay 源 | Flink on K8s + Kafka |
| 存储 | Lance 数据集(本地目录) | Lance 数据集(对象存储 URI) |
| 抽取 | trafilatura + resiliparse | trafilatura + resiliparse |
| 推理 | vLLM / 外部 API 适配层(小模型或 mock)| vLLM GPU 节点 |
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

## 11. 运行方式

两套 profile 共用同一份代码与表 schema,只通过配置文件切换后端实现:
本地用 `configs/mini.yaml`(默认),集群用 `configs/cluster.yaml`。

### 11.1 本地运行 (mini profile, 默认)

单机即可全链跑通(64GB 内存即可,无需 GPU/Kafka,需 **Java 17+** 跑 Flink
MiniCluster)。后端与集群同构:计算用 **Daft 单机 native runner**,存储用
**Lance 数据集**(`data/` 下每张表一个 dataset),抽取用 **trafilatura +
resiliparse**,M7 用 **Flink MiniCluster**(从文件 replay 源读事件)。仅 GPU
密集的模型组件(fastText langid / BGE-zh / vLLM 教师与裁判)在 mini 走可插拔
适配层的确定性 mock;registry 用 SQLite(`data/registry.sqlite`)。

```bash
uv sync                       # 创建 .venv 并按 uv.lock 安装 (含 daft/lance/flink/trafilatura)

# 全链 9 模块端到端 (自动生成模拟 WARC + 事件流): 30 文档 → 双层语料 → 偏好集
uv run python scripts/run_mini.py

# 单元 + 端到端测试 (含 M7 Flink↔replay 输出一致性校验)
uv run pytest -q

# registry 查询 (版本 / 消融报告 / 血缘反查)
uv run python -m registry.cli versions
uv run python -m registry.cli ablations
uv run python -m registry.cli trace --kind pair --id <pair_id>
```

> M7 默认后端是 Flink MiniCluster(`configs/mini.yaml` 的
> `m7_signal_ingest.backend: flink`)。仓库保留了纯 Python `replay` 后端,
> 仅用于 `tests/test_m7_flink.py` 校验两者输出逐行一致;切回它把 backend 改成
> `replay` 即可(无需 JVM)。

### 11.2 集群运行 (cluster profile) —— 只改配置

分布式运行**不需要改任何代码**,只换配置文件。所有后端选择都从 configs 读取并由
统一的分发层(`common/lake.py` 存储、`common/config.py` 计算 runner、
`registry/db.py` 元库、各模块适配层)路由:

```bash
uv sync --extra cluster            # 装 ray / psycopg / openai / s3fs
# GPU 节点上另装: uv pip install vllm fasttext-wheel

# 离线管线 (Daft Ray runner, 读 s3 WARC, 写 Lance on S3, 注册进 Postgres)
uv run python -c "from common.config import load_profile; from offline.m1_warc_ingest import ingest; ingest.run(load_profile('configs/cluster.yaml'))"
# M2–M8 同理传 configs/cluster.yaml; 或整链: uv run python scripts/run_mini.py configs/cluster.yaml
```

`configs/cluster.yaml` 与 `configs/mini.yaml` 字段一一对应,切换点全部是配置项:

| 组件 | mini | cluster | 配置项 |
|---|---|---|---|
| 计算引擎 | Daft native runner | Daft Ray runner | `runner: native\|ray` |
| 存储 | Lance(本地目录) | Lance(对象存储 URI) | `data_root` + `storage_options` |
| M1 抽取 | trafilatura + resiliparse | trafilatura + resiliparse | (同构,不变) |
| M2 语种 | CJK 启发式 | fastText lid.176 | `m2_filter.langid` |
| M4 教师 | 启发式 | 强模型 API (rubric) | `m4_quality.teacher` + `teacher_endpoint` |
| M4 嵌入 | char-ngram hashing | BGE-zh embeddings API | `m4_quality.embedding` + `embedding_endpoint` |
| M7 流源 | 文件 replay (有界) | Kafka (无界) | `m7_signal_ingest.source` + `kafka` |
| M7 执行 | Flink MiniCluster | Flink on K8s | `m7_signal_ingest.jobmanager` |
| M8 生成 | 确定性 mock | vLLM | `m8_pref.generator` + `generator_endpoint` |
| M8 裁判 | 确定性 mock | vLLM 裁判 | `m8_pref.judge` + `judge_endpoint` |
| registry | SQLite | Postgres | `registry_db: postgresql://...` |

分发逻辑均有测试覆盖:`tests/test_distributed_dispatch.py` 用假后端验证 cluster
配置确实把 storage_options 透传给 Lance、解析出 Postgres DSN、构造 vLLM 客户端与
BGE 嵌入 API,无需真实集群。

> 边界:分发代码已实现且经假后端验证,但**端到端的真实分布式联调**(实际 Ray
> 集群、Lance on S3/OSS、Flink on K8s + Kafka、Postgres、vLLM 服务)需在对应基础
> 设施上进行,本仓库未包含部署清单(K8s manifest / Helm)。fastText `lid.176.bin`、
> BGE/强模型/vLLM 服务需自行就位。
