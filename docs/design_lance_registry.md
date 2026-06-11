# 设计文档:Lance 版本管理与 Registry 结合

**状态**: 设计 → 实施
**关联**: README §1.2(可复现)、§6(注册与血缘)、M9 registry、`common/lake.py`
**日期**: 2026-06-11

---

## 1. 背景与问题

平台现在有**两套互不相干的版本系统**:

- **Registry**(`registry/db.py`)记录逻辑版本:`corpus-stable-v1`、`pref-v1`,
  外加一份 JSON manifest。但 manifest 不含任何指向物理数据的指针。
- **Lance**(`common/io.py` 写入层)每次写都生成单调递增的物理 `version`(整数)。
  实测 `mode="overwrite"` **也保留全部历史版本**,可 `lance.dataset(uri, version=N)`
  时间旅行读回。`run_mini` 每重跑一次,每张表就多一组不可变快照。

**问题**:逻辑版本指向"这张表此刻的样子",而表在重跑中不断变化。于是:

1. README §1.2 承诺的"每个数据集版本可复现"**没有真正兑现**——尽管底层物理
   快照都还在,registry 却没记录哪个逻辑版本对应哪个物理 version。
2. 血缘 `doc_id → warc offset` 在表被覆盖后会指到"现在同 id 的那行",而非产出时的行。
3. 消融 A/B 两臂指向表名而非固定快照,重跑后"数据变了、报告对不上"。
4. registry 与 Lance 写入非原子,可能出现 registry 指向不存在/已变化的数据。

**机会**:Lance 已经在免费攒不可变快照。只要把 registry 的逻辑版本**钉到** Lance
的物理 version,上述四点一并解决。

---

## 2. 目标与非目标

### 目标
- G1 **可复现**:任一已注册逻辑版本可精确还原其消费/产出的物理数据快照。
- G2 **快照级血缘**:血缘可定位到产出某样本时上游表的确切 Lance version。
- G3 **消融绑定快照**:A/B 两臂引用同一上游表的两个不可变 version。
- G4 **原子顺序**:registry 永远只指向已 commit 的 Lance 快照,不留悬空引用。
- G5 **可浏览**:Lance 数据集自带人类可读的版本标签(tag),脱离 registry 也能认。
- G6 **可回收**:提供清理旧 version 的入口,避免对象存储无限膨胀。

### 非目标
- 不改变 mini/cluster 同构与"切换只改配置"原则。
- 不引入外部事务协调器(2PC/Saga);用"先 commit 后记录"的弱保证(见 §4.6)。
- 不做 registry 与 Lance 的双向实时同步;registry 是唯一权威,tag 仅冗余镜像。

---

## 3. 核心概念:快照绑定 (snapshot binding)

引入一个桥接概念:**dataset snapshot** = 一个逻辑版本在某张物理表上钉住的
`(table_name, table_uri, lance_version)` 三元组。

一个逻辑版本(如 `pref-v1`)会绑定多个 snapshot:
- **output**:它产出的物理表快照(`preference_pair`、`rlhf_example`)。
- **input**:它消费的上游表快照(`turn_candidate`)。

这层关系存进新表 `dataset_snapshot`,成为 registry 逻辑版本与 Lance 物理 version
之间唯一的桥。所有可复现/血缘/消融能力都从这张表派生。

```
dataset_version (corpus-stable-v1) ──┐
                                     │  dataset_snapshot
                                     ├── output  doc... (n/a, corpus 是文本 shard)
                                     └── input   doc_scored @ lance_version=4
ablation_report (dedup_scope-v1) ────┐
                                     ├── arm_a   doc_scored_a @ version=2
                                     └── arm_b   doc_scored_b @ version=2
```

---

## 4. 详细设计

### 4.1 写入层返回物理 version(`common/io.py` + `common/lake.py`)

`write_table` 写完后读取并返回提交的 version:

```python
def write_table(rows, schema, uri, storage_options=None) -> int:
    ...
    lance.write_dataset(table, str(uri), mode="overwrite",
                        data_storage_version="2.2", storage_options=storage_options)
    return lance.dataset(str(uri), storage_options=storage_options).version
```

`Lake` 暴露:
- `write(name, rows, schema, partition=None) -> int`:返回该表新 version。
- `read(name, version=None) -> list[dict]`:`version` 非空时时间旅行读历史快照。
- `latest_version(name) -> int | None`:当前表的最新 version(不存在返回 None)。
- `snapshot(name, version=None) -> Snapshot`:构造 `(name, uri, version)` 三元组,
  `version` 缺省取最新。
- `read_daft(name, version=None)`、`tag(name, tag, version)`、`cleanup(name, keep)`。

`Snapshot` 为轻量 dataclass:`{name, uri, version}`。

### 4.2 registry schema 扩展

新增一张 `dataset_snapshot`,并给 `dataset_version` 增列(向后兼容,旧库自动建新表):

```sql
CREATE TABLE dataset_snapshot (
  version_id    TEXT,      -- 逻辑版本 id 或 决策 id
  role          TEXT,      -- output | input | arm_a | arm_b ...
  table_name    TEXT,      -- doc_scored / turn_candidate ...
  table_uri     TEXT,      -- Lance 数据集 URI
  lance_version BIGINT,    -- 物理快照号
  UNIQUE(version_id, role, table_name)
);
CREATE INDEX idx_snapshot_version ON dataset_snapshot(version_id);
```

`dataset_version` 不动结构(manifest 里已能放 free-form 字段);snapshot 单独建表
更规范、可索引、可 join。

### 4.3 register 绑定 snapshot

`register` 增加可选 `snapshots` 参数:

```python
def register(self, version_id, kind, manifest, snapshots: list[Snapshot|tuple] = None):
    # 1) decontam 闸不变
    # 2) 写 dataset_version
    # 3) 逐条写 dataset_snapshot (role 由调用方给, 见下)
    # 4) 镜像 Lance tag (§4.5), 失败不阻断
```

调用方改动:
- **M5 corpus**:`register(mid, "corpus_manifest", manifest, snapshots=[
  lake.snapshot("doc_scored").as_role("input")])`。corpus 产物是文本 shard,
  其"可复现"取决于上游 `doc_scored` 快照,故只绑 input。
- **M8 pref**:`register(version_id, "rlhf_example", manifest, snapshots=[
  out_pref.as_role("output"), out_rlhf.as_role("output"),
  lake.snapshot("turn_candidate").as_role("input")])`,
  其中 `out_pref = lake.write("preference_pair", ...)` 返回的 version 现场构造 snapshot。

### 4.4 消融绑定快照

`save_ablation` 额外接收两臂 snapshot,写进 `dataset_snapshot`(version_id=decision_id,
role=arm_a/arm_b)。ablation 跑完即知道两臂各自的 `doc_scored_{a,b}` 物理 version,
报告从此指向不可变快照。

### 4.5 Lance tag 镜像(G5)

`register` 对每个 **output** snapshot 调 `dataset.tags.create(tag, version)`,
`tag = sanitize(version_id)`(Lance tag 名限定字母数字与 `-_`,做一次替换)。
失败(远端权限/不支持)记 warning 不阻断——registry 仍是权威。
新增 `Lake.tag(name, tag, version)` 封装,内部 `lance.dataset(uri).tags.create/update`。

### 4.6 原子顺序(G4)

强制时序:**先写 Lance(拿到 version)→ 再 registry 记录**。
- `Lake.write` 返回 version 时,数据已 commit 落盘。
- `register` 把已知 version 写入 registry。
- 崩溃窗口:Lance 已 commit 但 registry 未记 → 产生"孤儿 version",可被 §4.8
  cleanup 回收;**绝不会**出现 registry 指向不存在/未提交的数据。

不引入分布式事务;这是符合数据管线语义的弱保证,代价低、够用。

### 4.7 时间旅行复现 API

- `Lake.read(name, version=N)`:还原历史快照。
- `Registry.snapshots(version_id) -> list[dict]`:取某逻辑版本绑定的全部快照。
- CLI 新增 `reproduce --id <version_id>`:打印每张表的 `uri @ version`,并可
  `--load` 实际读回首表行数,验证可还原。

### 4.8 版本清理(G6)

`Lake.cleanup(name, keep_versions=N, older_than_days=D)` 封装
`lance.dataset(uri).cleanup_old_versions(...)`。CLI `cleanup --keep N`。
受 registry 引用的 version **不应**被回收:cleanup 前先查 `dataset_snapshot`
收集被引用 version 集合,只清未被引用且超龄的。mini 默认不触发。

---

## 5. Schema 变更与迁移

- 新增表 `dataset_snapshot`(`CREATE TABLE IF NOT EXISTS`,旧库无痛升级)。
- `dataset_version`、`lineage_edge`、`decontam_log`、`ablation_report` 结构不变。
- 旧记录无 snapshot 行 → `reproduce`/`snapshots` 对其返回空,语义为"未绑定快照"。

## 6. API 变更一览

| 位置 | 变更 | 兼容性 |
|---|---|---|
| `io.write_table` | 返回 `int` version | 返回值新增,调用方可忽略 |
| `Lake.write` | 返回 `int` version | 同上 |
| `Lake.read` | 增 `version=None` 参数 | 默认行为不变 |
| `Lake` | 新增 `snapshot/latest_version/tag/cleanup` | 纯新增 |
| `Registry.register` | 增 `snapshots=None` 参数 | 默认 None,行为不变 |
| `Registry.save_ablation` | 增 `snap_a/snap_b=None` | 默认 None |
| `Registry` | 新增 `snapshots()` | 纯新增 |
| CLI | 新增 `reproduce`、`cleanup`、`snapshots` | 纯新增 |

## 7. 实施阶段

- **P1 写入层 + schema**:`write_table`/`Lake` 返回 version,建 `dataset_snapshot`,
  `register/save_ablation` 接收并落库。可复现读取(`read(version=)`、`snapshots()`)。
- **P2 接线**:M5/M8/ablation 现场采集 snapshot 传入。
- **P3 周边**:Lance tag 镜像、CLI `reproduce/snapshots/cleanup`。
- **P4 文档**:README §6 更新,补"版本可复现"说明。

## 8. 测试计划

- `write_table` 重复写,version 递增;`read(version=旧)` 取回旧内容。
- 端到端:`pref-v1` 注册后 `snapshots("pref-v1")` 含 output(preference_pair/
  rlhf_example)与 input(turn_candidate),且 `read(name, version=记录值)` 行数一致。
- 消融:`snapshots("dedup_scope-v1")` 含 arm_a/arm_b,version 为正整数。
- 复现:改写上游表(再 write 一次造新 version)后,用 registry 记录的旧 version
  仍能读回注册当时的数据(证明 G1)。
- tag 镜像:本地 Lance 数据集 `tags.list()` 含 sanitize(version_id)。
- 兼容:旧库(无 dataset_snapshot)打开自动建表,旧逻辑版本 `snapshots` 返回空。

## 9. 风险

- **快照膨胀**:每次重跑攒 version,集群对象存储成本上升 → §4.8 cleanup + 文档提示。
- **tag 名限制**:Lance tag 字符受限 → sanitize;冲突时 `update` 覆盖。
- **远端 tag/version 读取开销**:`latest_version` 每次开 dataset 有 IO → 仅在
  write 后与 register 时调用,频率低;必要时缓存。
- **SSOT 漂移**:tag 与 registry 不一致 → 明确 registry 权威,tag 只读镜像、可重建。
