# data-loop 可视化 Demo

演示训练数据在九个模块间的真实流转。后端是**纯标准库** `http.server`(无新依赖),
读取真实 mini-run 产物(Lance 表 + SQLite registry);前端是**零依赖**的单页
SVG(无构建步骤)。

## 运行

```bash
# 1) 先产出数据 (若还没跑过)
uv run python scripts/run_mini.py

# 2) 起可视化服务
uv run python -m webapp.server          # 默认 http://127.0.0.1:8000
uv run python -m webapp.server 8020     # 指定端口
```

浏览器打开后:

- **流程图**:M1→M9 三泳道拓扑(离线/在线/闭环),每个节点显示真实行数与
  Lance 物理版本 `@vN`,主链路上有流动粒子动画;虚线是消融/回流闭环边。可暂停流动、刷新数据。
- **节点面板**:点任一模块,展开它的技术栈、指标、对应 Lance 表的 schema 与抽样行。
- **版本/快照面板**:registry 的逻辑版本及其绑定的 Lance 物理快照(output/input)、
  消融报告的 A/B 两臂快照、去污染记录——演示"版本可复现"特性。
- **血缘面板**:输入一条偏好对 id 反查到 turn,或一篇文档 id 反查到 WARC 偏移(起点已预填真实样本)。

## 接口

| 路由 | 说明 |
|---|---|
| `GET /api/pipeline` | 九模块拓扑 + 各阶段真实指标 |
| `GET /api/table/<name>` | 某 Lance 表的 schema + 抽样行 + 物理 version |
| `GET /api/registry` | 数据集版本 / 快照绑定 / 消融 / 去污染 |
| `GET /api/lineage/<kind>/<id>` | 血缘反查(kind: pair / doc / turn …) |
| `GET /api/sample-ids` | 血缘探索起点(真实 pair / doc id) |

切到集群 profile 同样可用——后端走 `Lake` / `Registry` 抽象,改 `configs` 即指向
S3 上的 Lance 表与 Postgres registry(本服务默认读 `configs/mini.yaml`)。
