# data-loop 可视化

两套视图,各用所长,共用同一数据层 `data.py`(纯函数,读真实 mini-run 产物:
Lance 表 + SQLite registry):

| 视图 | 适合 | 技术 | 启动 |
|---|---|---|---|
| **流程图**(`server.py` + `static/`) | 对外讲"数据如何流转" | 标准库 http.server + 零依赖 SVG | `uv run dataloop-webapp` |
| **数据探索**(`explorer.py`) | 钻取/筛选真实表数据 | Streamlit | `uv run --extra viz streamlit run src/dataloop/webapp/explorer.py` |

```text
webapp/
├── data.py        # 共用数据层: pipeline/table/registry/lineage/sample_ids 纯函数
├── server.py      # SVG 流程图后端 (HTTP, 零依赖)
├── static/        # 单页 SVG 前端
└── explorer.py    # Streamlit 数据探索 app
```

## 流程图(动画版)

```bash
uv run dataloop-mini        # 先产数据 (若没跑过)
uv run dataloop-webapp      # http://127.0.0.1:8000
uv run dataloop-webapp 8020 # 指定端口
```

- 三泳道 M1→M9 拓扑(离线/在线/闭环),节点显示真实行数与 Lance 物理版本 `@vN`;
- 主链路有流动粒子动画,虚线为消融/回流闭环边,可暂停/刷新;
- 点节点看 schema + 抽样行;侧栏含 registry 版本/Lance 快照绑定、血缘反查。

后端接口(也可独立调用):`/api/pipeline`、`/api/table/<name>`、`/api/registry`、
`/api/lineage/<kind>/<id>`、`/api/sample-ids`。

## 数据探索(Streamlit)

```bash
uv sync --extra viz
uv run streamlit run src/dataloop/webapp/explorer.py
```

四个标签页:管线概览(指标卡)、表数据探索(全表 `st.dataframe` + 列筛选 +
质量分直方图)、版本/快照(逻辑版本及其绑定的 Lance 物理快照、消融、去污染)、
血缘反查。侧栏可切 mini/cluster profile。

两套视图都走 `Lake` / `Registry` 抽象,改 `configs` 即指向集群 profile 的
S3 Lance 表与 Postgres registry(默认读 `configs/mini.yaml`)。
