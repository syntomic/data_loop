"""数据探索 Streamlit app (与 SVG 流程图互补: 这里做表格钻取/筛选)。

数据逻辑共用 dataloop.webapp.data。启动:
    uv sync --extra viz
    uv run streamlit run src/dataloop/webapp/explorer.py
"""
import pandas as pd
import streamlit as st

from dataloop.common.config import load_profile
from dataloop.common.lake import Lake
from dataloop.registry.db import Registry
from dataloop.webapp import data as D

st.set_page_config(page_title="data-loop explorer", layout="wide")

LANE_COLOR = {"offline": "#58a6ff", "online": "#3fb950", "loop": "#d29922"}


@st.cache_data(show_spinner=False)
def _pipeline(profile):
    return D.pipeline(load_profile(profile))


@st.cache_data(show_spinner=False)
def _full_table(profile, name):
    rows = Lake(load_profile(profile)).read(name)
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def _registry(profile):
    return D.registry(load_profile(profile))


# ---- sidebar ----
st.sidebar.title("data-loop")
st.sidebar.caption("训练数据闭环 · 数据探索")
profile = st.sidebar.selectbox("Profile", ["configs/mini.yaml", "configs/cluster.yaml"], index=0)
if st.sidebar.button("↻ 清缓存刷新"):
    st.cache_data.clear()
st.sidebar.markdown("---")
st.sidebar.caption("流程图动画版: `uv run dataloop-webapp`")

try:
    pipe = _pipeline(profile)
except Exception as e:  # noqa
    st.error(f"读取失败: {e}\n\n先跑 `uv run dataloop-mini` 产出数据。")
    st.stop()

tab_flow, tab_tables, tab_reg, tab_lin = st.tabs(["管线概览", "表数据探索", "版本/快照", "血缘反查"])

# ---- 管线概览 ----
with tab_flow:
    st.subheader("九模块实时指标")
    for lane, label in [("offline", "离线管线 M1–M5"), ("online", "在线管线 M7–M8"),
                        ("loop", "闭环 M6 · M9")]:
        st.markdown(f"**:{'blue' if lane=='offline' else 'green' if lane=='online' else 'orange'}[{label}]**")
        nodes = [n for n in pipe["nodes"] if n["lane"] == lane]
        cols = st.columns(len(nodes))
        for c, n in zip(cols, nodes):
            v = f" · @v{n['version']}" if n["version"] is not None else ""
            c.metric(f"{n['id']} {n['name']}", n["metric"], help=f"{n['unit']}{v}\n{n['stack']}")
    st.caption("数据流: WARC → 过滤 → 去重 → 质量分 → 双层语料 →(消融)；反馈事件 → TurnCandidate → 偏好对 →(注册/血缘)")

# ---- 表数据探索 ----
with tab_tables:
    names = [n["table"] for n in pipe["nodes"] if n["table"] and n["table"] != "corpus"]
    name = st.selectbox("选择 Lance 表", names)
    df = _full_table(profile, name)
    lake = Lake(load_profile(profile))
    c1, c2, c3 = st.columns(3)
    c1.metric("行数", len(df))
    c2.metric("列数", df.shape[1] if not df.empty else 0)
    c3.metric("Lance 版本", f"@v{lake.latest_version(name)}")
    if df.empty:
        st.info("空表,先跑 dataloop-mini。")
    else:
        # 常见可筛选列
        for col in [c for c in ("kept", "dump_id", "judge_route", "chosen_source", "dt") if c in df.columns]:
            opts = sorted(map(str, df[col].dropna().unique()))
            if 1 < len(opts) <= 30:
                picked = st.multiselect(f"筛选 {col}", opts, default=opts)
                df = df[df[col].astype(str).isin(picked)]
        if "quality_score" in df.columns:
            st.bar_chart(df["quality_score"].round(1).value_counts().sort_index(), height=180)
        st.dataframe(df, use_container_width=True, height=420)
    st.caption(f"URI: `{lake.uri(name)}`")

# ---- 版本/快照 ----
with tab_reg:
    reg = _registry(profile)
    st.subheader("数据集版本 ↔ Lance 物理快照绑定")
    for v in reg["versions"]:
        with st.expander(f"📦 {v['version_id']}  ·  {v['kind']}", expanded=True):
            st.json(v["manifest"], expanded=False)
            if v["snapshots"]:
                st.dataframe(pd.DataFrame(v["snapshots"]), use_container_width=True, hide_index=True)
            else:
                st.caption("无快照绑定(旧版本)")
    st.subheader("消融报告")
    for a in reg["ablation"]:
        st.markdown(f"**{a['decision_id']}** — {'🟢 显著' if a['significant'] else '⚪ 无显著差异'}  "
                    f"`a={a['score_a']} b={a['score_b']}`")
        st.caption(a["conclusion"])
        if a["snapshots"]:
            st.dataframe(pd.DataFrame(a["snapshots"]), use_container_width=True, hide_index=True)
    if reg["decontam"]:
        st.subheader("去污染记录")
        st.dataframe(pd.DataFrame(reg["decontam"]), use_container_width=True, hide_index=True)

# ---- 血缘反查 ----
with tab_lin:
    cfg = load_profile(profile)
    ids = D.sample_ids(cfg)
    st.subheader("血缘反查")
    kind = st.radio("起点类型", ["pair", "doc"], horizontal=True)
    default_id = ids.get(kind, "")
    ident = st.text_input("id", value=default_id, help="偏好对 → turn;文档 → WARC 偏移")
    if ident:
        res = D.lineage(cfg, kind, ident)
        chain = [f"**{res['root']['kind']}** · `{res['root']['id']}`"]
        chain += [f"**{e['parent_kind']}** · `{e['parent_id']}`" for e in res["edges"]]
        st.markdown("  \n↓  \n".join(chain) if len(chain) > 1 else chain[0] + "\n\n_(无上游血缘)_")
