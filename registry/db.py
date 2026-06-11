"""M9 registry: 版本 / 血缘 / 去污染记录 / 消融报告 (README §6)。

按 registry_db 取值分发后端 (config-only 切换):
- 本地路径或 sqlite://...  → SQLite (mini)
- postgresql://...         → Postgres (cluster, 需 psycopg)

任何写入下游训练目录的数据集必须先有 decontam 记录, 否则拒绝注册。
SQL 用 `?` 占位与 sqlite 风格 upsert 书写, 由方言层翻译到目标后端。
"""
import json
import time
from pathlib import Path

_DDL = [
    """CREATE TABLE IF NOT EXISTS dataset_version (
         version_id TEXT PRIMARY KEY, kind TEXT, created_ts BIGINT, manifest TEXT)""",
    """CREATE TABLE IF NOT EXISTS lineage_edge (
         child_kind TEXT, child_id TEXT, parent_kind TEXT, parent_id TEXT,
         UNIQUE(child_kind, child_id, parent_kind, parent_id))""",
    """CREATE TABLE IF NOT EXISTS decontam_log (
         version_id TEXT, item_id TEXT, reason TEXT, ts BIGINT)""",
    """CREATE TABLE IF NOT EXISTS ablation_report (
         decision_id TEXT PRIMARY KEY, config_diff TEXT, score_a DOUBLE PRECISION,
         score_b DOUBLE PRECISION, significant INTEGER, conclusion TEXT,
         manifest_a TEXT, manifest_b TEXT, ts BIGINT)""",
    # Lance 物理快照绑定: 逻辑版本/决策 ↔ (表, version)。docs/design_lance_registry.md
    """CREATE TABLE IF NOT EXISTS dataset_snapshot (
         version_id TEXT, role TEXT, table_name TEXT, table_uri TEXT, lance_version BIGINT,
         UNIQUE(version_id, role, table_name))""",
    "CREATE INDEX IF NOT EXISTS idx_lineage_child ON lineage_edge(child_kind, child_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshot_version ON dataset_snapshot(version_id)",
]


def registry_dsn(cfg: dict) -> str:
    """按 registry_db 取值解析连接串: 本地路径→SQLite 文件路径; URL→原样 (Postgres)。"""
    dsn = str(cfg["registry_db"])
    if "://" not in dsn:
        from common.config import resolve
        return str(resolve(cfg, dsn))
    return dsn


class Registry:
    def __init__(self, dsn: str):
        dsn = str(dsn)
        self.is_pg = dsn.startswith("postgres")
        if self.is_pg:
            import psycopg
            self.conn = psycopg.connect(dsn)
        else:
            import sqlite3
            path = dsn[len("sqlite://"):] if dsn.startswith("sqlite://") else dsn
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(path)
        for stmt in _DDL:
            self._exec(self._ddl(stmt))
        self.conn.commit()

    @classmethod
    def from_cfg(cls, cfg: dict) -> "Registry":
        return cls(registry_dsn(cfg))

    # ---- 方言层 ----
    def _q(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.is_pg else sql

    def _ddl(self, sql: str) -> str:
        return sql if self.is_pg else sql.replace("DOUBLE PRECISION", "REAL").replace("BIGINT", "INTEGER")

    def _upsert(self, sql: str, conflict_cols: str) -> str:
        # sqlite: INSERT OR REPLACE / OR IGNORE; pg: ON CONFLICT
        if not self.is_pg:
            return sql
        return sql.replace("INSERT OR REPLACE INTO", "INSERT INTO").replace(
            "INSERT OR IGNORE INTO", "INSERT INTO")

    def _exec(self, sql: str, params: tuple = ()):  # noqa
        cur = self.conn.cursor()
        cur.execute(self._q(sql), params)
        return cur

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        """只读查询 (后端无关), 供 CLI / 报表使用。"""
        return self._exec(sql, params).fetchall()

    # ---- API ----
    def log_decontam(self, version_id: str, item_id: str, reason: str):
        self._exec("INSERT INTO decontam_log VALUES (?,?,?,?)",
                   (version_id, item_id, reason, int(time.time())))
        self.conn.commit()

    def bind_snapshot(self, version_id: str, role: str, table_name: str,
                      table_uri: str, lance_version: int | None):
        """钉住一个 (逻辑版本, 物理表快照) 绑定。lance_version 为 None 时跳过。"""
        if lance_version is None:
            return
        sql = ("INSERT INTO dataset_snapshot VALUES (?,?,?,?,?) "
               "ON CONFLICT (version_id, role, table_name) DO UPDATE SET "
               "table_uri=EXCLUDED.table_uri, lance_version=EXCLUDED.lance_version") if self.is_pg else \
              "INSERT OR REPLACE INTO dataset_snapshot VALUES (?,?,?,?,?)"
        self._exec(sql, (version_id, role, table_name, table_uri, lance_version))
        self.conn.commit()

    def snapshots(self, version_id: str) -> list[dict]:
        """取某逻辑版本/决策绑定的全部物理快照 (可复现依据)。"""
        rows = self._exec(
            "SELECT role, table_name, table_uri, lance_version FROM dataset_snapshot "
            "WHERE version_id=?", (version_id,)).fetchall()
        return [{"role": r[0], "table_name": r[1], "table_uri": r[2], "lance_version": r[3]}
                for r in rows]

    def register(self, version_id: str, kind: str, manifest: dict, snapshots: list = None):
        checked = self._exec("SELECT 1 FROM decontam_log WHERE version_id=?", (version_id,)).fetchone()
        if not checked:
            raise PermissionError(f"{version_id}: 无 decontam 记录, 拒绝注册 (README §10)")
        sql = ("INSERT INTO dataset_version VALUES (?,?,?,?) "
               "ON CONFLICT (version_id) DO UPDATE SET kind=EXCLUDED.kind, "
               "created_ts=EXCLUDED.created_ts, manifest=EXCLUDED.manifest") if self.is_pg else \
              "INSERT OR REPLACE INTO dataset_version VALUES (?,?,?,?)"
        self._exec(sql, (version_id, kind, int(time.time()), json.dumps(manifest, ensure_ascii=False)))
        self.conn.commit()
        for s in (snapshots or []):
            self.bind_snapshot(version_id, s.role, s.name, s.uri, s.version)

    def add_lineage(self, child_kind: str, child_id: str, parent_kind: str, parent_id: str):
        sql = ("INSERT INTO lineage_edge VALUES (?,?,?,?) ON CONFLICT DO NOTHING") if self.is_pg else \
              "INSERT OR IGNORE INTO lineage_edge VALUES (?,?,?,?)"
        self._exec(sql, (child_kind, child_id, parent_kind, parent_id))
        self.conn.commit()

    def trace(self, child_kind: str, child_id: str) -> list[tuple[str, str]]:
        out, frontier = [], [(child_kind, child_id)]
        while frontier:
            k, i = frontier.pop()
            for pk, pi in self._exec(
                    "SELECT parent_kind, parent_id FROM lineage_edge WHERE child_kind=? AND child_id=?",
                    (k, i)).fetchall():
                out.append((pk, pi))
                frontier.append((pk, pi))
        return out

    def save_ablation(self, decision_id: str, diff: str, score_a: float, score_b: float,
                      significant: bool, conclusion: str, manifest_a: str, manifest_b: str,
                      snap_a=None, snap_b=None):
        sql = ("INSERT INTO ablation_report VALUES (?,?,?,?,?,?,?,?,?) "
               "ON CONFLICT (decision_id) DO UPDATE SET config_diff=EXCLUDED.config_diff, "
               "score_a=EXCLUDED.score_a, score_b=EXCLUDED.score_b, significant=EXCLUDED.significant, "
               "conclusion=EXCLUDED.conclusion, manifest_a=EXCLUDED.manifest_a, "
               "manifest_b=EXCLUDED.manifest_b, ts=EXCLUDED.ts") if self.is_pg else \
              "INSERT OR REPLACE INTO ablation_report VALUES (?,?,?,?,?,?,?,?,?)"
        self._exec(sql, (decision_id, diff, score_a, score_b, int(significant),
                         conclusion, manifest_a, manifest_b, int(time.time())))
        self.conn.commit()
        if snap_a:
            self.bind_snapshot(decision_id, "arm_a", snap_a.name, snap_a.uri, snap_a.version)
        if snap_b:
            self.bind_snapshot(decision_id, "arm_b", snap_b.name, snap_b.uri, snap_b.version)
