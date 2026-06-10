"""M9 registry: 版本 / 血缘 / 去污染记录 / 消融报告 (README §6)。
mini 用 SQLite; cluster 换 Postgres, SQL 兼容。
任何写入下游训练目录的数据集必须先有 decontam 记录, 否则拒绝注册。"""
import json
import sqlite3
import time
from pathlib import Path

_DDL = """
CREATE TABLE IF NOT EXISTS dataset_version (
  version_id TEXT PRIMARY KEY, kind TEXT, created_ts INTEGER, manifest TEXT);
CREATE TABLE IF NOT EXISTS lineage_edge (
  child_kind TEXT, child_id TEXT, parent_kind TEXT, parent_id TEXT,
  UNIQUE(child_kind, child_id, parent_kind, parent_id));
CREATE TABLE IF NOT EXISTS decontam_log (
  version_id TEXT, item_id TEXT, reason TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS ablation_report (
  decision_id TEXT PRIMARY KEY, config_diff TEXT, score_a REAL, score_b REAL,
  significant INTEGER, conclusion TEXT, manifest_a TEXT, manifest_b TEXT, ts INTEGER);
CREATE INDEX IF NOT EXISTS idx_lineage_child ON lineage_edge(child_kind, child_id);
"""


class Registry:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.executescript(_DDL)

    def log_decontam(self, version_id: str, item_id: str, reason: str):
        self.conn.execute("INSERT INTO decontam_log VALUES (?,?,?,?)",
                          (version_id, item_id, reason, int(time.time())))
        self.conn.commit()

    def register(self, version_id: str, kind: str, manifest: dict):
        checked = self.conn.execute(
            "SELECT 1 FROM decontam_log WHERE version_id=?", (version_id,)).fetchone()
        if not checked:
            raise PermissionError(f"{version_id}: 无 decontam 记录, 拒绝注册 (README §10)")
        self.conn.execute("INSERT OR REPLACE INTO dataset_version VALUES (?,?,?,?)",
                          (version_id, kind, int(time.time()), json.dumps(manifest, ensure_ascii=False)))
        self.conn.commit()

    def add_lineage(self, child_kind: str, child_id: str, parent_kind: str, parent_id: str):
        self.conn.execute("INSERT OR IGNORE INTO lineage_edge VALUES (?,?,?,?)",
                          (child_kind, child_id, parent_kind, parent_id))
        self.conn.commit()

    def trace(self, child_kind: str, child_id: str) -> list[tuple[str, str]]:
        out, frontier = [], [(child_kind, child_id)]
        while frontier:
            k, i = frontier.pop()
            for pk, pi in self.conn.execute(
                    "SELECT parent_kind, parent_id FROM lineage_edge WHERE child_kind=? AND child_id=?",
                    (k, i)):
                out.append((pk, pi))
                frontier.append((pk, pi))
        return out

    def save_ablation(self, decision_id: str, diff: str, score_a: float, score_b: float,
                      significant: bool, conclusion: str, manifest_a: str, manifest_b: str):
        self.conn.execute("INSERT OR REPLACE INTO ablation_report VALUES (?,?,?,?,?,?,?,?,?)",
                          (decision_id, diff, score_a, score_b, int(significant),
                           conclusion, manifest_a, manifest_b, int(time.time())))
        self.conn.commit()
