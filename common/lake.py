"""湖表存储抽象 (README §7): 按 cfg["storage"] 在本地 Lance 与 集群 Paimon 间分发。

调用方只用表名 (doc_raw / turn_candidate ...), 不关心物理后端:
    lake = Lake(cfg)
    lake.write("doc_raw", rows, DOC_RAW, partition="dump_id")
    rows = lake.read("doc_raw")
    df   = lake.read_daft("doc_raw")   # 需要 Daft 算子下推时

切换 mini ↔ cluster 只改 configs: storage(lance|paimon) + data_root(本地目录|s3://)。
计算引擎 (Daft native / ray runner) 已由 common.config.load_profile 按 runner 配置设定。
"""
from pathlib import Path

import pyarrow as pa

from . import io as _io
from .config import REPO_ROOT


def _is_uri(s: str) -> bool:
    return "://" in str(s)


class Lake:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.storage = cfg.get("storage", "lance")
        self.data_root = str(cfg["data_root"])

    def uri(self, name: str) -> str:
        if _is_uri(self.data_root):
            return f"{self.data_root.rstrip('/')}/{name}"
        root = Path(self.data_root)
        if not root.is_absolute():
            root = Path(self.cfg.get("_root", REPO_ROOT)) / root
        return str(root / name)

    def exists(self, name: str) -> bool:
        if self.storage == "lance":
            return Path(self.uri(name)).exists()
        return True  # 远端表存在性交由读取时判定

    # ---- 写 ----
    def write(self, name: str, rows: list[dict], schema: pa.Schema, partition: str | None = None):
        if self.storage == "paimon":
            return self._write_paimon(name, rows, schema, partition)
        return self._write_lance(name, rows, schema)

    def _write_lance(self, name: str, rows: list[dict], schema: pa.Schema):
        _io.write_table(rows, schema, Path(self.uri(name)))

    def _write_paimon(self, name: str, rows: list[dict], schema: pa.Schema, partition: str | None):
        import daft
        from .paimon import paimon_table
        table = paimon_table(self.cfg, name, schema, partition, create=True)
        daft.from_arrow(pa.Table.from_pylist(rows, schema=schema)).write_paimon(table, mode="overwrite")

    # ---- 读 ----
    def read(self, name: str) -> list[dict]:
        if self.storage == "paimon":
            return self.read_daft(name).to_pylist()
        return _io.read_table(Path(self.uri(name)))

    def read_daft(self, name: str):
        if self.storage == "paimon":
            import daft
            from .paimon import paimon_table
            return daft.read_paimon(paimon_table(self.cfg, name))
        return _io.read_daft(Path(self.uri(name)))
