"""湖表存储抽象 (README §7): 统一用 Lance, 本地与分布式同一套代码。

调用方只用表名 (doc_raw / turn_candidate ...), 不关心物理位置:
    lake = Lake(cfg)
    lake.write("doc_raw", rows, DOC_RAW, partition="dump_id")
    rows = lake.read("doc_raw")
    df   = lake.read_daft("doc_raw")   # 需要 Daft 算子下推时

切换 mini ↔ cluster 只改 configs: data_root(本地目录 ↔ s3:// 等 URI) +
storage_options(对象存储凭证)。计算引擎 (Daft native/ray runner) 由
common.config.load_profile 按 runner 配置设定。
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
        self.data_root = str(cfg["data_root"])
        self.storage_options = cfg.get("storage_options") or None

    def uri(self, name: str) -> str:
        if _is_uri(self.data_root):
            return f"{self.data_root.rstrip('/')}/{name}"
        root = Path(self.data_root)
        if not root.is_absolute():
            root = Path(self.cfg.get("_root", REPO_ROOT)) / root
        return str(root / name)

    def exists(self, name: str) -> bool:
        if _is_uri(self.data_root):
            return bool(self.read(name))
        return Path(self.uri(name)).exists()

    def write(self, name: str, rows: list[dict], schema: pa.Schema, partition: str | None = None):
        _io.write_table(rows, schema, self.uri(name), self.storage_options)

    def read(self, name: str) -> list[dict]:
        return _io.read_table(self.uri(name), self.storage_options)

    def read_daft(self, name: str):
        return _io.read_daft(self.uri(name), self.storage_options)
