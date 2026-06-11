"""湖表存储抽象 (README §7): 统一用 Lance, 本地与分布式同一套代码。

调用方只用表名 (doc_raw / turn_candidate ...), 不关心物理位置:
    lake = Lake(cfg)
    v = lake.write("doc_raw", rows, DOC_RAW, partition="dump_id")  # 返回物理 version
    rows = lake.read("doc_raw")               # 最新快照
    rows = lake.read("doc_raw", version=v)    # 时间旅行历史快照
    snap = lake.snapshot("doc_raw")           # (name, uri, version) 三元组, 供 registry 绑定

切换 mini ↔ cluster 只改 configs: data_root(本地目录 ↔ s3:// 等 URI) +
storage_options(对象存储凭证)。Lance 版本与 registry 的结合见 docs/design_lance_registry.md。
"""
import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from . import io as _io
from .config import REPO_ROOT


def _is_uri(s: str) -> bool:
    return "://" in str(s)


_TAG_RE = re.compile(r"[^A-Za-z0-9_-]")


def sanitize_tag(s: str) -> str:
    """Lance tag 名仅允许字母数字与 - _。"""
    return _TAG_RE.sub("-", str(s))


@dataclass(frozen=True)
class Snapshot:
    """一张物理表在某 version 上的不可变快照引用 (registry ↔ Lance 的桥)。"""
    name: str
    uri: str
    version: int
    role: str = "output"

    def as_role(self, role: str) -> "Snapshot":
        return Snapshot(self.name, self.uri, self.version, role)


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
        return self.latest_version(name) is not None

    # ---- 写 / 读 ----
    def write(self, name: str, rows: list[dict], schema: pa.Schema, partition: str | None = None) -> int:
        return _io.write_table(rows, schema, self.uri(name), self.storage_options)

    def read(self, name: str, version: int | None = None) -> list[dict]:
        return _io.read_table(self.uri(name), self.storage_options, version=version)

    def read_daft(self, name: str, version: int | None = None):
        return _io.read_daft(self.uri(name), self.storage_options, version=version)

    # ---- 版本 / 快照 ----
    def latest_version(self, name: str) -> int | None:
        return _io.latest_version(self.uri(name), self.storage_options)

    def snapshot(self, name: str, version: int | None = None, role: str = "output") -> Snapshot:
        v = version if version is not None else self.latest_version(name)
        return Snapshot(name, self.uri(name), v, role)

    def tag(self, name: str, tag: str, version: int):
        """给物理 version 打人类可读标签 (registry 逻辑版本的镜像), 失败不阻断。"""
        import lance
        ds = lance.dataset(self.uri(name), storage_options=self.storage_options)
        t = sanitize_tag(tag)
        try:
            ds.tags.create(t, version)
        except Exception:
            try:
                ds.tags.update(t, version)
            except Exception:
                pass

    def cleanup(self, name: str, older_than_days: float = 7.0):
        """回收旧 version (受 registry 引用的不应在此清理, 由调用方先行筛除)。"""
        import datetime as _dt
        import lance
        ds = lance.dataset(self.uri(name), storage_options=self.storage_options)
        ds.cleanup_old_versions(older_than=_dt.timedelta(days=older_than_days))
