"""Paimon 目录/表句柄构造 (cluster profile)。

仅在 storage=paimon 时被惰性导入; 需要 pypaimon + 集群环境 (OSS/S3 warehouse),
本地 mini (storage=lance) 不依赖本模块。catalog 与 IO 凭证全部来自 configs:

    storage: paimon
    data_root: s3://data-loop/lake          # 作为 Paimon warehouse
    paimon:
      catalog: filesystem | hive
      options: {fs.oss.endpoint: ..., fs.oss.accessKeyId: ...}
"""
from functools import lru_cache


@lru_cache(maxsize=None)
def _catalog(warehouse: str, options_items: tuple):
    from pypaimon import Catalog
    return Catalog.create({"warehouse": warehouse, **dict(options_items)})


def _arrow_to_paimon_schema(schema, partition):
    from pypaimon import Schema
    return Schema.from_arrow(schema, partition_keys=[partition] if partition else None)


def paimon_table(cfg: dict, name: str, schema=None, partition: str | None = None, create: bool = False):
    pc = cfg.get("paimon", {}) or {}
    catalog = _catalog(str(cfg["data_root"]), tuple(sorted((pc.get("options") or {}).items())))
    identifier = f"{pc.get('database', 'data_loop')}.{name}"
    if create and schema is not None:
        catalog.create_database(pc.get("database", "data_loop"), ignore_if_exists=True)
        catalog.create_table(identifier, _arrow_to_paimon_schema(schema, partition), ignore_if_exists=True)
    return catalog.get_table(identifier)
