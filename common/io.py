"""湖表读写: 统一用 Lance 数据集 (本地目录或对象存储 URI), README §7。

- 计算引擎: Daft runner (native 单机 / ray 分布式), 由 common.config 按 runner 设定。
- 存储格式: Lance, 每张表一个 dataset; 本地用目录, 集群用 s3:// 等 URI。
- storage_options: 对象存储凭证 (region/key/endpoint), 来自 configs, 透传给 Lance。
- 分区列 (dump_id / dt) 作为普通数据列保留, 可下推过滤。
- map 类型 (filter_flags) 需 Lance 文件格式 2.2+。
"""
from pathlib import Path

import lance
import pyarrow as pa


def _local_parent(uri: str):
    if "://" not in uri:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)


def write_table(rows: list[dict], schema: pa.Schema, uri: str, storage_options: dict | None = None):
    _local_parent(str(uri))
    table = pa.Table.from_pylist(rows, schema=schema)
    lance.write_dataset(table, str(uri), mode="overwrite",
                        data_storage_version="2.2", storage_options=storage_options)


def read_table(uri: str, storage_options: dict | None = None) -> list[dict]:
    try:
        ds = lance.dataset(str(uri), storage_options=storage_options)
    except (ValueError, OSError, FileNotFoundError):
        return []  # 数据集不存在
    return ds.to_table().to_pylist()


def read_daft(uri: str, storage_options: dict | None = None):
    """供需要在 Daft DataFrame 上做下推/算子的调用方使用。"""
    import daft
    return daft.read_lance(str(uri), storage_options=storage_options)
