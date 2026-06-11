"""湖表读写: mini profile 用 Daft 单机 runner + Lance 数据集 (README §7)。

- 计算引擎: Daft native runner (单机), cluster profile 切 Ray runner。
- 存储格式: Lance 数据集 (每张表一个 dataset 目录), cluster 切 Paimon。
- 分区列 (dump_id / dt) 作为普通数据列保留, 可下推过滤; Lance 无 hive 目录分区。
- map 类型 (filter_flags) 需 Lance 文件格式 2.2+。
"""
from pathlib import Path

import daft
import lance
import pyarrow as pa


def write_table(rows: list[dict], schema: pa.Schema, path: Path, partition: str | None = None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    lance.write_dataset(table, str(path), mode="overwrite", data_storage_version="2.2")


def read_table(path: Path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    return daft.read_lance(str(path)).to_pylist()


def read_daft(path: Path) -> "daft.DataFrame":
    """供需要在 Daft DataFrame 上做下推/算子的调用方使用 (单机 runner)。"""
    return daft.read_lance(str(Path(path)))
