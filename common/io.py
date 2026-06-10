"""湖表读写: mini profile 用本地 Parquet 分区目录模拟 (README §7)。"""
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def write_table(rows: list[dict], schema: pa.Schema, path: Path, partition: str | None = None):
    path = Path(path)
    if partition:
        parts: dict[str, list[dict]] = {}
        for r in rows:
            parts.setdefault(str(r[partition]), []).append(r)
        for key, sub in parts.items():
            d = path / f"{partition}={key}"
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(sub, schema=schema), d / "part-0.parquet")
    else:
        path.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path / "part-0.parquet")


def read_table(path: Path) -> list[dict]:
    files = sorted(Path(path).rglob("*.parquet"))
    rows: list[dict] = []
    for f in files:
        rows.extend(pq.ParquetFile(f).read().to_pylist())
    return rows
