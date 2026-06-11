"""M1 warc-ingest: WARC 分片 → doc_raw 表 (README §3.1)。"""
import glob
import hashlib
from pathlib import Path

from warcio.archiveiterator import ArchiveIterator

from common.config import resolve
from common.lake import Lake
from schemas.tables import DOC_RAW
from .extractor import extract


def run(cfg: dict) -> Path:
    m = cfg["m1_warc_ingest"]
    rows = []
    for warc_path in sorted(glob.glob(str(resolve(cfg, m["warc_glob"])))):
        with open(warc_path, "rb") as fh:
            it = ArchiveIterator(fh)
            for record in it:
                if record.rec_type != "response":
                    continue
                ctype = record.http_headers.get_header("Content-Type", "") if record.http_headers else ""
                if "text/html" not in ctype:
                    continue
                url = record.rec_headers.get_header("WARC-Target-URI")
                html = record.content_stream().read().decode("utf-8", errors="replace")
                offset = it.get_record_offset()
                text, extractor = extract(html, m["extractor"])
                if not text:
                    continue
                rows.append({
                    "doc_id": hashlib.sha1(f"{url}{offset}".encode()).hexdigest(),
                    "url": url,
                    "warc_file": Path(warc_path).name,
                    "warc_offset": offset,
                    "dump_id": m["dump_id"],
                    "text": text,
                    "extractor": extractor,
                    "fetch_ts": 1750000000000,
                })
    Lake(cfg).write("doc_raw", rows, DOC_RAW, partition="dump_id")
    return "doc_raw"
