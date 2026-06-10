"""replay 与 PyFlink 两个 M7 后端输出逐行一致 (语义对齐校验)。"""
import subprocess
import sys

import pytest

pytest.importorskip("pyflink")

from common.config import resolve
from common.io import read_table


def _key(rows):
    return {(r["conversation_id"], r["turn_id"]):
            (r["thumbs"], r["regenerated"], r["stopped"], r["followup_correction"],
             r["user_edit"], r["late"], r["prompt"], r["response"], r["dt"])
            for r in rows}


def test_flink_matches_replay(cfg):
    subprocess.run([sys.executable, str(resolve(cfg, "replay-data/generate.py"))], check=True)
    from online.m7_flink import job, job_flink
    job.run(cfg)
    replay = read_table(resolve(cfg, cfg["data_root"]) / "turn_candidate")
    job_flink.run(cfg)
    flink = read_table(resolve(cfg, cfg["data_root"]) / "turn_candidate")
    assert len(flink) == len(replay) > 0
    assert _key(flink) == _key(replay)
