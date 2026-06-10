"""registry CLI: dataloop-registry versions|trace|ablations"""
import argparse
import sys

from common.config import load_profile, resolve
from .db import Registry


def main(argv=None):
    ap = argparse.ArgumentParser("dataloop-registry")
    ap.add_argument("cmd", choices=["versions", "trace", "ablations"])
    ap.add_argument("--kind"), ap.add_argument("--id")
    ap.add_argument("--profile", default="configs/mini.yaml")
    a = ap.parse_args(argv)
    cfg = load_profile(a.profile)
    reg = Registry(resolve(cfg, cfg["registry_db"]))
    if a.cmd == "versions":
        for row in reg.conn.execute("SELECT version_id, kind, created_ts FROM dataset_version"):
            print(*row, sep="\t")
    elif a.cmd == "trace":
        if not (a.kind and a.id):
            sys.exit("trace 需要 --kind --id")
        for pk, pi in reg.trace(a.kind, a.id):
            print(pk, pi, sep="\t")
    else:
        for row in reg.conn.execute("SELECT decision_id, score_a, score_b, conclusion FROM ablation_report"):
            print(*row, sep="\t")


if __name__ == "__main__":
    main()
