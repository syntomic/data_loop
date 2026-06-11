"""registry CLI: dataloop-registry versions|trace|ablations|snapshots|reproduce|cleanup"""
import argparse
import sys

from dataloop.common.config import load_profile
from .db import Registry


def main(argv=None):
    ap = argparse.ArgumentParser("dataloop-registry")
    ap.add_argument("cmd", choices=["versions", "trace", "ablations", "snapshots", "reproduce", "cleanup"])
    ap.add_argument("--kind"), ap.add_argument("--id")
    ap.add_argument("--load", action="store_true", help="reproduce: 实际读回首表行数验证可还原")
    ap.add_argument("--keep-days", type=float, default=7.0, help="cleanup: 回收超过该天数的旧 version")
    ap.add_argument("--profile", default="configs/mini.yaml")
    a = ap.parse_args(argv)
    cfg = load_profile(a.profile)
    reg = Registry.from_cfg(cfg)

    if a.cmd == "versions":
        for row in reg.query("SELECT version_id, kind, created_ts FROM dataset_version"):
            print(*row, sep="\t")
    elif a.cmd == "trace":
        if not (a.kind and a.id):
            sys.exit("trace 需要 --kind --id")
        for pk, pi in reg.trace(a.kind, a.id):
            print(pk, pi, sep="\t")
    elif a.cmd == "ablations":
        for row in reg.query("SELECT decision_id, score_a, score_b, conclusion FROM ablation_report"):
            print(*row, sep="\t")
    elif a.cmd == "snapshots":
        if not a.id:
            sys.exit("snapshots 需要 --id <version_id>")
        for s in reg.snapshots(a.id):
            print(f"{s['role']}\t{s['table_name']}\t{s['table_uri']}@v{s['lance_version']}")
    elif a.cmd == "reproduce":
        if not a.id:
            sys.exit("reproduce 需要 --id <version_id>")
        from dataloop.common.lake import Lake
        lake = Lake(cfg)
        snaps = reg.snapshots(a.id)
        if not snaps:
            sys.exit(f"{a.id}: 无 snapshot 绑定 (旧版本或未注册)")
        for s in snaps:
            line = f"{s['role']}\t{s['table_name']} @ v{s['lance_version']}\t{s['table_uri']}"
            if a.load:
                rows = lake.read(s["table_name"], version=s["lance_version"])
                line += f"\t→ {len(rows)} rows"
            print(line)
    elif a.cmd == "cleanup":
        from dataloop.common.lake import Lake
        lake = Lake(cfg)
        # registry 引用中的 version 不回收: 只清未被引用且超龄的
        referenced = {(r[0], r[1]) for r in reg.query(
            "SELECT table_name, lance_version FROM dataset_snapshot")}
        tables = {r[0] for r in reg.query("SELECT DISTINCT table_name FROM dataset_snapshot")}
        for t in sorted(tables):
            try:
                lake.cleanup(t, older_than_days=a.keep_days)
                print(f"cleaned {t} (保留被引用 version: "
                      f"{sorted(v for n, v in referenced if n == t)})")
            except Exception as e:
                print(f"skip {t}: {e}")


if __name__ == "__main__":
    main()
