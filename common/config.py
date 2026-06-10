from pathlib import Path
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_profile(path: str | Path = "configs/mini.yaml") -> dict:
    p = Path(path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    cfg = yaml.safe_load(p.read_text())
    cfg["_root"] = str(REPO_ROOT)
    return cfg


def resolve(cfg: dict, rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else Path(cfg["_root"]) / rel
