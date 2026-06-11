from pathlib import Path
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_profile(path: str | Path = "configs/mini.yaml") -> dict:
    p = Path(path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    cfg = yaml.safe_load(p.read_text())
    cfg["_root"] = str(REPO_ROOT)
    _set_runner(cfg.get("runner", "native"))
    return cfg


def _set_runner(runner: str):
    import daft
    try:
        if runner == "ray":
            daft.set_runner_ray()
        else:
            daft.set_runner_native()
    except Exception:
        pass  # runner 只能设置一次, 重复调用忽略


def resolve(cfg: dict, rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else Path(cfg["_root"]) / rel
