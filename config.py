"""Shared paths for data processing. Works when run from project root or notebooks/."""
from pathlib import Path


def get_project_root() -> Path:
    """Find project root by looking for data/raw directory."""
    for p in [Path.cwd(), Path.cwd().parent]:
        if (p / "data" / "raw").exists():
            return p
    return Path.cwd()


PROJECT_ROOT = get_project_root()
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DB_PATH = DATA_PROCESSED / "healthcare.sqlite"
