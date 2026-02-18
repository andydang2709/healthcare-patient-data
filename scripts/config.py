"""Shared paths for data processing. Resolves project root from script location."""
from pathlib import Path

# Project root: parent of scripts/ directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DB_PATH = str(DATA_PROCESSED / "healthcare.sqlite")
