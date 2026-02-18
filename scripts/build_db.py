"""Build SQLite database from NDJSON files. Run from project root: python scripts/build_db.py"""
import json
import sqlite3

import pandas as pd

from config import DATA_PROCESSED, DATA_RAW, DB_PATH


def _flatten_value(value, prefix, out):
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten_value(v, f"{prefix}.{k}" if prefix else k, out)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            _flatten_value(v, f"{prefix}[{i}]", out)
        if len(value) == 0:
            out[prefix] = None
    else:
        out[prefix] = value


def flatten_record(record):
    out = {}
    _flatten_value(record, "", out)
    return out


def build_db():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    ndjson_files = sorted(DATA_RAW.glob("*.ndjson"))
    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {DATA_RAW}")

    conn = sqlite3.connect(DB_PATH)
    name_counts = {}

    for path in ndjson_files:
        with path.open("r", encoding="utf-8") as f:
            records = [flatten_record(json.loads(line)) for line in f if line.strip()]

        df = pd.DataFrame(records)
        base_name = path.name.split(".", 1)[0].lower()
        count = name_counts.get(base_name, 0)
        table_name = f"{base_name}_{count}" if count > 0 else base_name
        name_counts[base_name] = count + 1

        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Loaded {len(df)} rows into '{table_name}'")

    # Combine observation files into one union table
    obs_cols = [row[1] for row in conn.execute("PRAGMA table_info(observation);").fetchall()]
    obs1_cols = [row[1] for row in conn.execute("PRAGMA table_info(observation_1);").fetchall()]
    common_cols = [col for col in obs_cols if col in obs1_cols]

    conn.execute("""
        CREATE TABLE IF NOT EXISTS observation_union AS
        SELECT * FROM observation
        UNION ALL
        SELECT * FROM observation_1;
    """)
    conn.commit()
    print(f"Created 'observation_union' with {len(common_cols)} columns")

    conn.close()
    print(f"Database saved to {DB_PATH}")


if __name__ == "__main__":
    build_db()
