import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def export_encounter(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "encounter.csv")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            CASE
                WHEN `subject.reference` IS NULL THEN NULL
                ELSE substr(`subject.reference`, instr(`subject.reference`, 'Patient/') + 8)
            END AS patient_id,
            id AS encounter_id,
            `type[0].coding[0].display` AS encounter_type,
            `period.start` AS encounter_start_date,
            `period.end` AS encounter_end_date,
            ROUND(
                (julianday(substr(`period.end`, 1, 19))
               - julianday(substr(`period.start`, 1, 19))) * 24,
                2
            ) AS encounter_length_hours,
            COALESCE(`reasonCode[0].coding[0].display`, `type[0].coding[0].display`) AS encounter_reason
        FROM encounter
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_encounter()
