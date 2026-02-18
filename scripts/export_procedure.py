import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def export_procedure(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "procedure.csv")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            CASE
                WHEN `subject.reference` IS NULL THEN NULL
                ELSE substr(`subject.reference`, instr(`subject.reference`, 'Patient/') + 8)
            END AS patient_id,
            CASE
                WHEN `encounter.reference` IS NULL THEN NULL
                ELSE substr(`encounter.reference`, instr(`encounter.reference`, 'Encounter/') + 10)
            END AS encounter_id,
            CASE
                WHEN `reasonReference[0].reference` IS NULL THEN NULL
                ELSE substr(`reasonReference[0].reference`, instr(`reasonReference[0].reference`, 'Condition/') + 10)
            END AS condition_id,
            status,
            `code.coding[0].display` AS procedure_name,
            `performedPeriod.start` AS procedure_start_date,
            `performedPeriod.end` AS procedure_end_date,
            ROUND(
                (julianday(substr(`performedPeriod.end`, 1, 19))
               - julianday(substr(`performedPeriod.start`, 1, 19))) * 24,
                2
            ) AS procedure_length_hours,
            `reasonCode[0].coding[0].display` AS reason_for_procedure
        FROM
            procedure
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_procedure()
