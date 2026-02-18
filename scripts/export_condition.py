import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def export_condition(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "condition.csv")
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
            id AS condition_id,
            `code.text` AS condition,
            CASE
                WHEN `clinicalStatus.coding[0].code` IS 'resolved' THEN True
                ELSE False
            END AS condition_resolved,
            `onsetDateTime` AS condition_onset_date,
            `abatementDateTime` AS condition_abatement_date
        FROM
            condition
        WHERE
            `verificationStatus.coding[0].code` = 'confirmed'
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_condition()
