import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def export_allergyintolerance(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "allergyintolerance.csv")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            CASE
                WHEN `patient.reference` IS NULL THEN NULL
                ELSE substr(`patient.reference`, instr(`patient.reference`, 'Patient/') + 8)
            END AS patient_id,
            `type` AS allergy_type,
            `category[0]` AS allergy_category,
            `recordedDate` AS allergy_date,
            `code.coding[0].display` AS allergy_name,
            CASE
                WHEN `reaction[0].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[0].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[0].severity`, 'unknown')
            END AS allergy_reaction_1,
            CASE
                WHEN `reaction[1].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[1].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[1].severity`, 'unknown')
            END AS allergy_reaction_2,
            CASE
                WHEN `reaction[2].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[2].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[2].severity`, 'unknown')
            END AS allergy_reaction_3,
            CASE
                WHEN `reaction[3].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[3].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[3].severity`, 'unknown')
            END AS allergy_reaction_4,
            CASE
                WHEN `reaction[4].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[4].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[4].severity`, 'unknown')
            END AS allergy_reaction_5,
            CASE
                WHEN `reaction[5].manifestation[0].coding[0].display` IS NULL THEN NULL
                ELSE `reaction[5].manifestation[0].coding[0].display` || ' - ' || COALESCE(`reaction[5].severity`, 'unknown')
            END AS allergy_reaction_6
        FROM
            allergyintolerance
        WHERE
            `clinicalStatus.coding[0].code` = 'active' AND
            `verificationStatus.coding[0].code` = 'confirmed'
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_allergyintolerance()
