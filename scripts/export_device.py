import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def export_device(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "device.csv")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            CASE
                WHEN `patient.reference` IS NULL THEN NULL
                ELSE substr(`patient.reference`, instr(`patient.reference`, 'Patient/') + 8)
            END AS patient_id,
            `expirationDate` AS device_expiration_date,
            `deviceName[0].name` AS device_name,
            CASE
                WHEN julianday(substr(`expirationDate`, 1, 19)) < julianday('now') THEN True
                ELSE False
            END AS device_expired
        FROM device;
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_device()
