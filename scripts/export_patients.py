import sqlite3

import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config import DB_PATH, DATA_PROCESSED


def export_patients(db_path: str = DB_PATH, output_csv: str = None) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "patients.csv")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        """
        SELECT
            `id` AS patient_id,
            `extension[4].valueAddress.state` AS state,
            `extension[4].valueAddress.country` AS country,
            `birthDate` AS birth_date,
            `deceasedDateTime` AS deceased_date,
            FLOOR((julianday('now') - julianday(`birthDate`)) / 365.25) AS age,
            CASE
                WHEN `extension[0].extension[0].valueCoding.display` = 'Unknown' THEN 'Undeclared'
                ELSE `extension[0].extension[0].valueCoding.display`
            END AS race,
            CASE
                WHEN `extension[1].extension[0].valueCoding.display` IS NOT NULL THEN `extension[1].extension[0].valueCoding.display`
                ELSE NULL
            END AS hispanic,
            `extension[3].valueCode` AS gender,
            `extension[5].valueDecimal` AS disability_adjust_life_years,
            `extension[6].valueDecimal` AS quality_adjusted_life_years,
            `maritalStatus.coding[0].display` AS marital_status,
            CASE
                WHEN `deceasedDateTime` IS NOT NULL THEN True
                ELSE False
            END AS deceased,
            CASE
                WHEN `deceasedDateTime` IS NOT NULL THEN FLOOR((julianday(`deceasedDateTime`) - julianday(`birthDate`)) / 365.25)
            END AS age_at_death,
            CASE
                WHEN `extension[4].valueAddress.country` = 'US' THEN True
                ELSE False
            END AS us_resident,
            CASE
                WHEN `multipleBirthInteger` IS NOT NULL THEN True
                ELSE False
            END AS multiple_birth
        FROM
            patients
        """,
        conn,
    )
    conn.close()

    df_metrics = df[["disability_adjust_life_years", "quality_adjusted_life_years"]].copy()
    df_metrics = df_metrics.fillna(0)

    scaler = MinMaxScaler()
    df_metrics[["DALY_norm", "QALY_norm"]] = scaler.fit_transform(df_metrics)

    df_metrics["DALY_inv"] = 1 - df_metrics["DALY_norm"]
    df_metrics["HealthIndex"] = (df_metrics["DALY_inv"] + df_metrics["QALY_norm"]) / 2

    df["DALY_norm"] = df_metrics["DALY_norm"]
    df["QALY_norm"] = df_metrics["QALY_norm"]
    df["DALY_inv"] = df_metrics["DALY_inv"]
    df["HealthIndex"] = df_metrics["HealthIndex"]

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_patients()
