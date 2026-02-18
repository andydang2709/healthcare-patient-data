import sqlite3

import pandas as pd

from config import DB_PATH, DATA_PROCESSED


def ensure_observation_union(conn: sqlite3.Connection) -> None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='observation_union';"
    ).fetchone()
    if exists is None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS observation_union AS
            SELECT * FROM observation
            UNION ALL
            SELECT * FROM observation_1;
            """
        )
        conn.commit()


def export_observation(
    db_path: str = DB_PATH,
    output_csv: str = None,
) -> None:
    if output_csv is None:
        output_csv = str(DATA_PROCESSED / "observation.csv")
    conn = sqlite3.connect(db_path)
    ensure_observation_union(conn)
    df = pd.read_sql_query(
        """
        SELECT
            CASE WHEN `subject.reference` IS NULL THEN NULL
                 ELSE substr(`subject.reference`, instr(`subject.reference`, 'Patient/') + 8)
            END AS patient_id,
            CASE WHEN `encounter.reference` IS NULL THEN NULL
                 ELSE substr(`encounter.reference`, instr(`encounter.reference`, 'Encounter/') + 10)
            END AS encounter_id,
            `effectiveDateTime` AS observation_date,
            `category[0].coding[0].display` AS observation_category,
            `code.coding[0].display` AS observation_name,
            COALESCE(
                `valueQuantity.value` || CASE WHEN `valueQuantity.unit` IS NOT NULL THEN ' ' || `valueQuantity.unit` END,
                `valueCodeableConcept.text`,
                `valueCodeableConcept.coding[0].display`,
                `valueString`
            ) AS value,
            `component[0].code.coding[0].display` AS component_0_name,
            COALESCE(
                `component[0].valueQuantity.value` || CASE WHEN `component[0].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[0].valueQuantity.unit` END,
                `component[0].valueCodeableConcept.text`,
                `component[0].valueCodeableConcept.coding[0].display`
            ) AS component_0_value,
            `component[1].code.coding[0].display` AS component_1_name,
            COALESCE(
                `component[1].valueQuantity.value` || CASE WHEN `component[1].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[1].valueQuantity.unit` END,
                `component[1].valueCodeableConcept.text`,
                `component[1].valueCodeableConcept.coding[0].display`,
                `component[1].valueString`
            ) AS component_1_value,
            `component[2].code.coding[0].display` AS component_2_name,
            COALESCE(
                `component[2].valueQuantity.value` || CASE WHEN `component[2].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[2].valueQuantity.unit` END,
                `component[2].valueCodeableConcept.text`,
                `component[2].valueCodeableConcept.coding[0].display`
            ) AS component_2_value,
            `component[3].code.coding[0].display` AS component_3_name,
            COALESCE(
                `component[3].valueQuantity.value` || CASE WHEN `component[3].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[3].valueQuantity.unit` END,
                `component[3].valueCodeableConcept.text`,
                `component[3].valueCodeableConcept.coding[0].display`
            ) AS component_3_value,
            `component[4].code.coding[0].display` AS component_4_name,
            COALESCE(
                `component[4].valueQuantity.value` || CASE WHEN `component[4].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[4].valueQuantity.unit` END,
                `component[4].valueCodeableConcept.text`,
                `component[4].valueCodeableConcept.coding[0].display`
            ) AS component_4_value,
            `component[5].code.coding[0].display` AS component_5_name,
            COALESCE(
                `component[5].valueQuantity.value` || CASE WHEN `component[5].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[5].valueQuantity.unit` END,
                `component[5].valueCodeableConcept.text`,
                `component[5].valueCodeableConcept.coding[0].display`
            ) AS component_5_value,
            `component[6].code.coding[0].display` AS component_6_name,
            COALESCE(
                `component[6].valueQuantity.value` || CASE WHEN `component[6].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[6].valueQuantity.unit` END,
                `component[6].valueCodeableConcept.text`,
                `component[6].valueCodeableConcept.coding[0].display`
            ) AS component_6_value,
            `component[7].code.coding[0].display` AS component_7_name,
            COALESCE(
                `component[7].valueQuantity.value` || CASE WHEN `component[7].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[7].valueQuantity.unit` END,
                `component[7].valueCodeableConcept.text`,
                `component[7].valueCodeableConcept.coding[0].display`
            ) AS component_7_value,
            `component[8].code.coding[0].display` AS component_8_name,
            `component[8].valueQuantity.value` || CASE WHEN `component[8].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[8].valueQuantity.unit` END AS component_8_value,
            `component[9].code.coding[0].display` AS component_9_name,
            COALESCE(
                `component[9].valueQuantity.value` || CASE WHEN `component[9].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[9].valueQuantity.unit` END,
                `component[9].valueCodeableConcept.text`,
                `component[9].valueCodeableConcept.coding[0].display`
            ) AS component_9_value,
            `component[10].code.coding[0].display` AS component_10_name,
            COALESCE(
                `component[10].valueQuantity.value` || CASE WHEN `component[10].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[10].valueQuantity.unit` END,
                `component[10].valueCodeableConcept.text`,
                `component[10].valueCodeableConcept.coding[0].display`
            ) AS component_10_value,
            `component[11].code.coding[0].display` AS component_11_name,
            COALESCE(
                `component[11].valueCodeableConcept.text`,
                `component[11].valueCodeableConcept.coding[0].display`
            ) AS component_11_value,
            `component[12].code.coding[0].display` AS component_12_name,
            `component[12].valueString` AS component_12_value,
            `component[13].code.coding[0].display` AS component_13_name,
            COALESCE(
                `component[13].valueCodeableConcept.text`,
                `component[13].valueCodeableConcept.coding[0].display`
            ) AS component_13_value,
            `component[14].code.coding[0].display` AS component_14_name,
            COALESCE(
                `component[14].valueCodeableConcept.text`,
                `component[14].valueCodeableConcept.coding[0].display`
            ) AS component_14_value,
            `component[15].code.coding[0].display` AS component_15_name,
            `component[15].valueQuantity.value` || CASE WHEN `component[15].valueQuantity.unit` IS NOT NULL THEN ' ' || `component[15].valueQuantity.unit` END AS component_15_value,
            `component[16].code.coding[0].display` AS component_16_name,
            COALESCE(
                `component[16].valueCodeableConcept.text`,
                `component[16].valueCodeableConcept.coding[0].display`
            ) AS component_16_value,
            `component[17].code.coding[0].display` AS component_17_name,
            COALESCE(
                `component[17].valueCodeableConcept.text`,
                `component[17].valueCodeableConcept.coding[0].display`
            ) AS component_17_value,
            `component[18].code.coding[0].display` AS component_18_name,
            COALESCE(
                `component[18].valueCodeableConcept.text`,
                `component[18].valueCodeableConcept.coding[0].display`
            ) AS component_18_value,
            `component[19].code.coding[0].display` AS component_19_name,
            COALESCE(
                `component[19].valueCodeableConcept.text`,
                `component[19].valueCodeableConcept.coding[0].display`
            ) AS component_19_value,
            `component[20].code.coding[0].display` AS component_20_name,
            COALESCE(
                `component[20].valueCodeableConcept.text`,
                `component[20].valueCodeableConcept.coding[0].display`
            ) AS component_20_value
        FROM observation_union;
        """,
        conn,
    )
    conn.close()

    df.to_csv(output_csv, index=False)
    print(f"Wrote {len(df)} rows to {output_csv}")


if __name__ == "__main__":
    export_observation()
