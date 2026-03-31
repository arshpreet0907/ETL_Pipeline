import csv
import pymysql

DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "root",
    "database": "vehicle_manufacturing_src",
}
tables=[
     "suppliers",
     "vehicles",
     "parts",
     "quality_checks"
]

DELIMITER  = ","

columns=[
    ["supplier_code", "supplier_name", "country", "tier", "rating", "contract_start", "contract_end", "is_active", "created_at", "updated_at" ],
    ["vin", "model_code", "variant", "color_code", "engine_type", "plant_code", "line_number", "production_date", "shift", "status", "quality_score", "weight_kg", "created_at", "updated_at"],
    ["vehicle_id", "part_number", "part_name", "supplier_code", "quantity", "unit_cost", "currency", "install_time_min", "defect_flag", "batch_number", "created_at"],
    ["vehicle_id", "check_date", "inspector_id", "station", "test_type", "result", "defect_code", "rework_hours", "pass_fail", "created_at"]
]

COLUMN_NAMES = None
CSV_FILE = None
TABLE_NAME = None

def insert_all():
    for i in range(4):
        global TABLE_NAME,CSV_FILE,COLUMN_NAMES

        TABLE_NAME=tables[i]
        CSV_FILE = f"csv/{TABLE_NAME}.csv"
        COLUMN_NAMES = columns[i]
        print(f"Loading data into {TABLE_NAME}...")
        print(COLUMN_NAMES)
        insert_from_csv()

def insert_from_csv():
    conn   = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    placeholders = ", ".join(["%s"] * len(COLUMN_NAMES))
    columns      = ", ".join(COLUMN_NAMES)
    sql          = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

    inserted = 0
    skipped  = 0

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=DELIMITER)

        # next(reader)

        for line_num, row in enumerate(reader, start=1):
            row = [v.strip().strip("'\"") for v in row]
            row = [None if v.upper() == "NULL" or v == "" else v for v in row]
            if len(row) != len(COLUMN_NAMES):
                print(f"  [!] Row {line_num} skipped — expected {len(COLUMN_NAMES)} columns, got {len(row)}")
                skipped += 1
                continue
            try:
                cursor.execute(sql, row)
                inserted += 1
            except Exception as e:
                print(f"  [!] Row {line_num} failed: {e}")
                skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n Done — {inserted} row(s) inserted, {skipped} skipped.")

if __name__ == "__main__":
    insert_all()

