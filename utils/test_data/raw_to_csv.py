import csv
import re

# input_files=["suppliers","vehicles","parts","quality_checks"]
# input_files=["suppliers_xl","vehicles_xl","parts_xl","quality_checks_xl"]
input_files=["suppliers_xs","vehicles_xs","parts_xs","quality_checks_xs"]
csv_files=["suppliers","vehicles","parts","quality_checks"]

INPUT_FILE = ""
RAW_DATA=""
OUTPUT_FILE = ""

def convert_all():
    global OUTPUT_FILE,INPUT_FILE,RAW_DATA

    for i in range(4):
        INPUT_FILE=f"txt/{input_files[i]}.txt"
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            RAW_DATA = f.read()
        OUTPUT_FILE=f"csv/{csv_files[i]}.csv"
        convert_to_csv()


def convert_to_csv():
    rows = re.findall(r"\(([^)]+)\)", RAW_DATA.strip())

    if not rows:
        print("No data found. Make sure your format is (v1,v2,...),(v1,v2,...)")
        return

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            values = [v.strip() for v in row.split(",")]
            writer.writerow(values)

    print(f"Done — {len(rows)} row(s) written to {OUTPUT_FILE}")

if __name__ == "__main__":
    convert_all()
