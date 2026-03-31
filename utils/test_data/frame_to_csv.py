import pandas as pd

def writeCleanResults(clean_data):
    with pd.ExcelWriter('assertions/assertion_output/clean_data.xlsx', engine='openpyxl') as writer:
        for sheet_name, df in clean_data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def writeCleanResultsCSV(clean_data):
    for table_name, df in clean_data.items():
        output_path = f"clean_data_csv/{table_name}_clean.csv"
        df.to_csv(output_path, index=False, sep=",", header=False)
        print(f"Saved {len(df)} row(s) to {output_path}")