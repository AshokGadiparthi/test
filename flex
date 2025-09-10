import pandas as pd

# Load input CSV
input_file = "input.csv"
df = pd.read_csv(input_file)

# List of table columns (all except DesiredColumns)
tables = [c for c in df.columns if c != "DesiredColumns"]

# Get all unique DesiredColumns
desired_cols = df["DesiredColumns"].unique()

# 1️⃣ Build SQL-friendly matrix
sql_matrix = []

for col in desired_cols:
    row = {"DesiredColumn": col}  # First column stays as-is
    for table in tables:
        if col in df[table].values:
            # Existing column: CAST as STRING
            row[table] = f"CAST('{col}' AS STRING) AS {col}"
        else:
            # Missing column: NULL AS col_name
            row[table] = f"NULL AS {col}"
    sql_matrix.append(row)

sql_matrix_df = pd.DataFrame(sql_matrix)

# Save to CSV (or you can directly export to SQL generation)
sql_matrix_df.to_csv("sql_matrix_output.csv", index=False)

print("✅ SQL matrix generated: sql_matrix_output.csv")
