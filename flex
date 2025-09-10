import pandas as pd

# Load input CSV
input_file = "input.csv"
df = pd.read_csv(input_file)

# List of table columns (all except DesiredColumns)
tables = [c for c in df.columns if c != "DesiredColumns"]

# Get all unique DesiredColumns
desired_cols = df["DesiredColumns"].unique()

# 1️⃣ Build the matrix
matrix_rows = []
for col in desired_cols:
    row = {"DesiredColumn": col}
    for table in tables:
        # Check if col exists in any row for this table
        if col in df[table].values:
            row[table] = col
        else:
            row[table] = ""
    matrix_rows.append(row)

matrix_df = pd.DataFrame(matrix_rows)
matrix_df.to_csv("matrix_output.csv", index=False)

# 2️⃣ Find common columns across all tables
common_cols = []
for col in desired_cols:
    if all(col in df[table].values for table in tables):
        common_cols.append(col)

common_df = pd.DataFrame(common_cols, columns=["CommonColumns"])
common_df.to_csv("common_columns.csv", index=False)

print("✅ Outputs generated: matrix_output.csv, common_columns.csv")
