import pandas as pd

# Input file
input_file = "input.csv"
df = pd.read_csv(input_file)

# Tables = all columns except DesiredColumns
tables = [c for c in df.columns if c != "DesiredColumns"]

# Unique list of desired columns
desired_cols = df["DesiredColumns"].unique()

# 1️⃣ Build the matrix
matrix_rows = []
for col in desired_cols:
    row = {"DesiredColumn": col}
    for table in tables:
        values = df.loc[df["DesiredColumns"] == col, table].dropna().unique()
        # Only put col if it actually exists in that table
        row[table] = col if col in values else ""
    matrix_rows.append(row)

matrix_df = pd.DataFrame(matrix_rows)

# Save matrix output
matrix_df.to_csv("matrix_output.csv", index=False)

# 2️⃣ Find common columns across ALL tables
common_cols = []
for col in desired_cols:
    present = True
    for table in tables:
        values = df.loc[df["DesiredColumns"] == col, table].dropna().unique()
        if col not in values:
            present = False
            break
    if present:
        common_cols.append(col)

common_df = pd.DataFrame(common_cols, columns=["CommonColumns"])
common_df.to_csv("common_columns.csv", index=False)

print("✅ Outputs generated: matrix_output.csv, common_columns.csv")
