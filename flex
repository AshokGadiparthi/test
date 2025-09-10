import pandas as pd

# Load input CSV
input_file = "input.csv"
df = pd.read_csv(input_file)

# List of table columns (all except DesiredColumns)
tables = [c for c in df.columns if c != "DesiredColumns"]

# Get all unique DesiredColumns
desired_cols = df["DesiredColumns"].unique()

# Build SQL-friendly matrix with commas and no quotes
sql_matrix = []

for col in desired_cols:
    row = {"DesiredColumn": f"{col},"}  # First column as-is + comma
    for table in tables:
        if col in df[table].values:
            row[table] = f"CAST({col} AS STRING) AS {col},"
        else:
            row[table] = f"NULL AS {col},"
    sql_matrix.append(row)

sql_matrix_df = pd.DataFrame(sql_matrix)

# Save to CSV
sql_matrix_df.to_csv("sql_matrix_output.csv", index=False)

print("✅ SQL matrix generated: sql_matrix_output.csv")
