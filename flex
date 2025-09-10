import pandas as pd

# Input CSV file
input_file = "input.csv"
df = pd.read_csv(input_file)

# Extract desired columns list
desired_cols = df['DesiredColumns'].unique()

# 1️⃣ Inverted mapping: which tables contain which column
inverted = {}
for col in desired_cols:
    tables = [table for table in df.columns[:-1] if df[df['DesiredColumns']==col][table].notna().any()]
    inverted[col] = tables

# Save inverted mapping
inverted_df = pd.DataFrame([(col, ",".join(tables)) for col, tables in inverted.items()],
                           columns=["DesiredColumn", "TablesFoundIn"])
inverted_df.to_csv("inverted_mapping.csv", index=False)

# 2️⃣ Common columns across all tables
common_cols = [col for col, tables in inverted.items() if len(tables) == len(df.columns)-1]
pd.DataFrame(common_cols, columns=["CommonColumns"]).to_csv("common_columns.csv", index=False)

# 3️⃣ Matrix CSV (desired_column vs tables)
matrix = []
for col in desired_cols:
    row = {"DesiredColumn": col}
    for table in df.columns[:-1]:
        # keep the column value if exists, else blank
        row[table] = col if df[df["DesiredColumns"]==col][table].notna().any() else ""
    matrix.append(row)

matrix_df = pd.DataFrame(matrix)
matrix_df.to_csv("matrix_output.csv", index=False)

print("✅ Outputs generated: inverted_mapping.csv, common_columns.csv, matrix_output.csv")
