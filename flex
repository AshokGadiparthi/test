Got it 👍 — thanks for clarifying.

You don’t want just a filter; you want a full outer join–style comparison (like SQL FULL OUTER JOIN) between:

this.dataFromParent (big table, has source_table and smp_down_pricing_plans)

this.vendorsFromParent (smaller table, has only keys: cust_id, acct_num, cust_line_seq_id)



and then highlight differences across all values.

🔑 Approach in TypeScript


We can simulate a SQL join by:

Building a combined key = cust_id|acct_num|cust_line_seq_id.

Putting both lists into a Map keyed by that composite key.

Producing an output that includes:

Rows existing in both

Rows existing only in dataFromParent

Rows existing only in vendorsFromParent

Adding highlight logic based on your rules.

✅ TypeScript Code (Join Simulation)
interface ParentData {
  cust_id: string;
  acct_num: number;
  cust_line_seq_id: number;
  source_table: string;
  smp_down_pricing_plans: string | null;
}

interface VendorData {
  cust_id: string;
  acct_num: number;
  cust_line_seq_id: number;
}

interface JoinedRow {
  key: string;
  dataRow?: ParentData;
  vendorRow?: VendorData;
  highlight?: "red" | "yellow" | "none";
}

function joinAndCompare(
  dataFromParent: ParentData[],
  vendorsFromParent: VendorData[]
): JoinedRow[] {
  const map = new Map<string, JoinedRow>();

  // Insert all from dataFromParent
  for (const d of dataFromParent) {
    const key = `${d.cust_id}|${d.acct_num}|${d.cust_line_seq_id}`;
    if (!map.has(key)) {
      map.set(key, { key, dataRow: d });
    } else {
      // if multiple data rows with same key (different source_table)
      // keep as array if needed, but here we just overwrite/add
      const existing = map.get(key)!;
      existing.dataRow = d;
    }
  }

  // Insert all from vendorsFromParent
  for (const v of vendorsFromParent) {
    const key = `${v.cust_id}|${v.acct_num}|${v.cust_line_seq_id}`;
    if (!map.has(key)) {
      map.set(key, { key, vendorRow: v });
    } else {
      map.get(key)!.vendorRow = v;
    }
  }

  // Build results with highlight rules
  const results: JoinedRow[] = [];
  for (const [key, row] of map) {
    const { dataRow, vendorRow } = row;

    if (dataRow && vendorRow) {
      // Both exist, apply highlight rules
      if (dataRow.source_table === "real_line_nrt" && dataRow.smp_down_pricing_plans !== null) {
        row.highlight = "red";
      } else if (dataRow.source_table === "cust_line_profile" && dataRow.smp_down_pricing_plans !== null) {
        row.highlight = "yellow";
      } else {
        row.highlight = "none";
      }
    } else {
      // Exists only in one side
      row.highlight = "none";
    }

    results.push(row);
  }

  return results;
}
🔄 Example Usage
const joined = joinAndCompare(this.dataFromParent, this.vendorsFromParent);

console.table(joined);
/*
[
  {
    key: "11111111|1|501",
    dataRow: { ... },
    vendorRow: { ... },
    highlight: "red" | "yellow" | "none"
  },
  {
    key: "22222222|1|222",
    dataRow: { ... },
    vendorRow: undefined,
    highlight: "none"
  }
]
*/
👉 This works like a SQL FULL OUTER JOIN and ensures all rows from both lists are preserved.
