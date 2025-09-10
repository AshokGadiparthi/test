You have two lists of objects in TypeScript:



Example data:
this.dataFromParent = [
  { cust_id: "11111111", acct_num: 1, cust_line_seq_id: 501, source_table: "real_line_nrt", smp_down_pricing_plans: null },
  { cust_id: "11111111", acct_num: 1, cust_line_seq_id: 501, source_table: "MTZ_adobe", smp_down_pricing_plans: null },
  { cust_id: "11111111", acct_num: 1, cust_line_seq_id: 501, source_table: "cust_line_profile", smp_down_pricing_plans: "N" },
  { cust_id: "22222222", acct_num: 1, cust_line_seq_id: 222, source_table: "cust_line_profile", smp_down_pricing_plans: "X" }
];

this.vendorsFromParent = [
  { cust_id: "11111111", acct_num: 1, cust_line_seq_id: 501 }
];
What you want:
Compare dataFromParent and vendorsFromParent based on

cust_id, acct_num, cust_line_seq_id.

Apply color coding rules:

🔴 Light Red Highlight → Row exists in batch_line_vzw & cust_line_profile,

but values differ in MTZ_adobe vs real_line_nrt.

🟡 Light Yellow Highlight → Row exists in batch_line_vzw & cust_line_profile,

but values differ in MTZ_adobe.

TypeScript solution


Here’s how you can write the comparison logic:

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

function compareLists(
  dataFromParent: ParentData[],
  vendorsFromParent: VendorData[]
) {
  // Filter only rows matching vendor cust_id/acct_num/seq_id
  const filtered = dataFromParent.filter(d =>
    vendorsFromParent.some(v =>
      v.cust_id === d.cust_id &&
      v.acct_num === d.acct_num &&
      v.cust_line_seq_id === d.cust_line_seq_id
    )
  );

  // Group by cust_id + acct_num + seq_id
  const grouped: Record<string, ParentData[]> = {};
  for (const row of filtered) {
    const key = `${row.cust_id}_${row.acct_num}_${row.cust_line_seq_id}`;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(row);
  }

  const results = [];
  for (const key of Object.keys(grouped)) {
    const rows = grouped[key];

    const realLine = rows.find(r => r.source_table === "real_line_nrt");
    const mtzAdobe = rows.find(r => r.source_table === "MTZ_adobe");
    const custProfile = rows.find(r => r.source_table === "cust_line_profile");

    if (realLine && custProfile && mtzAdobe) {
      if (realLine.smp_down_pricing_plans !== mtzAdobe.smp_down_pricing_plans) {
        results.push({ key, highlight: "red", rows });
      } else if (custProfile.smp_down_pricing_plans !== mtzAdobe.smp_down_pricing_plans) {
        results.push({ key, highlight: "yellow", rows });
      }
    }
  }

  return results;
}
Example usage:
const mismatches = compareLists(this.dataFromParent, this.vendorsFromParent);

console.log(mismatches);
/*
[
  {
    key: "11111111_1_501",
    highlight: "red",   // or "yellow"
    rows: [ ...matching rows... ]
  }
]
*/
👉 This gives you the rows grouped by cust_id + acct_num + cust_line_seq_id, with the correct highlight flag (red or yellow) depending on your rule.



Would you like me to also give you a frontend rendering snippet (Angular/React/Vanilla HTML) where these rows actually show up with background colors (light red / light yellow)?
