import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CustomRowBuilder {

    public static List<CustomRow> buildFromResultSet(ResultSet rs) throws SQLException {
        List<CustomRow> customRows = new ArrayList<>();
        Map<String, List<ParentData>> grouped = new HashMap<>();

        // 1️⃣ Read ResultSet and group ParentData
        while (rs.next()) {
            String custId = rs.getString("cust_id");
            int acctNum = rs.getInt("acct_num");
            int custLineSeqId = rs.getInt("cust_line_seq_id");
            String colName = rs.getString("col_name");
            String sourceTable = rs.getString("source_table");

            ParentData pd = new ParentData(custId, acctNum, custLineSeqId, colName, sourceTable);

            String key = custId + "|" + acctNum + "|" + custLineSeqId;
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(pd);
        }

        // 2️⃣ Iterate grouped data and build CustomRows
        for (Map.Entry<String, List<ParentData>> entry : grouped.entrySet()) {
            List<ParentData> children = entry.getValue();

            String custId = children.get(0).getCustId();
            int acctNum = children.get(0).getAcctNum();
            int custLineSeqId = children.get(0).getCustLineSeqId();

            // Extract relevant column values
            String custLineProfile = getColumnValue(children, "cust_line_profile");
            String mtzAdobe = getColumnValue(children, "mtz_adobe");
            String batchVal = getColumnValue(children, "batch_line_vzw");
            String realVal = getColumnValue(children, "real_line_nrt");

            // Determine highlight
            String highlight = "none";
            if (Objects.equals(batchVal, custLineProfile) && !Objects.equals(batchVal, mtzAdobe)) {
                highlight = (realVal == null) ? "light-red" : "light-yellow";
            }

            customRows.add(new CustomRow(custId, acctNum, custLineSeqId, custLineProfile, mtzAdobe, highlight, children));
        }

        return customRows;
    }

    private static String getColumnValue(List<ParentData> list, String colName) {
        for (ParentData pd : list) {
            if (colName.equalsIgnoreCase(pd.getColName())) {
                return pd.getColName(); // Using colName as "value"
            }
        }
        return null;
    }
}
