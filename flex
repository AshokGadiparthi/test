import java.util.*;
import java.util.stream.Collectors;

public class DataTransformer {

    public static List<CustomRow> transform(List<ParentData> rows) {

        Map<String, List<ParentData>> grouped = rows.stream()
                .collect(Collectors.groupingBy(r -> r.getCustId() + "|" + r.getAcctNum() + "|" + r.getCustLineSeqId()));

        List<CustomRow> result = new ArrayList<>();

        for (Map.Entry<String, List<ParentData>> entry : grouped.entrySet()) {
            List<ParentData> group = entry.getValue();

            String custId = group.get(0).getCustId();
            int acctNum = group.get(0).getAcctNum();
            int custLineSeqId = group.get(0).getCustLineSeqId();

            // Extract cust_line_profile and mtz_adobe
            String profileVal = getValue(group, "cust_line_profile");
            String mtzVal = getValue(group, "mtz_adobe");
            String batchVal = getValue(group, "batch_line_vzw");
            String realVal = getValue(group, "real_line_nrt");

            // Highlight rules
            String highlight = "none";
            if (Objects.equals(batchVal, profileVal) && !Objects.equals(batchVal, mtzVal)) {
                highlight = (realVal == null) ? "light-red" : "light-yellow";
            }

            result.add(new CustomRow(custId, acctNum, custLineSeqId, profileVal, mtzVal, highlight, group));
        }

        return result;
    }

    private static String getValue(List<ParentData> group, String colName) {
        return group.stream()
                .filter(d -> colName.equalsIgnoreCase(d.getColName()))
                .map(ParentData::getColName) // Using colName itself as "value"
                .findFirst()
                .orElse(null);
    }
}



import java.util.List;

public class CustomRow {
    private String custId;
    private int acctNum;
    private int custLineSeqId;
    private String custLineProfile;
    private String mtzAdobe;
    private String highlight;
    private List<ParentData> children;

    public CustomRow(String custId, int acctNum, int custLineSeqId,
                     String custLineProfile, String mtzAdobe,
                     String highlight, List<ParentData> children) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.custLineProfile = custLineProfile;
        this.mtzAdobe = mtzAdobe;
        this.highlight = highlight;
        this.children = children;
    }

    @Override
    public String toString() {
        return custId + "|" + acctNum + "|" + custLineSeqId +
                " cust_line_profile=" + custLineProfile +
                " mtz_adobe=" + mtzAdobe +
                " highlight=" + highlight +
                " children=" + children;
    }
}



public class ParentData {
    private String custId;
    private int acctNum;
    private int custLineSeqId;
    private String colName;
    private String sourceTable;

    public ParentData(String custId, int acctNum, int custLineSeqId, String colName, String sourceTable) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.colName = colName;
        this.sourceTable = sourceTable;
    }

    public String getCustId() { return custId; }
    public int getAcctNum() { return acctNum; }
    public int getCustLineSeqId() { return custLineSeqId; }
    public String getColName() { return colName; }
    public String getSourceTable() { return sourceTable; }

    @Override
    public String toString() {
        return colName + "@" + sourceTable;
    }
}
