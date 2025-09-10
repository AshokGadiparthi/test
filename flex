

import java.util.List;

public class CustomRow {
    private String custId;
    private int acctNum;
    private int custLineSeqId;
    private String custLineProfile;
    private String mtzAdobe;
    private String highlight;
    private List<ParentData> allRowsWithSameKey;

    public CustomRow(String custId, int acctNum, int custLineSeqId,
                     String custLineProfile, String mtzAdobe,
                     String highlight, List<ParentData> allRowsWithSameKey) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.custLineProfile = custLineProfile;
        this.mtzAdobe = mtzAdobe;
        this.highlight = highlight;
        this.allRowsWithSameKey = allRowsWithSameKey;
    }

    @Override
    public String toString() {
        return custId + "|" + acctNum + "|" + custLineSeqId +
               " cust_line_profile=" + custLineProfile +
               " mtz_adobe=" + mtzAdobe +
               " highlight=" + highlight +
               " allRows=" + allRowsWithSameKey;
    }
}


public class ParentData {
    private String custId;
    private int acctNum;
    private int custLineSeqId;
    private String sourceTable;
    private String smpDownPricingPlans;
    private String highlight; // new column

    public ParentData(String custId, int acctNum, int custLineSeqId, String sourceTable, String smpDownPricingPlans) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.sourceTable = sourceTable;
        this.smpDownPricingPlans = smpDownPricingPlans;
        this.highlight = "none";
    }

    public String getCustId() { return custId; }
    public int getAcctNum() { return acctNum; }
    public int getCustLineSeqId() { return custLineSeqId; }
    public String getSourceTable() { return sourceTable; }
    public String getSmpDownPricingPlans() { return smpDownPricingPlans; }
    public String getHighlight() { return highlight; }
    public void setHighlight(String highlight) { this.highlight = highlight; }

    @Override
    public String toString() {
        return "ParentData{" +
                "custId='" + custId + '\'' +
                ", acctNum=" + acctNum +
                ", custLineSeqId=" + custLineSeqId +
                ", sourceTable='" + sourceTable + '\'' +
                ", smpDownPricingPlans='" + smpDownPricingPlans + '\'' +
                ", highlight='" + highlight + '\'' +
                '}';
    }
}

import java.util.*;
import java.util.stream.Collectors;

public class DataTransformer {

    public static List<CustomRow> deriveWithHighlight(List<ParentData> rows) {

        // Group by composite key
        Map<String, List<ParentData>> grouped = rows.stream()
                .collect(Collectors.groupingBy(d -> d.getCustId() + "|" + d.getAcctNum() + "|" + d.getCustLineSeqId()));

        List<CustomRow> result = new ArrayList<>();

        for (Map.Entry<String, List<ParentData>> entry : grouped.entrySet()) {
            List<ParentData> group = entry.getValue();

            String custId = group.get(0).getCustId();
            int acctNum = group.get(0).getAcctNum();
            int custLineSeqId = group.get(0).getCustLineSeqId();

            // extract relevant column values
            String profileVal = getValue(group, "cust_line_profile");
            String mtzVal = getValue(group, "mtz_adobe");
            String batchVal = getValue(group, "batch_line_vzw");
            String realVal = getValue(group, "real_line_nrt");

            // highlight rules
            String highlight = "none";
            if (Objects.equals(batchVal, profileVal) && !Objects.equals(batchVal, mtzVal)) {
                highlight = (realVal == null) ? "light-red" : "light-yellow";
            }

            // set highlight for all ParentData objects in the group
            for (ParentData pd : group) {
                pd.setHighlight(highlight);
            }

            result.add(new CustomRow(custId, acctNum, custLineSeqId,
                                     profileVal, mtzVal, highlight, group));
        }

        return result;
    }

    private static String getValue(List<ParentData> group, String table) {
        return group.stream()
                .filter(d -> table.equalsIgnoreCase(d.getSourceTable()))
                .map(ParentData::getSmpDownPricingPlans)
                .findFirst()
                .orElse(null);
    }
}
