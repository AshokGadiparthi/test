import java.util.*;
import java.util.stream.Collectors;

class ParentData {
    private String custId;
    private int acctNum;
    private Integer custLineSeqId; // may be null
    private String sourceTable;
    private String smpDownPricingPlans;

    public ParentData(String custId, int acctNum, Integer custLineSeqId, String sourceTable, String smpDownPricingPlans) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.sourceTable = sourceTable;
        this.smpDownPricingPlans = smpDownPricingPlans;
    }

    public String getCustId() { return custId; }
    public int getAcctNum() { return acctNum; }
    public Integer getCustLineSeqId() { return custLineSeqId; }
    public String getSourceTable() { return sourceTable; }
    public String getSmpDownPricingPlans() { return smpDownPricingPlans; }

    @Override
    public String toString() {
        return String.format("%s|%d|%s|%s|%s",
                custId, acctNum,
                custLineSeqId == null ? "NULL" : custLineSeqId,
                sourceTable,
                smpDownPricingPlans);
    }
}

class MasterRow {
    private String custId;
    private int acctNum;
    private Integer custLineSeqId;
    private String highlight;                // highlight for this master row
    private List<ParentData> children;       // attached BQ rows

    public MasterRow(String custId, int acctNum, Integer custLineSeqId) {
        this.custId = custId;
        this.acctNum = acctNum;
        this.custLineSeqId = custLineSeqId;
        this.highlight = "none";
        this.children = new ArrayList<>();
    }

    public void attachChildren(List<ParentData> bqRows) {
        this.children.addAll(bqRows);
        applyHighlightLogic();
    }

    private void applyHighlightLogic() {
        boolean hasBatch = children.stream().anyMatch(c -> "batch_line_vzw".equals(c.getSourceTable()));
        boolean hasCustLine = children.stream().anyMatch(c -> "cust_line_profile".equals(c.getSourceTable()));
        Optional<ParentData> adobe = children.stream().filter(c -> "mtz_adobe".equals(c.getSourceTable())).findFirst();
        Optional<ParentData> realLine = children.stream().filter(c -> "real_line_nrt".equals(c.getSourceTable())).findFirst();

        if (hasBatch && hasCustLine && adobe.isPresent()) {
            String adobeVal = adobe.get().getSmpDownPricingPlans();
            Set<String> batchCustVals = children.stream()
                    .filter(c -> "batch_line_vzw".equals(c.getSourceTable()) || "cust_line_profile".equals(c.getSourceTable()))
                    .map(ParentData::getSmpDownPricingPlans)
                    .collect(Collectors.toSet());

            if (batchCustVals.size() == 1 && !batchCustVals.contains(adobeVal)) {
                if (realLine.isPresent() && realLine.get().getSmpDownPricingPlans() == null) {
                    this.highlight = "light_red";
                } else {
                    this.highlight = "light_yellow";
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format("MasterRow(%s|%d|%s, highlight=%s, children=%s)",
                custId, acctNum,
                custLineSeqId == null ? "NULL" : custLineSeqId,
                highlight,
                children);
    }
}

public class MasterWithBQ {
    public static void main(String[] args) {
        // Master list (10 rows in your real case)
        List<MasterRow> masterList = Arrays.asList(
                new MasterRow("111", 1, 501),
                new MasterRow("222", 1, 502)
        );

        // Simulated BQ results (≈90 in your real case)
        List<ParentData> bqResults = Arrays.asList(
                new ParentData("111", 1, 501, "batch_line_vzw", "X"),
                new ParentData("111", 1, 501, "cust_line_profile", "X"),
                new ParentData("111", 1, 501, "mtz_adobe", "Y"),
                new ParentData("222", 1, 502, "batch_line_vzw", "A"),
                new ParentData("222", 1, 502, "cust_line_profile", "A"),
                new ParentData("222", 1, 502, "mtz_adobe", "B"),
                new ParentData("222", 1, 502, "real_line_nrt", null)
        );

        // Group BQ results by composite key
        Map<String, List<ParentData>> groupedBQ = bqResults.stream()
                .collect(Collectors.groupingBy(pd ->
                        pd.getCustId() + "|" + pd.getAcctNum() + "|" +
                        (pd.getCustLineSeqId() == null ? "NULL" : pd.getCustLineSeqId())));

        // Attach grouped children to each master row
        for (MasterRow master : masterList) {
            String key = master.custId + "|" + master.acctNum + "|" +
                    (master.custLineSeqId == null ? "NULL" : master.custLineSeqId);
            List<ParentData> children = groupedBQ.getOrDefault(key, Collections.emptyList());
            master.attachChildren(children);
        }

        // Print result
        masterList.forEach(System.out::println);
    }
}
