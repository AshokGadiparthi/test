import java.util.*;
import java.util.stream.Collectors;

public class DataTransformer {

    public static List<CustomRow> transform(List<ParentData> rows) {
        // Group by composite key custId|acctNum|custLineSeqId
        Map<String, List<ParentData>> grouped = rows.stream()
                .collect(Collectors.groupingBy(r -> r.getCustId() + "|" + r.getAcctNum() + "|" + r.getCustLineSeqId()));

        List<CustomRow> result = new ArrayList<>();

        // Iterate grouped entries
        for (List<ParentData> group : grouped.values()) {

            String custId = group.get(0).getCustId();
            int acctNum = group.get(0).getAcctNum();
            int custLineSeqId = group.get(0).getCustLineSeqId();

            // Extract cust_line_profile and mtz_adobe
            String profileVal = getColValue(group, "cust_line_profile");
            String mtzVal = getColValue(group, "mtz_adobe");
            String batchVal = getColValue(group, "batch_line_vzw");
            String realVal = getColValue(group, "real_line_nrt");

            // Determine highlight
            String highlight = "none";
            if (Objects.equals(batchVal, profileVal) && !Objects.equals(batchVal, mtzVal)) {
                highlight = (realVal == null) ? "light-red" : "light-yellow";
            }

            // Add to result
            result.add(new CustomRow(custId, acctNum, custLineSeqId, profileVal, mtzVal, highlight, group));
        }

        return result;
    }

    private static String getColValue(List<ParentData> group, String colName) {
        return group.stream()
                .filter(d -> colName.equalsIgnoreCase(d.getColName()))
                .map(ParentData::getColName) // using colName itself as "value"
                .findFirst()
                .orElse(null);
    }
}
