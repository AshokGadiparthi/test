import java.util.*;
import java.util.stream.Collectors;

public class DataHighlighter {

    public static Map<String, String> highlight(List<ParentData> rows) {
        // Group by composite key
        Map<String, List<ParentData>> grouped = rows.stream()
                .collect(Collectors.groupingBy(d -> d.getCustId() + "|" + d.getAcctNum() + "|" + d.getCustLineSeqId()));

        Map<String, String> highlights = new HashMap<>();

        for (Map.Entry<String, List<ParentData>> entry : grouped.entrySet()) {
            String key = entry.getKey();
            List<ParentData> group = entry.getValue();

            String batchVal = getValue(group, "batch_line_vzw");
            String profileVal = getValue(group, "cust_line_profile");
            String mtzVal = getValue(group, "mtz_adobe");
            String realVal = getValue(group, "real_line_nrt");

            String color = "none";

            if (Objects.equals(batchVal, profileVal) && !Objects.equals(batchVal, mtzVal)) {
                if (realVal == null) {
                    color = "light-red";
                } else {
                    color = "light-yellow";
                }
            }

            highlights.put(key, color);
        }

        return highlights;
    }

    private static String getValue(List<ParentData> group, String table) {
        return group.stream()
                .filter(d -> table.equalsIgnoreCase(d.getSourceTable()))
                .map(ParentData::getSmpDownPricingPlans)
                .findFirst()
                .orElse(null);
    }
}
