import java.util.*;
import java.util.stream.Collectors;

public class Example {
    public static void main(String[] args) {
        List<ParentData> list = Arrays.asList(
            new ParentData("111", 1, 501, "batch_line_vzw", "X"),
            new ParentData("222", 2, 502, "cust_line_profile", "Y"),
            new ParentData("333", 3, 503, "mtz_adobe", "Z")
        );

        // Build comma-separated strings
        String custIds = list.stream()
                .map(ParentData::getCustId)
                .collect(Collectors.joining(","));

        String acctNums = list.stream()
                .map(p -> String.valueOf(p.getAcctNum()))
                .collect(Collectors.joining(","));

        String custLineSeqs = list.stream()
                .map(p -> String.valueOf(p.getCustLineSeqId()))
                .collect(Collectors.joining(","));

        System.out.println("custIds: " + custIds);
        System.out.println("acctNums: " + acctNums);
        System.out.println("custLineSeqs: " + custLineSeqs);
    }
}
