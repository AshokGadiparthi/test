import java.util.*;
import java.util.stream.Collectors;

public class SqlColumnBuilder {

    public static void main(String[] args) {
        // Example input: table -> set of available columns
        Map<String, Set<String>> tableColumns = new HashMap<>();
        tableColumns.put("TABLE1", new HashSet<>(Arrays.asList("AA")));
        tableColumns.put("TABLE2", new HashSet<>(Arrays.asList("AA")));
        tableColumns.put("TABLE3", new HashSet<>(Arrays.asList("BB")));
        tableColumns.put("TABLE4", new HashSet<>(Arrays.asList("AA")));
        tableColumns.put("TABLE5", new HashSet<>(Arrays.asList("BB")));
        tableColumns.put("TABLE6", new HashSet<>(Arrays.asList("AA")));

        // Desired columns
        List<String> desiredCols = Arrays.asList("AA", "BB", "CC");

        // Generate SQL for each desired column
        for (String col : desiredCols) {
            List<String> expressions = new ArrayList<>();
            for (String table : tableColumns.keySet()) {
                if (tableColumns.get(table).contains(col)) {
                    expressions.add("CAST(" + col + " AS STRING) AS " + col);
                } else {
                    expressions.add("NULL AS " + col);
                }
            }

            // Join with commas, no trailing comma
            String sqlLine = expressions.stream().collect(Collectors.joining(", "));
            System.out.println("-- For column: " + col);
            System.out.println(sqlLine);
            System.out.println();
        }
    }
}
