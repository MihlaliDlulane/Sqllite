package dataTypes;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TableInfo {
    private final String name;
    private final List<SQLiteRecord> records = new ArrayList<>();
    private int rootPage = -1;
    private String sql;
    private List<String> columnNames = new ArrayList<>();
    private List<String> columnTypes = new ArrayList<>();
    private Map<String, Integer> columnIndexMap = new HashMap<>();
    private boolean isPrimaryKeyAutoIncrement = false;

    public TableInfo(String name) {
        this.name = name;
    }

    public void addRecord(SQLiteRecord record) {
        record.setTableName(this.name);
        records.add(record);
    }

    // Getters and setters
    public String getName() { return name; }
    public List<SQLiteRecord> getRecords() { return new ArrayList<>(records); }
    public int getRecordCount() { return records.size(); }
    public int getRootPage() { return rootPage; }
    public void setRootPage(int rootPage) { this.rootPage = rootPage; }
    public String getSql() { return sql; }

    public void setSql(String sql) {
        this.sql = sql;
        parseColumnNames();
    }

    // Get column names
    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    // Get column types
    public List<String> getColumnTypes() {
        return new ArrayList<>(columnTypes);
    }

    // Get column count
    public int getColumnCount() {
        return columnNames.size();
    }

    // Get column index by name
    public int getColumnIndex(String columnName) {
        Integer index = columnIndexMap.get(columnName.toLowerCase());
        return index != null ? index : -1;
    }

    // Get specific column values from all records
    public List<Object> getColumnValues(String columnName) {
        int index = getColumnIndex(columnName);
        if (index == -1) {
            return Collections.emptyList();
        }

        return records.stream()
                .map(record -> record.getValue(index))
                .collect(Collectors.toList());
    }

    // Get records where column matches value
    public List<SQLiteRecord> getRecordsWhere(String columnName, Object value) {
        int index = getColumnIndex(columnName);
        if (index == -1) {
            return Collections.emptyList();
        }

        return records.stream()
                .filter(record -> {
                    Object recordValue = record.getValue(index);
                    if (value == null) {
                        return recordValue == null;
                    }
                    return value.equals(recordValue);
                })
                .collect(Collectors.toList());
    }

    // Get records matching multiple column conditions
    public List<SQLiteRecord> getRecordsWhere(Map<String, Object> conditions) {
        return records.stream()
                .filter(record -> {
                    for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                        int index = getColumnIndex(entry.getKey());
                        if (index == -1) {
                            return false;
                        }
                        Object recordValue = record.getValue(index);
                        Object expectedValue = entry.getValue();

                        if (expectedValue == null) {
                            if (recordValue != null) return false;
                        } else {
                            if (!expectedValue.equals(recordValue)) return false;
                        }
                    }
                    return true;
                })
                .collect(Collectors.toList());
    }

    // Get distinct values for a column
    public Set<Object> getDistinctValues(String columnName) {
        int index = getColumnIndex(columnName);
        if (index == -1) {
            return Collections.emptySet();
        }

        return records.stream()
                .map(record -> record.getValue(index))
                .collect(Collectors.toSet());
    }

    // Get record by primary key (assumes first column is primary key)
    public SQLiteRecord getRecordByPrimaryKey(Object primaryKey) {
        if (columnNames.isEmpty()) {
            return null;
        }

        return records.stream()
                .filter(record -> primaryKey.equals(record.getValue(0)))
                .findFirst()
                .orElse(null);
    }

    // Sort records by column
    public List<SQLiteRecord> getRecordsSortedBy(String columnName, boolean ascending) {
        int index = getColumnIndex(columnName);
        if (index == -1) {
            return new ArrayList<>(records);
        }

        Comparator<SQLiteRecord> comparator = (r1, r2) -> {
            Object v1 = r1.getValue(index);
            Object v2 = r2.getValue(index);

            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return ascending ? -1 : 1;
            if (v2 == null) return ascending ? 1 : -1;

            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                @SuppressWarnings("unchecked")
                Comparable<Object> c1 = (Comparable<Object>) v1;
                return c1.compareTo(v2);
            }

            return v1.toString().compareTo(v2.toString());
        };

        if (!ascending) {
            comparator = comparator.reversed();
        }

        return records.stream()
                .sorted(comparator)
                .collect(Collectors.toList());
    }

    // Parse column names and types from CREATE TABLE SQL
    private void parseColumnNames() {
        if (sql == null || sql.isEmpty()) {
            return;
        }

        columnNames.clear();
        columnTypes.clear();
        columnIndexMap.clear();

        // Remove CREATE TABLE and get the column definitions
        String upperSql = sql.toUpperCase();
        int startIndex = upperSql.indexOf("(");
        int endIndex = upperSql.lastIndexOf(")");

        if (startIndex == -1 || endIndex == -1 || startIndex >= endIndex) {
            return;
        }

        String columnDefs = sql.substring(startIndex + 1, endIndex);

        // Split by comma, but respect parentheses (for CHECK constraints, etc.)
        List<String> columnDefList = splitColumnDefinitions(columnDefs);

        int index = 0;
        for (String columnDef : columnDefList) {
            columnDef = columnDef.trim();

            // Skip constraints like PRIMARY KEY, FOREIGN KEY, CHECK, etc.
            String upperDef = columnDef.toUpperCase();
            if (upperDef.startsWith("PRIMARY KEY") ||
                    upperDef.startsWith("FOREIGN KEY") ||
                    upperDef.startsWith("UNIQUE") ||
                    upperDef.startsWith("CHECK") ||
                    upperDef.startsWith("CONSTRAINT")) {
                continue;
            }

            // Parse column name and type
            String[] parts = columnDef.split("\\s+", 2);
            if (parts.length > 0) {
                String columnName = parts[0]
                        .replace("\"", "")
                        .replace("'", "")
                        .replace("`", "")
                        .replace("[", "")
                        .replace("]", "");
                columnNames.add(columnName);
                columnIndexMap.put(columnName.toLowerCase(), index++);

                // Extract column type
                if (parts.length > 1) {
                    String typeDef = parts[1];
                    String type = extractColumnType(typeDef);
                    columnTypes.add(type);

                    // Check for AUTOINCREMENT
                    if (typeDef.toUpperCase().contains("AUTOINCREMENT")) {
                        isPrimaryKeyAutoIncrement = true;
                    }
                } else {
                    columnTypes.add("TEXT"); // Default type
                }
            }
        }
    }

    private List<String> splitColumnDefinitions(String columnDefs) {
        List<String> result = new ArrayList<>();
        int parenCount = 0;
        StringBuilder current = new StringBuilder();

        for (char c : columnDefs.toCharArray()) {
            if (c == '(') {
                parenCount++;
            } else if (c == ')') {
                parenCount--;
            } else if (c == ',' && parenCount == 0) {
                result.add(current.toString());
                current = new StringBuilder();
                continue;
            }
            current.append(c);
        }

        if (current.length() > 0) {
            result.add(current.toString());
        }

        return result;
    }

    private String extractColumnType(String typeDef) {
        // Extract the base type (INTEGER, TEXT, REAL, BLOB, etc.)
        Pattern pattern = Pattern.compile("^(\\w+)");
        Matcher matcher = pattern.matcher(typeDef);

        if (matcher.find()) {
            return matcher.group(1).toUpperCase();
        }

        return "TEXT"; // Default
    }

    // Get statistics about the table
    public TableStatistics getStatistics() {
        return new TableStatistics(this);
    }

    // Inner class for table statistics
    public static class TableStatistics {
        private final int recordCount;
        private final int columnCount;
        private final Map<String, ColumnStatistics> columnStats;

        public TableStatistics(TableInfo table) {
            this.recordCount = table.getRecordCount();
            this.columnCount = table.getColumnCount();
            this.columnStats = new HashMap<>();

            // Calculate statistics for each column
            for (int i = 0; i < table.columnNames.size(); i++) {
                String columnName = table.columnNames.get(i);
                String columnType = i < table.columnTypes.size() ? table.columnTypes.get(i) : "UNKNOWN";

                List<Object> values = table.getColumnValues(columnName);
                columnStats.put(columnName, new ColumnStatistics(columnName, columnType, values));
            }
        }

        public int getRecordCount() { return recordCount; }
        public int getColumnCount() { return columnCount; }
        public ColumnStatistics getColumnStats(String columnName) {
            return columnStats.get(columnName);
        }

        public void printStatistics() {
            System.out.println("Record Count: " + recordCount);
            System.out.println("Column Count: " + columnCount);

            for (Map.Entry<String, ColumnStatistics> entry : columnStats.entrySet()) {
                System.out.println("\nColumn: " + entry.getKey());
                entry.getValue().print();
            }
        }
    }

    // Inner class for column statistics
    public static class ColumnStatistics {
        private final String name;
        private final String type;
        private final int totalValues;
        private final int nullCount;
        private final int distinctCount;
        private final Object minValue;
        private final Object maxValue;

        public ColumnStatistics(String name, String type, List<Object> values) {
            this.name = name;
            this.type = type;
            this.totalValues = values.size();
            this.nullCount = (int) values.stream().filter(Objects::isNull).count();

            Set<Object> distinctValues = new HashSet<>(values);
            this.distinctCount = distinctValues.size();

            // Calculate min/max for comparable values
            Object min = null, max = null;
            for (Object value : values) {
                if (value instanceof Comparable) {
                    if (min == null || ((Comparable) value).compareTo(min) < 0) {
                        min = value;
                    }
                    if (max == null || ((Comparable) value).compareTo(max) > 0) {
                        max = value;
                    }
                }
            }
            this.minValue = min;
            this.maxValue = max;
        }

        public void print() {
            System.out.println("  Type: " + type);
            System.out.println("  Total Values: " + totalValues);
            System.out.println("  Null Count: " + nullCount);
            System.out.println("  Distinct Count: " + distinctCount);
            if (minValue != null) {
                System.out.println("  Min Value: " + minValue);
                System.out.println("  Max Value: " + maxValue);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(name).append("\n");
        sb.append("  Root Page: ").append(rootPage).append("\n");
        sb.append("  Record Count: ").append(records.size()).append("\n");
        sb.append("  Columns: ").append(columnNames).append("\n");
        if (sql != null) {
            sb.append("  SQL: ").append(sql.replaceAll("\n", "\n       "));
        }
        return sb.toString();
    }
}