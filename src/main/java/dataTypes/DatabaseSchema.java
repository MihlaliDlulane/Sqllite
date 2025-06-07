package dataTypes;

import java.util.*;
import java.util.stream.Collectors;

public class DatabaseSchema {
    private final Map<String, TableInfo> tables = new HashMap<>();
    private final List<SQLiteRecord> allRecords = new ArrayList<>();
    private final List<SQLiteRecord> schemaRecords = new ArrayList<>();

    public void addRecord(SQLiteRecord record) {
        allRecords.add(record);

        if (record.isSchemaRecord()) {
            schemaRecords.add(record);
            processSchemaRecord(record);
        } else if (record.getTableName() != null) {
            TableInfo table = tables.computeIfAbsent(record.getTableName(),
                    k -> new TableInfo(k));
            table.addRecord(record);
        }
    }

    private void processSchemaRecord(SQLiteRecord record) {
        SQLiteRecord.SchemaInfo schemaInfo = record.getSchemaInfo();
        if (schemaInfo != null && schemaInfo.isTable()) {
            TableInfo table = tables.computeIfAbsent(schemaInfo.name,
                    k -> new TableInfo(k));
            table.setRootPage(schemaInfo.rootPage);
            table.setSql(schemaInfo.sql);
        }
    }

    // Query methods
    public int getTableCount() {
        return (int) tables.values().stream()
                .filter(t -> t.getSql() != null && !t.getName().startsWith("sqlite_"))
                .count();
    }

    public List<String> getTableNames() {
        return tables.keySet().stream()
                .filter(name -> !name.startsWith("sqlite_"))
                .sorted()
                .collect(Collectors.toList());
    }

    public TableInfo getTable(String tableName) {
        return tables.get(tableName);
    }

    public List<TableInfo> getTables(String... tableNames) {
        return Arrays.stream(tableNames)
                .map(tables::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<SQLiteRecord> getRecordsFromTable(String tableName) {
        TableInfo table = tables.get(tableName);
        return table != null ? table.getRecords() : Collections.emptyList();
    }

    // Get all records from multiple tables
    public List<SQLiteRecord> getRecordsFromTables(String... tableNames) {
        return Arrays.stream(tableNames)
                .map(this::getRecordsFromTable)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void printSummary() {
        System.out.println("=== Database Summary ===");
        System.out.println("Total tables: " + getTableCount());
        System.out.println("Table names: " + getTableNames());
        System.out.println("\nTable Details:");

        for (String tableName : getTableNames()) {
            TableInfo table = getTable(tableName);
            System.out.println("\n  Table: " + tableName);
            System.out.println("    Records: " + table.getRecordCount());
            System.out.println("    Root Page: " + table.getRootPage());
            if (table.getSql() != null) {
                System.out.println("    SQL: " + table.getSql().replaceAll("\n", "\n         "));
            }
        }
    }

    public String findTableByPage(int pageNumber) {
        // First check if it's a root page
        for (TableInfo table : tables.values()) {
            if (table.getRootPage() == pageNumber + 1) { // Pages are 1-indexed in SQLite
                return table.getName();
            }
        }

        // TODO: For a complete implementation, need to track which pages
        // belong to which table by traversing the B-tree structure
        return null;
    }
}
