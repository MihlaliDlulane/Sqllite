import dataTypes.CellInfo;
import dataTypes.DatabaseSchema;
import dataTypes.SQLiteRecord;
import parser.HeaderParser;
import parser.PageParser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class SQLiteInfoReader {
    private static final int SQLITE_HEADER_SIZE = 100;

    /**
     * display SQLite database file information
     *
     * @param dbPath filepath of .db file
     */
    public static void displayDatabaseInfo(String dbPath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(dbPath, "r")) {
            byte[] header = new byte[SQLITE_HEADER_SIZE];
            file.readFully(header);

            System.out.println("=== SQLite Database Information ===");
            System.out.println("File: " + dbPath);
            System.out.println("File size: " + file.length() + " bytes");

            HeaderParser.parserHeader(header);
        }
    }


    public static void analyseDatabasePages(String dbPath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(dbPath, "r")) {
            byte[] header = new byte[SQLITE_HEADER_SIZE];
            file.readFully(header);
            int pageSize = getPageSize(header);
            long fileSize = file.length();
            int totalPages = (int)(fileSize / pageSize);

            System.out.println("Total pages in database: " + totalPages);
            System.out.println("Page size: " + pageSize + " bytes");
            System.out.println();

            // Iterate through all pages
            for (int pageNum = 0; pageNum < totalPages; pageNum++) {
                // Seek to the start of the current page
                file.seek((long)pageNum * pageSize);

                // Read the page
                byte[] page = new byte[pageSize];
                file.readFully(page);

                System.out.println("=== Analyzing Page " + (pageNum + 1) + " of " + totalPages + " ===");

                // Parse the page - pass true for first page, false for others
                PageParser.parsePage(page, pageNum == 0);

                System.out.println(); // Add spacing between pages
            }
        }
    }

    public static void dotTableCommand(String dbPath) throws IOException {
        DatabaseSchema schema = new DatabaseSchema();

        try (RandomAccessFile file = new RandomAccessFile(dbPath, "r")) {
            byte[] header = new byte[SQLITE_HEADER_SIZE];
            file.readFully(header);
            int pageSize = getPageSize(header);
            long totalPages = file.length() / pageSize;

            // First, read the sqlite_master table from page 1
            file.seek(0);
            byte[] firstPage = new byte[pageSize];
            file.readFully(firstPage);

            // Parse the first page to get sqlite_master records
            PageParser.PageInfo firstPageInfo = PageParser.parsePageInfo(firstPage, true, 0);

            // Extract schema records
            for (CellInfo cell : firstPageInfo.cells) {
                if (!cell.hasError() && cell.getRecord() != null) {
                    SQLiteRecord record = cell.getRecord();
                    record.setRecordType(SQLiteRecord.RecordType.SCHEMA_RECORD);
                    record.setPageNumber(0);
                    record.setCellIndex(cell.getCellIndex());
                    schema.addRecord(record);
                }
            }

            // Now parse all pages to collect table records
            // This is a simple approach
            for (int pageNum = 1; pageNum < totalPages; pageNum++) {
                file.seek((long)pageNum * pageSize);
                byte[] page = new byte[pageSize];
                file.readFully(page);

                PageParser.PageInfo pageInfo = PageParser.parsePageInfo(page, false, pageNum);

                // Only process table leaf pages
                if (pageInfo.isTableLeafPage()) {
                    for (CellInfo cell : pageInfo.cells) {
                        if (!cell.hasError() && cell.getRecord() != null) {
                            SQLiteRecord record = cell.getRecord();
                            record.setPageNumber(pageNum);
                            record.setCellIndex(cell.getCellIndex());
                            record.setRecordType(SQLiteRecord.RecordType.TABLE_RECORD);

                            // Try to determine which table this record belongs to
                            // by checking which table's root page this is
                            String tableName = schema.findTableByPage(pageNum);
                            if (tableName != null) {
                                record.setTableName(tableName);
                            }

                            schema.addRecord(record);
                        }
                    }
                }
            }

            // Print results
            System.out.println("=== Tables in database ===");
            List<String> tableNames = schema.getTableNames();

            if (tableNames.isEmpty()) {
                System.out.println("No user tables found.");
            } else {
                // Format output in columns
                int maxWidth = tableNames.stream().mapToInt(String::length).max().orElse(0);
                int columnsPerRow = Math.max(1, 80 / (maxWidth + 2));

                for (int i = 0; i < tableNames.size(); i++) {
                    System.out.printf("%-" + (maxWidth + 2) + "s", tableNames.get(i));
                    if ((i + 1) % columnsPerRow == 0) {
                        System.out.println();
                    }
                }
                if (tableNames.size() % columnsPerRow != 0) {
                    System.out.println();
                }
            }

            // Print detailed information
            System.out.println("\n=== Detailed Table Information ===");
            schema.printSummary();
        }
    }


    /**
     * Parse page size from big-endian format to int
     *
     * @param header byte array of the header
     * @return page size as int
     */
    private static int getPageSize(byte[] header) {
        // Read 2 bytes at offset 16 as big-endian unsigned short
        int b1 = header[16] & 0xFF;
        int b2 = header[17] & 0xFF;
        int pageSize = (b1 << 8) | b2;

        // Special case: if stored value is 1, actual page size is 65536
        return (pageSize == 1) ? 65536 : pageSize;
    }

}



