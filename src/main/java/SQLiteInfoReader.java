import parser.HeaderParser;
import parser.PageParser;

import java.io.IOException;
import java.io.RandomAccessFile;

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

            // Read and parse first page
            file.seek(0);
            byte[] firstPage = new byte[pageSize];
            file.readFully(firstPage);

            System.out.println("=== Analyzing First Page ===");
            PageParser.parsePage(firstPage,true);

            // Optionally analyze more pages
            long fileSize = file.length();
            int totalPages = (int)(fileSize / pageSize);
            System.out.println("\nTotal pages in database: " + totalPages);
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


    static class CellInfo {
        int index;
        int offset;
        int size;

        CellInfo(int index, int offset) {
            this.index = index;
            this.offset = offset;
        }
    }
}



