import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class SQLiteInfoReader {
    private static final int SQLITE_HEADER_SIZE = 100;

    /**
     * display SQLite database file information
     * @param dbPath filepath of .db file
     */
    public static void displayDatabaseInfo(String dbPath) throws IOException {
        try(RandomAccessFile file = new RandomAccessFile(dbPath,"r")){
            byte[] header = new byte[SQLITE_HEADER_SIZE];
            file.readFully(header);

            System.out.println("=== SQLite Database Information ===");
            System.out.println("File: " + dbPath);
            System.out.println("File size: " + file.length() + " bytes");

            // Magic string
            String magic = new String(header, 0, 16, StandardCharsets.US_ASCII);
            System.out.println("Magic string: " + magic.replace("\0", "\\0"));


            // Page size
            int pageSize = getPageSize(header);
            System.out.println("Page size: " + pageSize + " bytes");


            // File format versions
            System.out.println("File format write version: " + (header[18] & 0xFF));
            System.out.println("File format read version: " + (header[19] & 0xFF));


            // Reserved space
            System.out.println("Reserved space at end of each page: " + (header[20] & 0xFF));


            // File change counter
            int changeCounter = ByteBuffer.wrap(header, 24, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("File change counter: " + changeCounter);



            // Database size in pages
            int dbSizeInPages = ByteBuffer.wrap(header, 28, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("Database size: " + dbSizeInPages + " pages");


            // Schema version
            int schemaVersion = ByteBuffer.wrap(header, 40, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("Schema format number: " + schemaVersion);


            // Text encoding
            int encoding = ByteBuffer.wrap(header, 56, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("Text encoding: " + getEncodingName(encoding));

            // User version
            int userVersion = ByteBuffer.wrap(header, 60, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("User version: " + userVersion);

            // Application ID
            int appId = ByteBuffer.wrap(header, 68, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            System.out.println("Application ID: " + appId);

            System.out.println("\n=== First Page Info ===");
            displayFirstPageInfo(file, pageSize);
        }
    }

    private static void displayFirstPageInfo(RandomAccessFile file, int pageSize) throws IOException {
        // Read first page completely
        file.seek(0);
        byte[] firstPage = new byte[pageSize];
        file.readFully(firstPage);

        // B-tree page header starts at offset 100
        byte pageType = firstPage[100];
        System.out.println("First page type: " + getPageTypeName(pageType));


        // Number of cells
        int numCells = ((firstPage[103] & 0xFF) << 8) | (firstPage[104] & 0xFF);
        System.out.println("Number of cells in first page: " + numCells);


        // First freeblock
        int firstFreeblock = ((firstPage[101] & 0xFF) << 8) | (firstPage[102] & 0xFF);
        System.out.println("First freeblock: " + firstFreeblock);


        // Cell content area
        int cellContentOffset = ((firstPage[105] & 0xFF) << 8) | (firstPage[106] & 0xFF);
        System.out.println("Cell content starts at: " + cellContentOffset);

        // Fragmented free bytes
        int fragmentedBytes = firstPage[107] & 0xFF;
        System.out.println("Fragmented free bytes: " + fragmentedBytes);
    }

    /**
     * Parse page size from big-endian format to int
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

    /**
     * Get the encoding type
     * @param encoding number
     * @return string name of the encoding type
     */
    private static String getEncodingName(int encoding) {
        return switch (encoding) {
            case 1 -> "UTF-8";
            case 2 -> "UTF-16LE";
            case 3 -> "UTF-16BE";
            default -> "Unknown (" + encoding + ")";
        };
    }

    private static String getPageTypeName(byte pageType) {
        return switch (pageType) {
            case 0x02 -> "Interior index b-tree page";
            case 0x05 -> "Interior table b-tree page";
            case 0x0A -> "Leaf index b-tree page";
            case 0x0D -> "Leaf table b-tree page";
            default -> "Unknown (0x" + String.format("%02X", pageType) + ")";
        };
    }

}
