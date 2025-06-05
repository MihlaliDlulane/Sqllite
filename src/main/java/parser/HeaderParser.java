package parser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class HeaderParser {

    public static void parserHeader(byte[] header){
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

    /**
     * Get the encoding type
     *
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

}
