package parser;

import helpers.VarintDecoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CellParser {

    // Table B-tree leaf cell (0x0D pages)
    public static void parseTableLeafCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing table leaf cell at offset " + cellOffset + " ===");

        int pos = cellOffset;

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        System.out.println("Payload size: " + payloadSize + " (varint: " + varintLen + " bytes)");

        // Read rowid
        result = VarintDecoder.decodeVarint(page, pos);
        long rowid = result[0];
        varintLen = (int)result[1];
        pos += varintLen;

        System.out.println("Rowid: " + rowid + " (varint: " + varintLen + " bytes)");

        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        System.out.println("Column count: " + record.getColumnCount());
        System.out.println("Values: " + record);

        // Access individual values
        for (int i = 0; i < record.getColumnCount(); i++) {
            Object value = record.getValue(i);
            long serialType = record.getSerialType(i);
            SQLiteRecord.ColumnType colType = SQLiteRecord.ColumnType.fromSerialType(serialType);
            System.out.printf("Column %d: type=%s, value=%s%n",
                    i, colType.type, value);
        }
    }

    // Table B-tree interior cell (0x05 pages)
    public static void parseTableInteriorCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing table interior cell at offset " + cellOffset + " ===");

        int pos = cellOffset;

        // Read 4-byte left child page number (big-endian)
        int leftChild = ByteBuffer.wrap(page, pos, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        System.out.println("Left child page: " + leftChild);

        // Read integer key (rowid)
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long rowid = result[0];

        System.out.println("Rowid (integer key): " + rowid);
    }

    // Index B-tree leaf cell (0x0A pages)
    public static void parseIndexLeafCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing index leaf cell at offset " + cellOffset + " ===");

        int pos = cellOffset;

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        System.out.println("Payload size: " + payloadSize + " bytes");

        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        System.out.println("Column count: " + record.getColumnCount());
        System.out.println("Values: " + record);

        // Access individual values
        for (int i = 0; i < record.getColumnCount(); i++) {
            Object value = record.getValue(i);
            long serialType = record.getSerialType(i);
            SQLiteRecord.ColumnType colType = SQLiteRecord.ColumnType.fromSerialType(serialType);
            System.out.printf("Column %d: type=%s, value=%s%n",
                    i, colType.type, value);
        }
    }

    // Index B-tree interior cell (0x02 pages)
    public static void parseIndexInteriorCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing index interior cell at offset " + cellOffset + " ===");

        int pos = cellOffset;

        // Read 4-byte left child page number
        int leftChild = ByteBuffer.wrap(page, pos, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        System.out.println("Left child page: " + leftChild);

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        System.out.println("Payload size: " + payloadSize + " bytes");


        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        System.out.println("Column count: " + record.getColumnCount());
        System.out.println("Values: " + record);

        // Access individual values
        for (int i = 0; i < record.getColumnCount(); i++) {
            Object value = record.getValue(i);
            long serialType = record.getSerialType(i);
            SQLiteRecord.ColumnType colType = SQLiteRecord.ColumnType.fromSerialType(serialType);
            System.out.printf("Column %d: type=%s, value=%s%n",
                    i, colType.type, value);
        }
    }


    // Dispatch to correct parser based on page type
    public static void parseCell(byte[] page, int cellOffset, byte pageType) {
        switch (pageType) {
            case 0x0D -> parseTableLeafCell(page, cellOffset);
            case 0x05 -> parseTableInteriorCell(page, cellOffset);
            case 0x0A -> parseIndexLeafCell(page, cellOffset);
            case 0x02 -> parseIndexInteriorCell(page, cellOffset);
            default -> System.out.println("Unknown page type: 0x" +
                    String.format("%02X", pageType));
        }
    }

    private static String getSerialTypeName(long serialType) {
        if (serialType == 0) return "NULL";
        if (serialType == 1) return "8-bit int";
        if (serialType == 2) return "16-bit int";
        if (serialType == 3) return "24-bit int";
        if (serialType == 4) return "32-bit int";
        if (serialType == 5) return "48-bit int";
        if (serialType == 6) return "64-bit int";
        if (serialType == 7) return "64-bit float";
        if (serialType == 8) return "integer 0";
        if (serialType == 9) return "integer 1";
        if (serialType >= 12 && serialType % 2 == 0) {
            return "BLOB length " + ((serialType - 12) / 2);
        }
        if (serialType >= 13) {
            return "TEXT length " + ((serialType - 13) / 2);
        }
        return "Unknown type " + serialType;
    }

    private static int getIntegerBytes(long serialType) {
        return switch ((int)serialType) {
            case 1 -> 1;
            case 2 -> 2;
            case 3 -> 3;
            case 4 -> 4;
            case 5 -> 6;
            case 6 -> 8;
            default -> 0;
        };
    }

    private static long readInteger(byte[] data, int offset, int bytes) {
        long value = 0;
        for (int i = 0; i < bytes; i++) {
            value = (value << 8) | (data[offset + i] & 0xFF);
        }
        // Handle sign extension for negative values
        if (bytes < 8 && (data[offset] & 0x80) != 0) {
            // Sign extend
            for (int i = bytes; i < 8; i++) {
                value |= (0xFFL << (i * 8));
            }
        }
        return value;
    }
}



