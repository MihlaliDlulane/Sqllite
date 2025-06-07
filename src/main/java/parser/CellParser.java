package parser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dataTypes.CellInfo;
import dataTypes.SQLiteRecord;
import helpers.VarintDecoder;

public class CellParser {


    public static void parseCell(byte[] page, int cellOffset, byte pageType) {
        CellInfo cellInfo = parseCellInfo(page, cellOffset, pageType);
        if (cellInfo != null && !cellInfo.hasError()) {
            cellInfo.print();
        }
    }


    public static CellInfo parseCellInfo(byte[] page, int cellOffset, byte pageType) {
        try {
            switch (pageType) {
                case 0x0D -> { return parseTableLeafCellInfo(page, cellOffset); }
                case 0x05 -> { return parseTableInteriorCellInfo(page, cellOffset); }
                case 0x0A -> { return parseIndexLeafCellInfo(page, cellOffset); }
                case 0x02 -> { return parseIndexInteriorCellInfo(page, cellOffset); }
                default -> {
                    CellInfo cellInfo = new CellInfo();
                    cellInfo.setError("Unknown page type: 0x" + String.format("%02X", pageType));
                    return cellInfo;
                }
            }
        } catch (Exception e) {
            CellInfo cellInfo = new CellInfo();
            cellInfo.setError(e.getMessage());
            return cellInfo;
        }
    }

    // Table B-tree leaf cell (0x0D pages)
    private static CellInfo parseTableLeafCellInfo(byte[] page, int cellOffset) {
        CellInfo cellInfo = new CellInfo();
        cellInfo.setCellType((byte) 0x0D);

        int pos = cellOffset;

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        cellInfo.setPayloadSize(payloadSize);

        // Read rowid
        result = VarintDecoder.decodeVarint(page, pos);
        long rowid = result[0];
        varintLen = (int)result[1];
        pos += varintLen;

        cellInfo.setRowId(rowid);

        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        cellInfo.setRecord(record);

        return cellInfo;
    }

    // Table B-tree interior cell (0x05 pages)
    private static CellInfo parseTableInteriorCellInfo(byte[] page, int cellOffset) {
        CellInfo cellInfo = new CellInfo();
        cellInfo.setCellType((byte) 0x05);

        int pos = cellOffset;

        // Read 4-byte left child page number (big-endian)
        int leftChild = ByteBuffer.wrap(page, pos, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        // Read integer key (rowid)
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long rowid = result[0];

        cellInfo.setRowId(rowid);
        // For interior cells, we might want to store the left child page
        // You could add a leftChildPage field to CellInfo if needed

        return cellInfo;
    }

    // Index B-tree leaf cell (0x0A pages)
    private static CellInfo parseIndexLeafCellInfo(byte[] page, int cellOffset) {
        CellInfo cellInfo = new CellInfo();
        cellInfo.setCellType((byte) 0x0A);

        int pos = cellOffset;

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        cellInfo.setPayloadSize(payloadSize);

        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        cellInfo.setRecord(record);

        return cellInfo;
    }

    // Index B-tree interior cell (0x02 pages)
    private static CellInfo parseIndexInteriorCellInfo(byte[] page, int cellOffset) {
        CellInfo cellInfo = new CellInfo();
        cellInfo.setCellType((byte) 0x02);

        int pos = cellOffset;

        // Read 4-byte left child page number
        int leftChild = ByteBuffer.wrap(page, pos, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        // Read payload size
        long[] result = VarintDecoder.decodeVarint(page, pos);
        long payloadSize = result[0];
        int varintLen = (int)result[1];
        pos += varintLen;

        cellInfo.setPayloadSize(payloadSize);

        // Parse the record
        SQLiteRecord record = SQLiteRecord.parse(page, pos, (int)payloadSize);
        cellInfo.setRecord(record);

        return cellInfo;
    }

    // Keep original print methods for backward compatibility
    public static void parseTableLeafCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing table leaf cell at offset " + cellOffset + " ===");

        CellInfo cellInfo = parseTableLeafCellInfo(page, cellOffset);

        System.out.println("Payload size: " + cellInfo.getPayloadSize() + " bytes");
        System.out.println("Rowid: " + cellInfo.getRowId());

        SQLiteRecord record = cellInfo.getRecord();
        if (record != null) {
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
    }

    public static void parseTableInteriorCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing table interior cell at offset " + cellOffset + " ===");

        CellInfo cellInfo = parseTableInteriorCellInfo(page, cellOffset);

        System.out.println("Rowid (integer key): " + cellInfo.getRowId());
    }

    public static void parseIndexLeafCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing index leaf cell at offset " + cellOffset + " ===");

        CellInfo cellInfo = parseIndexLeafCellInfo(page, cellOffset);

        System.out.println("Payload size: " + cellInfo.getPayloadSize() + " bytes");

        SQLiteRecord record = cellInfo.getRecord();
        if (record != null) {
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
    }

    public static void parseIndexInteriorCell(byte[] page, int cellOffset) {
        System.out.println("=== Parsing index interior cell at offset " + cellOffset + " ===");

        CellInfo cellInfo = parseIndexInteriorCellInfo(page, cellOffset);

        System.out.println("Payload size: " + cellInfo.getPayloadSize() + " bytes");

        SQLiteRecord record = cellInfo.getRecord();
        if (record != null) {
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
    }
}