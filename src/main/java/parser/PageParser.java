package parser;

import dataTypes.CellInfo;
import dataTypes.SQLiteRecord;

import java.util.ArrayList;
import java.util.List;

public class PageParser {

    // Data class to hold page information
    public static class PageInfo {
        public final byte pageType;
        public final int numCells;
        public final int firstFreeblock;
        public final int cellContentStart;
        public final int fragmentedBytes;
        public final List<CellInfo> cells;
        public final boolean isFirstPage;
        public final int pageNumber;

        public PageInfo(byte pageType, int numCells, int firstFreeblock,
                        int cellContentStart, int fragmentedBytes,
                        boolean isFirstPage, int pageNumber) {
            this.pageType = pageType;
            this.numCells = numCells;
            this.firstFreeblock = firstFreeblock;
            this.cellContentStart = cellContentStart;
            this.fragmentedBytes = fragmentedBytes;
            this.cells = new ArrayList<>();
            this.isFirstPage = isFirstPage;
            this.pageNumber = pageNumber;
        }

        public String getPageTypeName() {
            return PageParser.getPageTypeName(this.pageType);
        }

        public boolean isTableLeafPage() {
            return pageType == 0x0D;
        }

        public boolean isIndexLeafPage() {
            return pageType == 0x0A;
        }
    }


    public static void parsePage(byte[] page, boolean isFirstPage) {
        PageInfo pageInfo = parsePageInfo(page, isFirstPage, 0);
        printPageInfo(pageInfo);
    }


    public static PageInfo parsePageInfo(byte[] page, boolean isFirstPage, int pageNumber) {
        // First page has 100-byte file header before B-tree header
        int btreeOffset = isFirstPage ? 100 : 0;

        // Read B-tree page header (8 bytes)
        byte pageType = page[btreeOffset];
        int firstFreeblock = ((page[btreeOffset + 1] & 0xFF) << 8) |
                (page[btreeOffset + 2] & 0xFF);
        int numCells = ((page[btreeOffset + 3] & 0xFF) << 8) |
                (page[btreeOffset + 4] & 0xFF);
        int cellContentStart = ((page[btreeOffset + 5] & 0xFF) << 8) |
                (page[btreeOffset + 6] & 0xFF);
        int fragmentedBytes = page[btreeOffset + 7] & 0xFF;

        PageInfo pageInfo = new PageInfo(pageType, numCells, firstFreeblock,
                cellContentStart, fragmentedBytes,
                isFirstPage, pageNumber);

        // Cell pointer array starts after B-tree header
        int pointerArrayStart = btreeOffset + 8;

        // Read all cell pointers and parse cells
        for (int i = 0; i < numCells; i++) {
            int pointerAddr = pointerArrayStart + (i * 2);
            int cellOffset = ((page[pointerAddr] & 0xFF) << 8) |
                    (page[pointerAddr + 1] & 0xFF);

            try {
                // This will need to be updated when you show me CellParser
                CellInfo cellInfo = CellParser.parseCellInfo(page, cellOffset, pageType);
                if (cellInfo != null) {
                    cellInfo.setCellIndex(i);
                    cellInfo.setPageNumber(pageNumber);
                    pageInfo.cells.add(cellInfo);
                }
            } catch (Exception e) {
                // Create an error cell info
                CellInfo errorCell = new CellInfo();
                errorCell.setCellIndex(i);
                errorCell.setError(e.getMessage());
                pageInfo.cells.add(errorCell);
            }
        }

        return pageInfo;
    }

    // Helper method to print PageInfo (for debugging)
    private static void printPageInfo(PageInfo pageInfo) {
        System.out.println("=== Page Header ===");
        System.out.println("Page type: " + pageInfo.getPageTypeName());
        System.out.println("Number of cells: " + pageInfo.numCells);
        System.out.println("First freeblock: " + pageInfo.firstFreeblock);
        System.out.println("Cell content area starts at: " + pageInfo.cellContentStart);
        System.out.println("Fragmented free bytes: " + pageInfo.fragmentedBytes);

        System.out.println("\n=== Cell Contents ===");
        for (int i = 0; i < pageInfo.cells.size(); i++) {
            System.out.println("\n--- Cell " + i + " ---");
            CellInfo cell = pageInfo.cells.get(i);
            if (cell.hasError()) {
                System.err.println("Error parsing cell: " + cell.getError());
            } else {
                // Print cell info
                cell.print();
            }
        }
    }

    // Method to get all records from a page
    public static List<SQLiteRecord> getPageRecords(byte[] page, boolean isFirstPage, int pageNumber) {
        PageInfo pageInfo = parsePageInfo(page, isFirstPage, pageNumber);
        List<SQLiteRecord> records = new ArrayList<>();

        // Only process table leaf pages for now
        if (pageInfo.isTableLeafPage()) {
            for (CellInfo cell : pageInfo.cells) {
                if (!cell.hasError() && cell.getRecord() != null) {
                    SQLiteRecord record = cell.getRecord();
                    record.setPageNumber(pageNumber);
                    record.setCellIndex(cell.getCellIndex());
                    records.add(record);
                }
            }
        }

        return records;
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