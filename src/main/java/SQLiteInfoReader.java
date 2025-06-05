import parser.HeaderParser;
import parser.PageParser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;

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


            System.out.println("\n=== First Page Info ===");
            int pageSize = getPageSize(header);
            byte[] firstPage = new byte[pageSize];
            file.readFully(firstPage);
            PageParser.parsePage(firstPage);
        }
    }


    public static void analyseDatabaseCells(String dbPath) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(dbPath, "r")) {
            byte[] header = new byte[100];
            file.readFully(header);
            int pageSize = getPageSize(header);

            file.seek(0);
            byte[] page = new byte[pageSize];
            file.readFully(page);

            // Get page info
            int numCells = ((page[103] & 0xFF) << 8) | (page[104] & 0xFF);
            int cellContentStart = ((page[105] & 0xFF) << 8) | (page[106] & 0xFF);

            System.out.println("=== Page Structure ===");
            System.out.println("Page size: " + pageSize);
            System.out.println("Number of cells: " + numCells);
            System.out.println("Cell content starts at: " + cellContentStart);
            System.out.println("Pointer array ends at: " + (108 + numCells * 2));

            // Collect all cell info
            CellInfo[] cells = new CellInfo[numCells];
            for (int i = 0; i < numCells; i++) {
                int pointerAddr = 108 + (i * 2);
                int offset = ((page[pointerAddr] & 0xFF) << 8) |
                        (page[pointerAddr + 1] & 0xFF);
                cells[i] = new CellInfo(i, offset);
            }


            // Sort by offset to find physical layout
            CellInfo[] physicalOrder = cells.clone();
            Arrays.sort(physicalOrder, Comparator.comparingInt(a -> a.offset));


            // Calculate sizes
            for (int i = 0; i < physicalOrder.length; i++) {
                if (i < physicalOrder.length - 1) {
                    physicalOrder[i].size = physicalOrder[i + 1].offset - physicalOrder[i].offset;
                } else {
                    // Last cell extends to end of page
                    physicalOrder[i].size = pageSize - physicalOrder[i].offset;
                }
            }


            // Display in physical order
            System.out.println("\n=== Cells (Physical Order on Page) ===");
            for (CellInfo cell : physicalOrder) {
                System.out.printf("Cell %d: offset=%d-%d (%d bytes)%n",
                        cell.index, cell.offset,
                        cell.offset + cell.size - 1, cell.size);
            }

            System.out.println("\n=== Cell Data ===");
            for (CellInfo cell: physicalOrder){
                byte[] cellByteData = Arrays.copyOfRange(page,cell.offset,cell.offset+cell.size);
                printCellData(cellByteData,cell.size);
            }

            // Show free space
            int usedByHeaders = 108 + numCells * 2;
            int usedByCells = pageSize - cellContentStart;
            int freeSpace = cellContentStart - usedByHeaders;


            System.out.println("\n=== Space Usage ===");
            System.out.println("Headers & pointers: " + usedByHeaders + " bytes");
            System.out.println("Cell data: " + usedByCells + " bytes");
            System.out.println("Free space: " + freeSpace + " bytes");
        }
    }

    private static void printCellData(byte[] cell,int cellsize){

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



