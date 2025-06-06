package parser;

public class PageParser {

    public static void parsePage(byte[] page, boolean isFirstPage) {
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

        System.out.println("=== Page Header ===");
        System.out.println("Page type: " + getPageTypeName(pageType));
        System.out.println("Number of cells: " + numCells);
        System.out.println("First freeblock: " + firstFreeblock);
        System.out.println("Cell content area starts at: " + cellContentStart);
        System.out.println("Fragmented free bytes: " + fragmentedBytes);

        // Cell pointer array starts after B-tree header
        int pointerArrayStart = btreeOffset + 8;

        // Read all cell pointers first
        int[] cellOffsets = new int[numCells];
        System.out.println("\n=== Cell Pointers ===");
        for (int i = 0; i < numCells; i++) {
            int pointerAddr = pointerArrayStart + (i * 2);
            cellOffsets[i] = ((page[pointerAddr] & 0xFF) << 8) |
                    (page[pointerAddr + 1] & 0xFF);
            System.out.printf("Cell %d pointer: %d%n", i, cellOffsets[i]);
        }

        // Parse each cell
        System.out.println("\n=== Cell Contents ===");
        for (int i = 0; i < numCells; i++) {
            System.out.println("\n--- Cell " + i + " ---");
            try {
                CellParser.parseCell(page, cellOffsets[i], pageType);
            } catch (Exception e) {
                System.err.println("Error parsing cell " + i + ": " + e.getMessage() + "\n likely contain some special data being parsed incorrectly ?");
            }
        }
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
