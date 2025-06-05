package parser;

public class PageParser {

    public static void parsePage(byte[] page){
        byte pageType = page[0];
        System.out.println("Page type: " + getPageTypeName(pageType));

        // Number of cells
        int numCells = ((page[3] & 0xFF) << 8) | (page[4] & 0xFF);
        System.out.println("Number of cells in first page: " + numCells);


        // First freeblock
        int firstFreeblock = ((page[1] & 0xFF) << 8) | (page[2] & 0xFF);
        System.out.println("First freeblock: " + firstFreeblock);


        // Cell content area
        int cellContentOffset = ((page[5] & 0xFF) << 8) | (page[6] & 0xFF);
        System.out.println("Cell content starts at: " + cellContentOffset);

        // Fragmented free bytes
        int fragmentedBytes = page[7] & 0xFF;
        System.out.println("Fragmented free bytes: " + fragmentedBytes);
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
