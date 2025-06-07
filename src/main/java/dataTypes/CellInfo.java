package dataTypes;

public class CellInfo {
    private int cellIndex;
    private int pageNumber;
    private long rowId;
    private SQLiteRecord record;
    private String error;
    private byte cellType;

    // For table cells
    private long payloadSize;
    private int headerSize;

    // For interior cells
    private int leftChildPage;

    // Getters and setters
    public int getCellIndex() { return cellIndex; }
    public void setCellIndex(int cellIndex) { this.cellIndex = cellIndex; }

    public int getPageNumber() { return pageNumber; }
    public void setPageNumber(int pageNumber) { this.pageNumber = pageNumber; }

    public long getRowId() { return rowId; }
    public void setRowId(long rowId) { this.rowId = rowId; }

    public SQLiteRecord getRecord() { return record; }
    public void setRecord(SQLiteRecord record) { this.record = record; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
    public boolean hasError() { return error != null; }

    public byte getCellType() { return cellType; }
    public void setCellType(byte cellType) { this.cellType = cellType; }

    public long getPayloadSize() { return payloadSize; }
    public void setPayloadSize(long payloadSize) { this.payloadSize = payloadSize; }

    public int getHeaderSize() { return headerSize; }
    public void setHeaderSize(int headerSize) { this.headerSize = headerSize; }

    public int getLeftChildPage() { return leftChildPage; }
    public void setLeftChildPage(int leftChildPage) { this.leftChildPage = leftChildPage; }

    public boolean isTableLeafCell() { return cellType == 0x0D; }
    public boolean isTableInteriorCell() { return cellType == 0x05; }
    public boolean isIndexLeafCell() { return cellType == 0x0A; }
    public boolean isIndexInteriorCell() { return cellType == 0x02; }

    public void print() {
        if (hasError()) {
            System.err.println("Error: " + error);
            return;
        }

        System.out.println("Cell Type: " + getCellTypeName());
        System.out.println("Cell Index: " + cellIndex);

        if (isTableLeafCell() || isTableInteriorCell()) {
            System.out.println("Row ID: " + rowId);
        }

        if (leftChildPage > 0) {
            System.out.println("Left Child Page: " + leftChildPage);
        }

        if (payloadSize > 0) {
            System.out.println("Payload Size: " + payloadSize);
        }

        if (record != null) {
            System.out.println("Record: " + record);
        }
    }

    private String getCellTypeName() {
        return switch (cellType) {
            case 0x02 -> "Index Interior Cell";
            case 0x05 -> "Table Interior Cell";
            case 0x0A -> "Index Leaf Cell";
            case 0x0D -> "Table Leaf Cell";
            default -> "Unknown Cell Type (0x" + String.format("%02X", cellType) + ")";
        };
    }
}
