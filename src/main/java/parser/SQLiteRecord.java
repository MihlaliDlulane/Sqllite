package parser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import helpers.VarintDecoder;

public class SQLiteRecord {
    private final List<Long> serialTypes;
    private final List<Object> values;
    private final int headerSize;
    private final int dataSize;

    private SQLiteRecord(List<Long> serialTypes, List<Object> values,
                         int headerSize, int dataSize) {
        this.serialTypes = serialTypes;
        this.values = values;
        this.headerSize = headerSize;
        this.dataSize = dataSize;
    }

    public static SQLiteRecord parse(byte[] data, int offset, int payloadSize) {
        RecordParser parser = new RecordParser(data, offset, payloadSize);
        return parser.parse();
    }

    // Getters
    public List<Object> getValues() { return values; }
    public Object getValue(int column) { return values.get(column); }
    public int getColumnCount() { return values.size(); }
    public long getSerialType(int column) { return serialTypes.get(column); }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            Object value = values.get(i);
            if (value instanceof byte[]) {
                sb.append("BLOB(").append(((byte[])value).length).append(" bytes)");
            } else if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else {
                sb.append(value);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    // Inner parser class
    private static class RecordParser {
        private final byte[] data;
        private final int startOffset;
        private final int payloadSize;
        private final int endOffset;
        private int pos;

        RecordParser(byte[] data, int offset, int payloadSize) {
            this.data = data;
            this.startOffset = offset;
            this.payloadSize = payloadSize;
            this.endOffset = offset + payloadSize;
            this.pos = offset;

            // Validate inputs
            if (offset < 0 || offset >= data.length) {
                throw new IllegalArgumentException("Invalid offset: " + offset);
            }
            if (payloadSize < 0 || offset + payloadSize > data.length) {
                throw new IllegalArgumentException("Invalid payload size: " + payloadSize +
                        " at offset " + offset +
                        " (data length: " + data.length + ")");
            }
        }

        SQLiteRecord parse() {

            if (pos >= endOffset) {
                throw new IllegalArgumentException("No data for header size varint");
            }

            // Read header size varint
            long[] result = VarintDecoder.decodeVarint(data, pos);
            long headerSizeValue = result[0];
            int headerSizeBytes = (int)result[1];

            System.out.println("DEBUG: At position " + pos + ", read header size: " +
                    headerSizeValue + " (varint bytes: " + headerSizeBytes + ")");
            System.out.println("DEBUG: Payload size limit: " + payloadSize);

            if (headerSizeValue > payloadSize) {
                throw new IllegalArgumentException("Header size " + headerSizeValue +
                        " exceeds payload size " + payloadSize);
            }

            pos += headerSizeBytes;

            int headerSize = (int)headerSizeValue;
            int headerEnd = startOffset + headerSize;

            // Read all serial types
            List<Long> serialTypes = new ArrayList<>();
            while (pos < headerEnd) {
                result = VarintDecoder.decodeVarint(data, pos);
                serialTypes.add(result[0]);
                pos += (int)result[1];
            }

            // Now parse the actual values
            List<Object> values = new ArrayList<>();
            for (Long serialType : serialTypes) {
                Object value = parseValue(serialType);
                values.add(value);
            }

            int dataSize = pos - (startOffset + headerSize);
            return new SQLiteRecord(serialTypes, values, headerSize, dataSize);
        }

        private Object parseValue(long serialType) {
            if (serialType == 0) {
                return null; // NULL
            } else if (serialType == 1) {
                return readInt8();
            } else if (serialType == 2) {
                return readInt16();
            } else if (serialType == 3) {
                return readInt24();
            } else if (serialType == 4) {
                return readInt32();
            } else if (serialType == 5) {
                return readInt48();
            } else if (serialType == 6) {
                return readInt64();
            } else if (serialType == 7) {
                return readFloat64();
            } else if (serialType == 8) {
                return 0L; // Integer constant 0
            } else if (serialType == 9) {
                return 1L; // Integer constant 1
            } else if (serialType >= 12 && serialType % 2 == 0) {
                // BLOB
                int length = (int)(serialType - 12) / 2;
                return readBlob(length);
            } else if (serialType >= 13) {
                // TEXT
                int length = (int)(serialType - 13) / 2;
                return readText(length);
            } else {
                throw new IllegalArgumentException("Unknown serial type: " + serialType);
            }
        }

        private long readInt8() {
            byte value = data[pos++];
            return value; // Sign extension is automatic
        }

        private long readInt16() {
            int value = ((data[pos] & 0xFF) << 8) |
                    (data[pos + 1] & 0xFF);
            pos += 2;
            // Sign extend if negative
            if ((value & 0x8000) != 0) {
                value |= 0xFFFF0000;
            }
            return value;
        }

        private long readInt24() {
            int value = ((data[pos] & 0xFF) << 16) |
                    ((data[pos + 1] & 0xFF) << 8) |
                    (data[pos + 2] & 0xFF);
            pos += 3;
            // Sign extend if negative
            if ((value & 0x800000) != 0) {
                value |= 0xFF000000;
            }
            return value;
        }

        private long readInt32() {
            int value = ByteBuffer.wrap(data, pos, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            pos += 4;
            return value;
        }

        private long readInt48() {
            long value = 0;
            for (int i = 0; i < 6; i++) {
                value = (value << 8) | (data[pos + i] & 0xFF);
            }
            pos += 6;
            // Sign extend if negative
            if ((value & 0x800000000000L) != 0) {
                value |= 0xFFFF000000000000L;
            }
            return value;
        }

        private long readInt64() {
            long value = ByteBuffer.wrap(data, pos, 8)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getLong();
            pos += 8;
            return value;
        }

        private double readFloat64() {
            double value = ByteBuffer.wrap(data, pos, 8)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getDouble();
            pos += 8;
            return value;
        }

        private byte[] readBlob(int length) {
            byte[] blob = new byte[length];
            System.arraycopy(data, pos, blob, 0, length);
            pos += length;
            return blob;
        }

        private String readText(int length) {
            String text = new String(data, pos, length, StandardCharsets.UTF_8);
            pos += length;
            return text;
        }
    }

    // Helper class to get column type info
    public static class ColumnType {
        public enum Type {
            NULL, INTEGER, FLOAT, TEXT, BLOB
        }

        public final Type type;
        public final int size;

        private ColumnType(Type type, int size) {
            this.type = type;
            this.size = size;
        }

        public static ColumnType fromSerialType(long serialType) {
            if (serialType == 0) {
                return new ColumnType(Type.NULL, 0);
            } else if (serialType >= 1 && serialType <= 6) {
                return new ColumnType(Type.INTEGER, getIntegerBytes(serialType));
            } else if (serialType == 7) {
                return new ColumnType(Type.FLOAT, 8);
            } else if (serialType == 8 || serialType == 9) {
                return new ColumnType(Type.INTEGER, 0); // Constants
            } else if (serialType >= 12 && serialType % 2 == 0) {
                int length = (int)(serialType - 12) / 2;
                return new ColumnType(Type.BLOB, length);
            } else if (serialType >= 13) {
                int length = (int)(serialType - 13) / 2;
                return new ColumnType(Type.TEXT, length);
            }
            throw new IllegalArgumentException("Unknown serial type: " + serialType);
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
    }
}
