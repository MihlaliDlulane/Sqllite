package helpers;

public class VarintDecoder {

    // Decode a varint and return [value, bytesRead]

    public static long[] decodeVarint(byte[] data, int offset) {
        long value = 0;
        int bytesRead = 0;

        for (int i = 0; i < 9; i++) {
            if (offset + i >= data.length) {
                throw new IllegalArgumentException("Varint extends beyond data");
            }

            byte b = data[offset + i];

            if (i < 8) {
                value = (value << 7) | (b & 0x7F);
                bytesRead++;

                if ((b & 0x80) == 0) {
                    break;
                }
            } else {
                value = (value << 8) | (b & 0xFF);
                bytesRead = 9;
                break;
            }
        }

        return new long[]{value, bytesRead};
    }

}
