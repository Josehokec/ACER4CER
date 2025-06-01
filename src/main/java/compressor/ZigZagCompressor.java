package compressor;

public class ZigZagCompressor {

    // START: From protobuf
    /**
     * Encode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    public static int encodeZigZag32(final int n) {
        // Note:  the right-shift must be arithmetic
        return (n << 1) ^ (n >> 31);
    }

    public static int decodeZigZag32(final int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    // END: From protobuf
}
