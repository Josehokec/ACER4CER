package compressor;

import java.util.List;

/**
 * original data (k data points): ts_1, ..., ts_k
 * given a start timestamp ts_0, then compressed data as follows:
 * ts_0, varint(zigzig(delta_1)), ..., varint(zigzig(delta_k)),
 * where delta_i = ts_i - ts_{i-1}
 */
public class DeltaVarIntCompressor {
    private long previousTimestamp;
    private BitOutput out;

    public DeltaVarIntCompressor(long timestamp, BitOutput output) {
        previousTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
    }

    /**
     * due to varint cannot define an end marker, we need to know the number of points
     * @param timestamp start timestamp
     */
    private void addHeader(long timestamp) {
        out.writeBits(timestamp, 64);
        //out.writeBits(itemNum, 32);
    }

    /**
     * @param timestamp event timestamp or other long value
     * if this value is the first item, then calculate delta value
     */
    public final void addValue(long timestamp) {
        if((timestamp - previousTimestamp) > Integer.MAX_VALUE){
            throw new RuntimeException("The gap between adjacent timestamps is too large, we may need to fix code to support long value");
        }
        int delta = (int) (timestamp - previousTimestamp);
        // delta maybe negative, so we need use zigzig to transform positive
        int zigzigDelta = ZigZagCompressor.encodeZigZag32(delta);
        // 0 means end
        // Original code:   0001111   1011111
        // ->VarInt code: 1 0001111 0 1011111
        if (zigzigDelta < 128) {
            // 0 means end, write 8 bits, highest bit is 0, format: 0XXX XXXX
            // System.out.println("writeValue: " + Long.toBinaryString(zigzigDelta));
            out.writeBits(zigzigDelta, 8);
        } else if (zigzigDelta < 16384) {
            // System.out.println("write 2 byte, " + Long.toBinaryString(zigzigDelta));
            long writeValue = (((zigzigDelta >>> 7) | 0x80) << 8) + (zigzigDelta & 0x7f);
            // System.out.println("writeValue: " + Long.toBinaryString(writeValue));
            out.writeBits(writeValue, 16);
        } else if (zigzigDelta < 2097152) {
            // System.out.println("write 3 byte, " + Long.toBinaryString(zigzigDelta));
            long writeValue = (((zigzigDelta >>> 14) | 0x80) << 16) + (((zigzigDelta >>> 7) & 0x7f | 0x80) << 8) + (zigzigDelta & 0x7f);
            // System.out.println("writeValue: " + Long.toBinaryString(writeValue));
            out.writeBits(writeValue, 24);
        } else if (zigzigDelta < 268435456) {
            // System.out.println("write 4 byte, " + Long.toBinaryString(zigzigDelta));
            long writeValue = (((zigzigDelta >>> 21) | 0x80L) << 24) + (((zigzigDelta >>> 14) & 0x7f | 0x80) << 16)
                    + (((zigzigDelta >>> 7) & 0x7f | 0x80) << 8) + (zigzigDelta & 0x7f);
            // System.out.println("writeValue: " + Long.toBinaryString(writeValue));
            out.writeBits(writeValue, 32);
        } else {
            //System.out.println("write 5 byte, " + Long.toBinaryString(zigzigDelta));
            long writeValue = (((zigzigDelta >>> 28) | 0x80L) << 32) + (((zigzigDelta >>> 21) & 0x7f | 0x80L) << 24) +
                    (((zigzigDelta >>> 14) & 0x7f | 0x80) << 16) + (((zigzigDelta >>> 7) & 0x7f | 0x80) << 8) + (zigzigDelta & 0x7f);
            // System.out.println("writeValue: " + Long.toBinaryString(writeValue));
            out.writeBits(writeValue, 40);
        }
        previousTimestamp = timestamp;
    }

    /**
     * we will call this function
     * @param valueList value list
     */
    public static long[] compress(List<Long> valueList) {
        int itemNum = valueList.size();
        // note that a long value occupies 8 bytes, however, a compressed value may only need 2 bytes
        LongArrayOutput output = new LongArrayOutput(itemNum * 8);
        DeltaVarIntCompressor deltaVarint = new DeltaVarIntCompressor(valueList.get(0), output);
        for(Long value : valueList) {
            deltaVarint.addValue(value);
        }
        return output.getLongArray();
    }
}