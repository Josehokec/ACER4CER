package compressor;

import java.util.List;

/**
 * delta of delta encoding
 * ts_1, ts_2, ..., ts_k
 * source code url: https://github.com/burmanm/gorilla-tsc/
 */
public class DeltaOfDeltaCompressor{
    private final long startTimestamp;
    private long previousTimestamp = 0;
    private int storedDelta = 0;
    public final static int FIRST_DELTA_BITS = 27;
    // DELTAD_7_MASK (9 bits): 0000 0001 0000 0000
    private static int DELTAD_7_MASK = 0x02 << 7;
    // DELTAD_9_MASK (12 bits): 0000 1100 0000 0000
    private static int DELTAD_9_MASK = 0x06 << 9;
    // DELTAD_12_MASK (16 bits): 1110 0000 0000 0000
    private static int DELTAD_12_MASK = 0x0E << 12;

    private BitOutput out;

    public DeltaOfDeltaCompressor(long timestamp, BitOutput output) {
        startTimestamp = timestamp;
        out = output;
        addHeader(timestamp);
    }

    private void addHeader(long timestamp) {
        out.writeBits(timestamp, 64);
    }

    /**
     * @param timestamp event timestamp or other long value
     * if this value is the first item, then calculate delta value
     */
    public final void addValue(long timestamp) {
        if(previousTimestamp == 0) {
            storedDelta = (int) (timestamp - startTimestamp);
            previousTimestamp = timestamp;
            out.writeBits(storedDelta, FIRST_DELTA_BITS);
        }else {
            // a) Calculate the delta of delta
            int newDelta = (int) (timestamp - previousTimestamp);
            int deltaD = newDelta - storedDelta;

            if(deltaD == 0) {
                out.skipBit();
            } else {
                deltaD = ZigZagCompressor.encodeZigZag32(deltaD);
                deltaD--; // Increase by one in the decompressing phase as we have one free bit
                int bitsRequired = 32 - Integer.numberOfLeadingZeros(deltaD); // Faster than highestSetBit

                // Turns to inlineable tableswitch
                switch(bitsRequired) {
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        deltaD |= DELTAD_7_MASK;
                        out.writeBits(deltaD, 9);
                        break;
                    case 8:
                    case 9:
                        deltaD |= DELTAD_9_MASK;
                        out.writeBits(deltaD, 12);
                        break;
                    case 10:
                    case 11:
                    case 12:
                        out.writeBits(deltaD | DELTAD_12_MASK, 16);
                        break;
                    default:
                        out.writeBits(0x0F, 4); // Store '1111'
                        out.writeBits(deltaD, 32); // Store delta using 32 bits
                        break;
                }
                storedDelta = newDelta;
            }

            previousTimestamp = timestamp;
        }
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public void close() {
        out.writeBits(0x0F, 4);
        out.writeBits(0xFFFFFFFF, 32);
        out.skipBit();
        out.flush();
    }

    /**
     * we will call this function
     * @param valueList value list
     */
    public static long[] compress(List<Long> valueList) {
        // note that a long value occupies 8 bytes, however, a compressed value may only need 2 bytes
        // when has exception, you need to expand the size
        LongArrayOutput output = new LongArrayOutput(valueList.size() * 8);
        DeltaOfDeltaCompressor delta2 = new DeltaOfDeltaCompressor(valueList.get(0), output);
        for(Long value : valueList) {
            delta2.addValue(value);
        }
        delta2.close();
        return output.getLongArray();
    }
}
