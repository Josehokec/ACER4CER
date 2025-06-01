package compressor;

import java.util.Arrays;

/**
 * delta-of-delta + ZigZig
 * An implementation of BitOutput interface that uses on-heap long array.
 * @author Michael Burman
 */
public class LongArrayOutput implements BitOutput {
    private long[] longArray;
    private int position = 0;

    protected long lB;                              // byte buffer has been written
    protected int bitsLeft = Long.SIZE;             // 64 bits

    public final static long[] MASK_ARRAY;
    public final static long[] BIT_SET_MASK;

    // Java does not allow creating 64 bit masks with (1L << 64) - 1; (end result is 0)
    static {
        MASK_ARRAY = new long[64];
        long mask = 1;
        long value = 0;
        for (int i = 0; i < MASK_ARRAY.length; i++) {
            value = value | mask;
            mask = mask << 1;

            MASK_ARRAY[i] = value;
        }

        BIT_SET_MASK = new long[64];
        for(int i = 0; i < BIT_SET_MASK.length; i++) {
            BIT_SET_MASK[i] = (1L << i);
        }
    }

    /**
     * a compressed timestamp about 4 byte (or less)
     * RID about 4 byte (or less)
     * @param expectedByteNum the expected byte number
     */
    public LongArrayOutput(int expectedByteNum) {
        int size = Math.max(expectedByteNum >>> 3, 4);
        longArray = new long[size];
        lB = longArray[position];
    }

    /**
     * reach it capacity, we need to expand it
     */
    protected void expandAllocation() {
        int newCapacity = longArray.length + longArray.length >> 1;
        long[] largerArray = new long[newCapacity];
        System.arraycopy(longArray, 0, largerArray, 0, longArray.length);
        longArray = largerArray;
    }

    private void checkAndFlipByte() {
        // Wish I could avoid this check in most cases...
        if(bitsLeft == 0) {
            flipWord();
        }
    }

    protected int capacityLeft() {
        return longArray.length - position;
    }

    protected void flipWord() {
        // We want to have always at least 2 longs available
        if(capacityLeft() <= 2) {
            expandAllocation();
        }
        flipWordWithoutExpandCheck();
    }

    protected void flipWordWithoutExpandCheck() {
        longArray[position] = lB;
        ++position;
        resetInternalWord();
    }

    private void resetInternalWord() {
        lB = 0;
        bitsLeft = Long.SIZE;
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     */
    public void writeBit() {
        lB |= BIT_SET_MASK[bitsLeft - 1];
        bitsLeft--;
        checkAndFlipByte();
    }

    public void skipBit() {
        bitsLeft--;
        checkAndFlipByte();
    }

    /**
     * Writes the given long to the stream using bits amount of meaningful bits. This command does not
     * check input values, so if they're larger than what can fit the bits (you should check this before writing),
     * expect some weird results.
     *
     * @param value Value to be written to the stream
     * @param bits How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        if(bits <= bitsLeft) {
            int lastBitPosition = bitsLeft - bits;
            lB |= (value << lastBitPosition) & MASK_ARRAY[bitsLeft - 1];
            bitsLeft -= bits;
            checkAndFlipByte(); // We could be at 0 bits left because of the <= condition .. would it be faster with
            // the other one?
        } else {
            value &= MASK_ARRAY[bits - 1];
            int firstBitPosition = bits - bitsLeft;
            lB |= value >>> firstBitPosition;
            bits -= bitsLeft;
            flipWord();
            lB |= value << (64 - bits);
            bitsLeft -= bits;
        }
    }

    /**
     * Causes the currently handled word to be written to the stream
     */
    @Override
    public void flush() {
        flipWord();
    }

    public long[] getLongArray() {
        long[] copy = Arrays.copyOf(longArray, position + 1);
        copy[copy.length - 1] = lB;
        return copy;
    }

}
