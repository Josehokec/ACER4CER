package compressor;

/**
 * delta of delta encoding
 * ts_1, ts_2, ..., ts_k
 * source code url: https://github.com/burmanm/gorilla-tsc/
 */
public class DeltaOfDeltaDecompressor {
    private long previousTimestamp = 0;
    private long storedDelta = 0;

    private long startTimestamp = 0;
    private boolean endOfStream = false;

    private final BitInput in;

    public DeltaOfDeltaDecompressor(BitInput input) {
        in = input;
        readHeader();
    }

    private void readHeader() {
        startTimestamp = in.getLong(64);
    }

    public long readTimestamp() {
        next();
        if(endOfStream) {
            return -1;
        }
        return previousTimestamp;
    }

    private void next() {
        // TODO I could implement a non-streaming solution also.. is there ever a need for streaming solution?
        if(previousTimestamp == 0) {
            first();
            return;
        }

        nextTimestamp();
    }

    private void first() {
        // First item to read
        storedDelta = in.getLong(DeltaOfDeltaCompressor.FIRST_DELTA_BITS);
        if(storedDelta == (1<<27) - 1) {
            endOfStream = true;
            return;
        }
        previousTimestamp = startTimestamp + storedDelta;
    }

    private void nextTimestamp() {
        // Next, read timestamp
        int readInstruction = in.nextClearBit(4);
        long deltaDelta;

        switch(readInstruction) {
            case 0x00:
                previousTimestamp = storedDelta + previousTimestamp;
                return;
            case 0x02:
                deltaDelta = in.getLong(7);
                break;
            case 0x06:
                deltaDelta = in.getLong(9);
                break;
            case 0x0e:
                deltaDelta = in.getLong(12);
                break;
            case 0x0F:
                deltaDelta = in.getLong(32);
                // For storage save.. if this is the last available word, check if remaining bits are all 1
                if ((int) deltaDelta == 0xFFFFFFFF) {
                    // End of stream
                    endOfStream = true;
                    return;
                }
                break;
            default:
                return;
        }

        deltaDelta++;
        deltaDelta = ZigZagCompressor.decodeZigZag32((int) deltaDelta);
        storedDelta = storedDelta + deltaDelta;

        previousTimestamp = storedDelta + previousTimestamp;
    }

    // define a unified interface
    public static long[] decompress(BitInput in, int itemNum) {
        assert(itemNum > 0);
        DeltaOfDeltaDecompressor decompressor = new DeltaOfDeltaDecompressor(in);
        long readTs = decompressor.readTimestamp();
        long[] realValues = new long[itemNum];
        int count = 0;
        // && count < itemNum
        while(readTs != -1){
            realValues[count++] = readTs;
            readTs = decompressor.readTimestamp();
        }
        assert(count == itemNum);
        return realValues;
    }
}
