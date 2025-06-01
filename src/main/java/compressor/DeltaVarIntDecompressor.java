package compressor;

public class DeltaVarIntDecompressor{
    private long previousTimestamp = 0;
    //private long itemNum;
    private long readCount = 0;
    private final BitInput in;

    public DeltaVarIntDecompressor(BitInput input) {
        in = input;
        readHeader();
    }

    private void readHeader() {
        previousTimestamp = in.getLong(64);
        //itemNum = in.getLong(32);
    }

    public long readTimestamp(){
        //if(readCount == itemNum){
        //    return -1;
        //}
        long delta = 0;
        long byteValue = in.getLong(8);
        //System.out.println("readValue: " + Long.toBinaryString(byteValue));
        // if is "1XXX XXXX"
        while(byteValue >= 128){
            delta = (delta << 7) + (byteValue & 0x7f);
            // System.out.println("delta: " + delta);
            byteValue = in.getLong(8);
            // System.out.println("readValue: " + Long.toBinaryString(byteValue));
        }
        delta = (delta << 7) + (byteValue & 0x7f);
        // System.out.println("delta: " + delta);
        // System.out.println("decodeZigZag32: " + delta);
        long ts = previousTimestamp + ZigZagCompressor.decodeZigZag32((int) delta);
        previousTimestamp = ts;
        readCount++;
        return ts;
    }

    // define a unified interface
    public static long[] decompress(BitInput in, int itemNum) {
        assert(itemNum > 0);
        DeltaVarIntDecompressor decompressor = new DeltaVarIntDecompressor(in);
        long[] realValues = new long[itemNum];
        for(int i = 0; i < itemNum; i++){
            realValues[i] = decompressor.readTimestamp();
        }
//        for(long v : realValues){
//            System.out.println("decompressed value: " + v);
//        }
        return realValues;
    }

}
