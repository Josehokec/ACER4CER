package compressor;

import java.nio.ByteBuffer;

public class DeltaDecompressor {
    public static long[] decompress(ByteBuffer in, int itemNum){
        long[] realValues = new long[itemNum];
        long minValue = in.getLong();
        //System.out.println("minValue: " + minValue);
        for(int i = 0; i < itemNum / 2; i++){
            long value = in.getLong();
            //System.out.println("result[" + (i + 1) + "]" + Long.toHexString(value));
            long lowValue = value & 0x0ffffffffL;
            long highValue = value >>> 32;
            realValues[i * 2] = lowValue + minValue;
            realValues[i * 2 + 1] = highValue + minValue;
        }
        if((itemNum & 0x1) == 1){
            realValues[itemNum - 1] = minValue + in.getLong();
        }
        return realValues;
    }
}
