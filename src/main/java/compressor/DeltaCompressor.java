package compressor;

import java.util.List;

public class DeltaCompressor {
    /**
     * we will call this function
     * @param valueList value list
     */
    public static long[] compress(List<Long> valueList) {
        int size = valueList.size();
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        for(long value : valueList) {
            if(value < minValue) {
                minValue = value;
            }
            if(value > maxValue) {
                maxValue = value;
            }
        }
        if(maxValue - minValue > 0x3fffffff){
            throw new RuntimeException("cannot call fixed delta compression algorithm");
        }

        // total size: 8 + size * 4 bytes
        int longValNum = 1 + (size >> 1) + (0x1 & size);
        long[] result = new long[longValNum];
        result[0] = minValue;
        //System.out.println("minValue: " + minValue);
        for(int i = 0; i < size; i++) {
            int shift = (i & 0x1) == 0 ? 0 : 32;
            int pos = (i >> 1) + 1;
            long delta = valueList.get(i) - minValue;
            //System.out.println("delat: " + Long.toHexString(delta));
            result[pos] |= (delta << shift);
            //System.out.println("result[" + pos + "]: " + Long.toHexString(result[pos]));
        }
        return result;
    }
}
