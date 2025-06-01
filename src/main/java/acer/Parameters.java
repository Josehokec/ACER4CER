package acer;

import compressor.CompressorAlgorithm;

public class Parameters {
    // enable optimization of index block layout
    static final boolean OPTIMIZED_LAYOUT = true;

    // compressor
    static final CompressorAlgorithm COMPRESSOR = CompressorAlgorithm.SIMPLE_8B;

    // capacity of buffer pool
    static final int CAPACITY = 64 * 1024;

    public static final int PAGE_SIZE = 8 * 1024;

    // please do not change this parameter
    // when true/enable ==> query range bitmap call bellow function
    //     rb.gte(long value, RoaringBitmap context) or rb.lte(long value, RoaringBitmap context)
    // if no, then call rb.gte(long value) or rb.lte(long value) or rb.between(long min, long max)
    static final boolean ENABLE_TRUNCATE = true;
}
