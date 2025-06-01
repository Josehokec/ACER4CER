package acer;

import common.IndexValuePair;
import compressor.*;
import condition.ICQueryQuad;
import store.RID;

import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * [updated] this is a new index block support fine/cluster-granularity filtering and reading
 * Load on demand instead of loading all the contents of the index block
 */
public class MappedIdxBlk {
    private final RangeBitmap[] rangeBitmaps;

    private final int startPos;
    private final int offset;
    //private final long[] minValues;
    // * @param minValues             minimum values in a given cluster

    private long[] clusterTsList;
    private long[] clusterRIDList;

    /**
     * Load on demand instead of loading all the contents of the index block
     * @param metaInfo              meta information for a given index block
     * @param clusterId             cluster id
     * @param startPos              start position
     * @param offset                number of events for a given cluster
     * @param indexAttrNum          number of indexed attributes
     * @param idxs                  attribute idxs in variable's condition
     * @param fileChannel           file channel
     * @throws IOException          exception
     */
    public MappedIdxBlk(IdxBlkMetaInfo metaInfo, int clusterId, int startPos, int offset,
                        int indexAttrNum, int[] idxs, FileChannel fileChannel) throws IOException {
        // we support fine-grained reading of index blocks

        this.startPos = startPos;
        this.offset = offset;

        rangeBitmaps = new RangeBitmap[indexAttrNum];

        // read range bitmap...
        long[] rbStartPos = new long[indexAttrNum + 1];
        rbStartPos[0] = metaInfo.storagePosition();
        int[] sizes = metaInfo.sizes();
        for (int i = 1; i <= indexAttrNum; i++) {
            rbStartPos[i] = rbStartPos[i - 1] + sizes[i-1];
        }
        for(int i : idxs) {
            ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, rbStartPos[i], sizes[i]);
            rangeBitmaps[i] = RangeBitmap.map(buffer);
        }

        ByteBuffer tsListBuffer;
        ByteBuffer ridListBuffer;
        int eventNum = Parameters.CAPACITY;

        if(Parameters.OPTIMIZED_LAYOUT){
            eventNum = offset;

            // |rbSize_1|....|rbSize_N|tsSize_1|ridSize_1|...|tsSize_g|ridSize_g|
            long tsStartPos = rbStartPos[indexAttrNum];
            int ptr = indexAttrNum;
            for(int i = 0; i < clusterId; i++){
                tsStartPos += (sizes[ptr] + sizes[ptr + 1]);
                ptr += 2;
            }
            long ridStartPos = tsStartPos + sizes[ptr];
            tsListBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, tsStartPos, sizes[ptr]);
            ridListBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, ridStartPos, sizes[ptr + 1]);
        }else{
            long tsListPosition = rbStartPos[indexAttrNum];
            long ridListPosition = tsListPosition + sizes[indexAttrNum];
            tsListBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, tsListPosition, sizes[indexAttrNum]);
            ridListBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, ridListPosition, sizes[indexAttrNum + 1]);
        }

        long[] tsList;
        long[] ridList;

        switch (Parameters.COMPRESSOR) {
            case DELTA_2 -> {
                tsList = DeltaOfDeltaDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaOfDeltaDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case VAR_INT -> {
                tsList = DeltaVarIntDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaVarIntDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case SIMPLE_8B -> {
                tsList = DeltaSimple8BDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaSimple8BDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case DELTA -> {
                tsList = DeltaDecompressor.decompress(tsListBuffer, eventNum);
                ridList = DeltaDecompressor.decompress(ridListBuffer, eventNum);
            }
            default -> throw new RuntimeException("Unsupported compression type: " + Parameters.COMPRESSOR);
        }

        // if disable optimization, we need to perform a truncation operation
        if(Parameters.OPTIMIZED_LAYOUT){
            clusterTsList = tsList;
            clusterRIDList = ridList;
        }else{
            clusterTsList = new long[offset];
            clusterRIDList = new long[offset];
            for(int i = 0; i < offset; i++) {
                clusterTsList[i] = tsList[startPos + i];
                clusterRIDList[i] = ridList[startPos + i];
            }
        }
    }


    /**
     * [old version] Load entire index block from disk
     * maybe this function has a faster speed
     * @param metaInfo              meta information for a given index block
     * @param clusterId             cluster id
     * @param startPos              start position
     * @param offset                number of events for a given cluster
     * @param indexAttrNum          number of indexed attributes
     * @param fileChannel           file channel
     * @throws IOException          exception
     */
    public MappedIdxBlk(IdxBlkMetaInfo metaInfo, int clusterId, int startPos, int offset,
                        int indexAttrNum, FileChannel fileChannel) throws IOException {
        long storagePosition = metaInfo.storagePosition();
        int blkSize = metaInfo.blockSize();
        ByteBuffer entireBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, storagePosition, blkSize);

        this.startPos = startPos;
        this.offset = offset;

        rangeBitmaps = new RangeBitmap[indexAttrNum];

        // read range bitmap...
        int[] rbStartPos = new int[indexAttrNum + 1];
        rbStartPos[0] = 0;
        int[] sizes = metaInfo.sizes();
        for (int i = 1; i <= indexAttrNum; i++) {
            rbStartPos[i] = rbStartPos[i - 1] + sizes[i-1];
        }
        for(int i = 0; i < indexAttrNum; i++) {
            byte[] rb = new byte[sizes[i]];
            entireBuffer.get(rb);
            ByteBuffer rbBuffer = ByteBuffer.wrap(rb);
            rangeBitmaps[i] = RangeBitmap.map(rbBuffer);
        }

        int eventNum = Parameters.CAPACITY;
        ByteBuffer tsListBuffer;
        ByteBuffer ridListBuffer;
        if(Parameters.OPTIMIZED_LAYOUT){
            eventNum = offset;
            // |rbSize_1|....|rbSize_N|tsSize_1|ridSize_1|...|tsSize_g|ridSize_g|
            int tsStartPos = rbStartPos[indexAttrNum];
            int ptr = indexAttrNum;
            for(int i = 0; i < clusterId; i++){
                tsStartPos += (sizes[ptr] + sizes[ptr + 1]);
                ptr += 2;
            }
            int ridStartPos = tsStartPos + sizes[ptr];
            byte[] tsBytes = new byte[sizes[ptr]];
            tsListBuffer = entireBuffer.get(tsStartPos, tsBytes);
            byte[] ridBytes = new byte[sizes[ptr + 1]];
            ridListBuffer = entireBuffer.get(ridStartPos, ridBytes);
        }else{
            int tsListPosition = rbStartPos[indexAttrNum];
            int ridListPosition = tsListPosition + sizes[indexAttrNum];
            byte[] tsBytes = new byte[sizes[indexAttrNum]];
            tsListBuffer = entireBuffer.get(tsListPosition, tsBytes);
            byte[] ridBytes = new byte[sizes[indexAttrNum + 1]];
            ridListBuffer = entireBuffer.get(ridListPosition, ridBytes);
        }
        long[] tsList;
        long[] ridList;

        switch (Parameters.COMPRESSOR) {
            case DELTA_2 -> {
                tsList = DeltaOfDeltaDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaOfDeltaDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case VAR_INT -> {
                tsList = DeltaVarIntDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaVarIntDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case SIMPLE_8B -> {
                tsList = DeltaSimple8BDecompressor.decompress(new ByteBufferBitInput(tsListBuffer), eventNum);
                ridList = DeltaSimple8BDecompressor.decompress(new ByteBufferBitInput(ridListBuffer), eventNum);
            }
            case DELTA -> {
                tsList = DeltaDecompressor.decompress(tsListBuffer, eventNum);
                ridList = DeltaDecompressor.decompress(ridListBuffer, eventNum);
            }
            default -> throw new RuntimeException("Unsupported compression type: " + Parameters.COMPRESSOR);
        }

        // if disable optimization, we need to perform a truncation operation
        if(!Parameters.OPTIMIZED_LAYOUT){
            clusterTsList = new long[offset];
            clusterRIDList = new long[offset];
            for(int i = 0; i < offset; i++) {
                clusterTsList[i] = tsList[startPos + i];
                clusterRIDList[i] = ridList[startPos + i];
            }
        }
    }

    public List<IndexValuePair> query(List<ICQueryQuad> icQuads){
        RoaringBitmap context = new RoaringBitmap();
        context.add((long) startPos, (offset + startPos));

        RoaringBitmap bitmap = Parameters.ENABLE_TRUNCATE ? null : context;

        for (ICQueryQuad quad : icQuads) {
            RoaringBitmap ans = queryRangeBitmapUsingIC(quad, context);
            if(bitmap == null){
                bitmap = ans;
            }else{
                bitmap.and(ans);
            }
        }
        // note that icQuads maybe empty, so we add this code line
        bitmap = (bitmap == null) ? context : bitmap;

        return getIndexValuePairs(bitmap);
    }

    private RoaringBitmap queryRangeBitmapUsingIC(ICQueryQuad quad, RoaringBitmap context) {
        int idx = quad.idx();
        // case 1: mark = 1, only have minimum value
        // case 2: mark = 2, only have maximum value
        // mark = 3, have minimum and maximum values
        int mark = quad.mark();
        RoaringBitmap rangeQueryRB;
        switch (mark) {
            case 1 -> rangeQueryRB = Parameters.ENABLE_TRUNCATE ?
                    rangeBitmaps[idx].gte(quad.min(), context) : rangeBitmaps[idx].gte(quad.min());
            case 2 -> rangeQueryRB = Parameters.ENABLE_TRUNCATE ?
                    rangeBitmaps[idx].lte(quad.max(), context) : rangeBitmaps[idx].lte(quad.max());
            case 3 -> {
                if(Parameters.ENABLE_TRUNCATE){
                    rangeQueryRB = rangeBitmaps[idx].gte(quad.min(), context);
                    rangeQueryRB.and(rangeBitmaps[idx].lte(quad.max(), context));
                }else{
                    rangeQueryRB = rangeBitmaps[idx].between(quad.min(), quad.max());
                }
            }
            default -> throw new RuntimeException("mark value must be {1, 2, 3}: " + mark);
        }
        return rangeQueryRB;
    }

    /**
     * note that we must subtract the offset
     * @param bitmap        indicate which locations/positions are selected
     * @return              index value pairs
     */
    public List<IndexValuePair> getIndexValuePairs(RoaringBitmap bitmap) {
        int len = bitmap.getCardinality();
        List<IndexValuePair> ans = new ArrayList<>(len);
        bitmap.forEach((Consumer<? super Integer>) i -> {
            int pos = i - startPos;
            long timestamp = clusterTsList[pos];
            long value = clusterRIDList[pos];
            RID rid = new RID((int) (value >>> 16), (short) (value & 0xffff));
            ans.add(new IndexValuePair(timestamp, rid));
        });
        return ans;
    }
}
