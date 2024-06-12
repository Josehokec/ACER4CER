package acer;

import common.IndexValuePair;
import condition.IndependentConstraintQuad;
import store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


/**
 * we have re-design IndexPartition class
 * fast index should store total byte size for an index partition
 * considering an index block store a large events, we have to compress
 * timestamp (8 bytes) and RID (8 bytes)
 * Suppose an index block has g events,
 * <Compress Algorithm>
 * Origin value list: timestamp_1, ..., timestamp_g
 * Compressed value list: startTimestamp, differValue_1, ..., differValue_g
 * timestamp_i (long) = startTimestamp (long) + differValue_i (int)
 * Origin RID list: rid_1 <page_1 (int), offset_1 (int)>, ..., rid_g <page_g (int), offset_g (int)>
 * Compressed RID list: startPage, <differPage_1 (short), offset_1 (short)>, ...
 */
public record IndexBlock(RoaringBitmap deletionMark, RangeBitmap[] rangeBitmaps, int eventNum,
                                   long startTimestamp, ByteBuffer timestampListBuff,
                                   int startPage, ByteBuffer ridListBuff) {


    public static ByteBuffer serialize(RoaringBitmap deletionMark, List<RangeBitmap.Appender> appenderList, List<Long> timestampList, List<RID> ridList) {
        /*
        write an index block to byte buffer
        suppose: h = indexNum - 1, g = N_f - 1
        then an index partition storage format as follows:
        |roaring bitmap size|roaring bitmap|
        |indexNum (int)|rbSize_1|rangeBitmap_1|...|rbSize_h|rangeBitmap_h|
        |g (int)|startTimestamp (long)|differTimestamp_1 (int)|...|differTimestamp_g|
        |startPage (int)|differPage_1 (short),offset_1 (short)|...|differPage_g,offset_g|
        */

        int deletionSize = deletionMark.serializedSizeInBytes();

        // step 1. calculate an index block size, and check
        int indexNum = appenderList.size();
        int[] sizes = new int[indexNum];
        int rangBitmapSumSize = 0;
        // tip: we should call 'serializedSizeInBytes' function before call 'serialize' function
        for(int i = 0; i < indexNum; ++i){
            sizes[i] = appenderList.get(i).serializedSizeInBytes();
            rangBitmapSumSize += sizes[i];
        }

        int g = timestampList.size();

        // 4 + 4 + size_0 + ... + 4 + size_h + 4 + g * 8 + g * 8
        int indexByteSize = 4 + 4 * indexNum + rangBitmapSumSize + 4 + deletionSize;
        int timeRidByteSize = 4 + 8 + 4 * g + 4 + 4 * g;
        int sumByteSize = indexByteSize + timeRidByteSize;
        // 4kB page = 4096 bytes
        int kBPageNum = (int) Math.ceil(sumByteSize / 1024.0);

        // add roaring bitmap
        ByteBuffer ans = ByteBuffer.allocate(kBPageNum * 1024);
        ans.putInt(deletionSize);
        ByteBuffer deletionBuff = ByteBuffer.allocate(deletionSize);
        deletionMark.serialize(deletionBuff);
        deletionBuff.flip();
        ans.put(deletionBuff);

        // step 2. write range bitmap to buffer
        ans.putInt(indexNum);
        for (int i = 0; i < indexNum; ++i) {
            int curSize = sizes[i];
            ans.putInt(curSize);
            ByteBuffer buff = ByteBuffer.allocate(curSize);
            // serialize range bitmap appender to buff
            appenderList.get(i).serialize(buff);
            // ensure to switch to write mode, if without flip, we cannot get answer
            buff.flip();
            // write byte buffer
            ans.put(buff);
        }

        long startTimestamp = timestampList.get(0);
        ans.putInt(g);
        ans.putLong(startTimestamp);
        // step 3. write timestamp list
        for (Long ts : timestampList) {
            long diffTimestamp = ts - startTimestamp;
            if(diffTimestamp < Integer.MIN_VALUE || diffTimestamp > Integer.MAX_VALUE){
                System.out.println("Difference timestamp: " + diffTimestamp);
                throw new RuntimeException("Difference timestamp greater than INT.MAX_VALUE or less than INT.MIN_VALUE");
            }
            ans.putInt((int) diffTimestamp);
        }

        RID startRid = ridList.get(0);
        int startPage = startRid.page();
        ans.putInt(startPage);
        // step 4. write RID list
        for (RID rid : ridList) {
            int diffPage = rid.page() - startPage;
            int offset = rid.offset();
            if(diffPage < Short.MIN_VALUE || diffPage > Short.MAX_VALUE || offset > Short.MAX_VALUE){
                System.out.println("Compress algorithm fail, differ page: " + diffPage + " offset: " + offset);
                throw new RuntimeException("Compress algorithm fail.");
            }
            ans.putShort((short) diffPage);
            ans.putShort((short) rid.offset());
        }

        ans.flip();
        return ans;
    }

    public static IndexBlock deserialize(MappedByteBuffer buff) {
        /*
        write an index block to byte buffer
        suppose: h = indexNum - 1, g = N_f - 1
        then an index partition storage format as follows:
        |roaring bitmap size|roaring bitmap|
        |indexNum (int)|rbSize_1|rangeBitmap_1|...|rbSize_h|rangeBitmap_h|
        |g (int)|startTimestamp (long)|differTimestamp_1 (int)|...|differTimestamp_g|
        |startPage (int)|differPage_1 (short),offset_1 (short)|...|differPage_g,offset_g|
        */
        int deletionSize = buff.getInt();
        byte[] deletionArray = new byte[deletionSize];
        buff.get(deletionArray);
        ByteBuffer deletionBuffer = ByteBuffer.wrap(deletionArray);
        RoaringBitmap deletionMark = new RoaringBitmap();
        try{
            deletionMark.deserialize(deletionBuffer);
        }catch (Exception e){
            throw new RuntimeException("deserialize fails");
        }

        // step 1. read indexNum
        int indexNum = buff.getInt();
        // step 2. read range bitmaps
        // debug
        // long time0 = System.nanoTime();
        RangeBitmap[] rbs = new RangeBitmap[indexNum];
        for (int i = 0; i < indexNum; ++i) {
            int curSize = buff.getInt();
            byte[] rbByteArray = new byte[curSize];
            buff.get(rbByteArray);
            ByteBuffer curBitmapBuffer = ByteBuffer.wrap(rbByteArray);
            RangeBitmap curRangeBitmap = RangeBitmap.map(curBitmapBuffer);
            rbs[i] = curRangeBitmap;
        }
        // long time1 = System.nanoTime();

        // step 3. read timestamp list
        int g = buff.getInt();
        long startTimestamp = buff.getLong();
        // we have compress timestamp to integer, so size is 4 * g
        byte[] timestampListByteArray = new byte[g << 2];
        // long time2 = System.nanoTime();
        buff.get(timestampListByteArray);
        // long time3 = System.nanoTime();
        ByteBuffer timestampListBuff = ByteBuffer.wrap(timestampListByteArray);

        // step 4. read RID list
        int startRid = buff.getInt();
        // we have compress page and offset to short
        byte[] ridListByteArray = new byte[g << 2];
        buff.get(ridListByteArray);
        // long time4 = System.nanoTime();
        ByteBuffer ridListBuff = ByteBuffer.wrap(ridListByteArray);
        /*
        System.out.println("range bitmap cost: " + (time1 - time0) / 1000 + "us");
        System.out.println("create array cost: " + (time2 - time1) / 1000 + "us");
        System.out.println("copy ts cost: " + (time3 - time2) / 1000 + "us");
        System.out.println("copy rid cost: " + (time4 - time3) / 1000 + "us");
         */

        return new IndexBlock(deletionMark, rbs, g, startTimestamp, timestampListBuff, startRid, ridListBuff);
    }

    public List<IndexValuePair> getIndexValuePairs(RoaringBitmap rb) {
        final int len = rb.getCardinality();
        List<IndexValuePair> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i -> {
            // compressed timestamp is 4 bytes, compressed RID also is 4 bytes
            int pos = i << 2;
            long timestamp = startTimestamp + timestampListBuff.getInt(pos);
            RID rid = new RID(startPage + ridListBuff.getShort(pos), ridListBuff.getShort(pos + 2));
            ans.add(new IndexValuePair(timestamp, rid));
        });

        return ans;
    }

    /**
     * find related event based on corresponding independent predicate constraints
     * before call this function, please check max/min values in SynopsisTable
     * @param icQuads  independent predicate constraints
     * @param startPos start storage position in this index position
     * @param offset   offset
     * @return <rid, timestamp> pair
     */
    public List<IndexValuePair> query(List<IndependentConstraintQuad> icQuads, int startPos, int offset) {
        List<IndexValuePair> pairs;
        RoaringBitmap rb = new RoaringBitmap();
        rb.add(startPos, (long) (offset + startPos));

        for (IndependentConstraintQuad quad : icQuads) {
            RoaringBitmap rangeQueryRB = queryRangeBitmapUsingIC(quad);
            if (rangeQueryRB == null || rangeQueryRB.getCardinality() == 0) {
                rb = null;
                break;
            } else {
                rb.and(rangeQueryRB);
            }
        }

        pairs = (rb == null) ? new ArrayList<>() : getIndexValuePairs(rb);
        return pairs;
    }

    // to support deletion operation, we have to return two List<IndexValuePair>
    public List<List<IndexValuePair>> query4Deletion(List<IndependentConstraintQuad> icQuads, int startPos, int offset) {
        List<IndexValuePair> pairsMore;
        List<IndexValuePair> pairsDeletion;

        RoaringBitmap rb = new RoaringBitmap();
        rb.add(startPos, (long) (offset + startPos));

        for (IndependentConstraintQuad quad : icQuads) {
            RoaringBitmap rangeQueryRB = queryRangeBitmapUsingIC(quad);
            if (rangeQueryRB == null || rangeQueryRB.getCardinality() == 0) {
                rb = null;
                break;
            } else {
                rb.and(rangeQueryRB);
            }
        }

        if (rb != null) {
            // do not contain deleted items
            pairsMore = getIndexValuePairs(rb);
            rb.and(deletionMark);
            pairsDeletion = getIndexValuePairs(rb);
        }else{
            pairsMore = new ArrayList<>();
            pairsDeletion = new ArrayList<>();
        }
        List<List<IndexValuePair>> ans = new ArrayList<>(2);
        ans.add(pairsMore);
        ans.add(pairsDeletion);
        return ans;
    }

    private RoaringBitmap queryRangeBitmapUsingIC(IndependentConstraintQuad quad) {
        int idx = quad.idx();
        int mark = quad.mark();
        long min = quad.min();
        long max = quad.max();

        RoaringBitmap rangeQueryRB = null;

        // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
        switch (mark) {
            case 1 -> rangeQueryRB = gteQuery(idx, min);
            case 2 -> rangeQueryRB = lteQuery(idx, max);
            case 3 -> rangeQueryRB = betweenQuery(idx, min, max);
        }
        return rangeQueryRB;
    }

    public RoaringBitmap gteQuery(int idx, long min) {
        return rangeBitmaps[idx].gte(min);
    }

    public RoaringBitmap lteQuery(int idx, long max) {
        return rangeBitmaps[idx].lte(max);
    }

    public RoaringBitmap betweenQuery(int idx, long min, long max) {
        return rangeBitmaps[idx].between(min, max);
    }
}
