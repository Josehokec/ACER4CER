package FastInDisk;

import condition.IndependentConstraintQuad;

import store.RID;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import common.IndexValuePair;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * index should store total byte size for an index partition
 * timestamp -> long (8 bytes)
 * RID -> int + short (12 bytes)
 * storage format:
 * |indexNum (int)|rbSize_1|rangeBitmap_1|...|rbSize_h|rangeBitmap_h|
 * |g (int)|timestamp_1 (long)|...|timestamp_g (long)|
 * |RID_1 (long)|...|RID_g (long)|
 */
public record OldIndexBlock(RangeBitmap[] rangeBitmaps, int eventNum,
                            ByteBuffer timestampListBuff, ByteBuffer ridListBuff) {

    /**
     * before call this function, we need to get totalByteSize
     * this function aims to serialize current index partition
     * for same schema, the size of index partition always same
     * @param appenderList  appender list
     * @param timestampList timestamp list
     * @param ridList       rid list
     * @return serialization byte buffer
     */
    public static ByteBuffer serialize(List<RangeBitmap.Appender> appenderList, List<Long> timestampList, List<RID> ridList) {
        /*
        write an index block to byte buffer
        suppose: h = indexNum - 1, g = N_f - 1
        then an index partition storage format as follows:
        |indexNum (int)|rbSize_1|rangeBitmap_1|...|rbSize_h|rangeBitmap_h|
        |g (int)|timestamp_1 (long)|...|timestamp_g (long)|
        |RID_1 (long)|...|RID_g (long)|
        */

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
        // 4 + 4 + size_0 + ... + 4 + size_h + 4 + g * 8 + g * 6
        int indexByteSize = 4 + 4 * indexNum + rangBitmapSumSize;
        int timeRidByteSize = 4 + 8 * g + 6 * g;
        int sumByteSize = indexByteSize + timeRidByteSize;
        // 1kB page = 1024 bytes
        int kBPageNum = (int) Math.ceil(sumByteSize / 1024.0);

        // step 2. write range bitmap to buffer
        ByteBuffer ans = ByteBuffer.allocate(kBPageNum * 1024);
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

        ans.putInt(g);
        // step 3. write timestamp list
        for (Long ts : timestampList) {
            ans.putLong(ts);
        }

        // step 4. write RID list
        for (RID rid : ridList) {
            int offset = rid.offset();
            if(offset > Short.MAX_VALUE){
                System.out.println("offset greater than Short.MAX_VALUE, offset: " + offset);
                throw new RuntimeException("Compress algorithm fail.");
            }
            ans.putInt(rid.page());
            ans.putShort((short) rid.offset());
        }

        ans.flip();
        return ans;
    }

    public static OldIndexBlock deserialize(MappedByteBuffer buff) {
        /*
        write an index block to byte buffer
        suppose: h = indexNum - 1, g = N_f - 1
        then an index partition storage format as follows:
        |indexNum (int)|rbSize_1|rangeBitmap_1|...|rbSize_h|rangeBitmap_h|
        |g (int)|timestamp_1 (long)|...|timestamp_g (long)|
        |RID_1 (long)|...|RID_g (long)|
        */

        // step 1. read indexNum
        int indexNum = buff.getInt();
        // step 2. read range bitmaps
        RangeBitmap[] rbs = new RangeBitmap[indexNum];
        for (int i = 0; i < indexNum; ++i) {
            int curSize = buff.getInt();
            byte[] rbByteArray = new byte[curSize];
            buff.get(rbByteArray);
            ByteBuffer curBitmapBuffer = ByteBuffer.wrap(rbByteArray);
            RangeBitmap curRangeBitmap = RangeBitmap.map(curBitmapBuffer);
            rbs[i] = curRangeBitmap;
        }

        // step 3. read timestamp list
        int g = buff.getInt();
        // timestamp list byte size: 8 * g
        byte[] timestampListByteArray = new byte[g << 3];
        buff.get(timestampListByteArray);
        ByteBuffer timestampListBuff = ByteBuffer.wrap(timestampListByteArray);

        // step 4. read RID list
        byte[] ridListByteArray = new byte[g * 6];
        buff.get(ridListByteArray);
        ByteBuffer ridListBuff = ByteBuffer.wrap(ridListByteArray);

        return new OldIndexBlock(rbs, g, timestampListBuff, ridListBuff);
    }

    public List<IndexValuePair> getIndexValuePairs(RoaringBitmap rb) {
        final int len = rb.getCardinality();
        List<IndexValuePair> ans = new ArrayList<>(len);
        rb.forEach((Consumer<? super Integer>) i -> {
            int tsPos = i << 3;
            int ridPos = i * 6;
            long timestamp = timestampListBuff.getLong(tsPos);
            RID rid = new RID(ridListBuff.getInt(ridPos), ridListBuff.getShort(ridPos + 4));
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

        // determine query range in the index  partition
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
            // use bitmap to get <rid, timestamp> pair
            pairs = getIndexValuePairs(rb);
        }else{
            pairs = new ArrayList<>();
        }

        return pairs;
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

