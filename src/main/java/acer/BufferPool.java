package acer;

import common.IndexValuePair;
import condition.ICQueryQuad;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import store.RID;

import java.nio.ByteBuffer;
import java.util.*;

public class BufferPool {
    public static int capability = 42 * 1024;                   // buffer pool capability 42 * 1024;
    //public static int capability = 20;                        // it is used for debugging
    private int recordNum;                                      // number of stored event
    private final int indexNum;                                 // number of indexed attribute
    private final HashMap<String, SingleBuffer> buffers;        // buffer events, key is event type, value is a single buffer

    /**
     * Construct function
     * @param indexNum      number of indexed attributes
     */
    public BufferPool(int indexNum){
        this.recordNum = 0;
        this.indexNum = indexNum;
        this.buffers = new HashMap<>();
    }

    // unify deletion and insertion operations
    public final ByteBuffer insertOrDeleteRecord(boolean orderedFlag, String eventType, int blockId, ACERTemporaryQuad quad, SynopsisTable synopsisTable){
        long[] attrValues = quad.attrValues();
        // here we need to know the max/min value for each index block
        assert(attrValues.length == indexNum);

        // if a given buffer exists
        if(buffers.containsKey(eventType)) {
            // List<ACERTemporaryQuad> quads = buffers.get(eventType); quads.add(quad);
            buffers.get(eventType).append(quad);
        }else{
            // else we need to create a new buffer
            SingleBuffer buffer = new SingleBuffer(indexNum);
            buffer.append(quad);
            buffers.put(eventType, buffer);
        }
        recordNum++;

        // when buffer pool reaches its capacity, we need to perform flush operation
        if(recordNum >= capability){
            return generateIndexBlock(orderedFlag, blockId, synopsisTable);
        }else{
            // if BufferPool does not flush, then return null
            return null;
        }
    }

    /**
     * generate an index block, here we use delta compression to compress attribute values
     * @param orderedFlag   in-order or out-of-order insertion
     * @param blockId       block id
     * @param synopsisTable synopsis table
     * @return              byte buffer
     */
    public ByteBuffer generateIndexBlock(boolean orderedFlag, int blockId, SynopsisTable synopsisTable){
        // new version: support deletion operation
        RoaringBitmap deletionMark = new RoaringBitmap();

        // we need to know that max ranges for each attribute so that we can create range bitmaps
        List<Long> maxRanges = new ArrayList<>(indexNum);
        for(int i = 0; i< indexNum; i++){
            maxRanges.add(Long.MIN_VALUE);
        }
        for(Map.Entry<String, SingleBuffer> entry : buffers.entrySet()) {
            //System.out.println("type: " + entry.getKey());
            SingleBuffer curBuffer = entry.getValue();
            List<Long> curRanges = curBuffer.getRanges();
            for(int i = 0; i< indexNum; i++){
                long range = curRanges.get(i);
                if(range > maxRanges.get(i)){
                    maxRanges.set(i, range);
                }
            }
        }

        // an index block contains: some range bitmaps + timestampList + ridList
        // create range bitmaps' appender
        List<RangeBitmap.Appender> appenderList = new ArrayList<>(indexNum);
        for(int i = 0; i < indexNum; ++i){
            // do we need to add one?
            RangeBitmap.Appender curAppender = RangeBitmap.appender(maxRanges.get(i));
            appenderList.add(curAppender);
        }
        List<Long> timestampList = new ArrayList<>(capability);
        List<RID>  ridList = new ArrayList<>(capability);

        // generate custer information and update synopsis
        int startPos = 0;
        for(Map.Entry<String, SingleBuffer> entry : buffers.entrySet()) {
            String curEventType = entry.getKey();
            SingleBuffer curBuffer = entry.getValue();
            List<ACERTemporaryQuad> curQuads = curBuffer.getAllQuads();
            if(!orderedFlag){
                curQuads.sort(Comparator.comparingLong(ACERTemporaryQuad::timestamp));
            }
            int size = curQuads.size();
            long startTime = curQuads.get(0).timestamp();
            long endTime = curQuads.get(size - 1).timestamp();

            List<Long> minValues = curBuffer.getMinValues();
            // insert appends, timestampList, and ridList
            for(int i = 0; i < size; ++i){
                ACERTemporaryQuad curQuad = curQuads.get(i);
                long[] curAttrValues = curQuad.attrValues();
                for(int k = 0; k < indexNum; ++k){
                    // 中文注释：注意到我们存储的value是减掉了每个集群的最小值
                    appenderList.get(k).add(curAttrValues[k] - minValues.get(k));
                }
                timestampList.add(curQuad.timestamp());
                ridList.add(curQuad.rid());
                // new version: support deletion operation
                if(curQuad.flag()){
                    deletionMark.add(startPos + i);
                }
            }

            // generate synopsis information and update it to synopsis table
            ClusterInfo info = new ClusterInfo(blockId, startPos, size, startTime, endTime, curBuffer.getMinValues(), curBuffer.getMaxValues());
            synopsisTable.updateSynopsisTable(curEventType, info);
            startPos += size;
            curBuffer.clear();
        }

        // clear the buffer pool, buffers = new HashMap<>();
        buffers.clear();
        // reset record/event number
        recordNum = 0;
        return IndexBlock.serialize(deletionMark, appenderList, timestampList, ridList);
    }

    /**
     * filter out relevant records that meet the conditions in the buffer
     * new version: support out-of-order insertion & deletion operation
     * 中文注释：这里的icQuads不能转换查询范围！！！
     * @param eventType     event type
     * @param icQuads       converted independent predicate constraints
     * @return              <rid, timestamp> pair
     */
    public final List<List<IndexValuePair>> query(boolean orderedFlag, String eventType, List<ICQueryQuad> icQuads){
        SingleBuffer buffer = buffers.get(eventType);

        List<List<IndexValuePair>> finalAns = new ArrayList<>(2);
        if(buffer == null || buffer.getSize() == 0){
            finalAns.add(new ArrayList<>(8));
            finalAns.add(new ArrayList<>(8));
            return finalAns;
        }

        List<IndexValuePair> satisfiedPairs = new ArrayList<>(512);
        List<IndexValuePair> deletedPairs = new ArrayList<>(64);

        for(ACERTemporaryQuad acerQuad : buffer.getAllQuads()) {
            boolean satisfy = true;
            long[] attrValues = acerQuad.attrValues();
            for (ICQueryQuad quad : icQuads) {
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                if (attrValues[idx] < min || attrValues[idx] > max) {
                    satisfy = false;
                    break;
                }
            }

            if (satisfy) {
                if (acerQuad.flag()) {
                    deletedPairs.add(new IndexValuePair(acerQuad.timestamp(), acerQuad.rid()));
                } else {
                    satisfiedPairs.add(new IndexValuePair(acerQuad.timestamp(), acerQuad.rid()));
                }
            }
        }
        // when we find it is out-of-order, we need to call sort
        if(!orderedFlag){
            satisfiedPairs.sort(Comparator.comparingLong(IndexValuePair::timestamp));
        }
        finalAns.add(satisfiedPairs);
        finalAns.add(deletedPairs);
        return finalAns;
    }

    public void print(){
        System.out.println("record number: " + recordNum);
    }

//    public void debug(){
//        System.out.println("BABA");
//        System.out.println("buffer ts:" + buffers.get("BABA").getAllQuads().get(0).timestamp());
//    }
}

