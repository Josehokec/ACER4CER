package acer;

import common.IndexValuePair;
import condition.IndependentConstraintQuad;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;
import store.RID;

import java.nio.ByteBuffer;
import java.util.*;

public class BufferPool {
    public static int capability = 42 * 1024;                   // buffer pool capability 42 * 1024;
    private int recordNum;                                      // number of stored event
    private final int indexNum;                                 // number of indexed attribute
    private List<Long> attrMaxRange;                            // record attrMaxRange, aims to create range bitmap
    private HashMap<String, List<ACERTemporaryTriple>> buffers; // buffer event
    // new version: support out-of-order insertion
    private HashMap<String, Boolean> orderMarkMap;              // mark each buffer whether sorted
    // new version: support out-of-order insertion
    private HashMap<String, Long> previousTimestampMap;         // mark each buffer previous timestamp

    public final int getRecordNum(){
        return recordNum;
    }

    /**
     * Construct function
     * @param indexNum      number of index attribute
     * @param attrMaxRange  maximum value for each index attribute
     */
    public BufferPool(int indexNum, List<Long> attrMaxRange){
        this.recordNum = 0;
        this.indexNum = indexNum;
        this.attrMaxRange = attrMaxRange;
        this.buffers = new HashMap<>();
        // new version: support out-of-order insertion
        this.orderMarkMap = new HashMap<>();
        this.previousTimestampMap = new HashMap<>();
    }

    // new version: deletion operation
    public final ByteBuffer deleteRecord(String eventType, int blockId, ACERTemporaryTriple triple, SynopsisTable synopsisTable){
        // Add to the partitionTable based on the event type of the data partition
        if(buffers.containsKey(eventType)){
            // directly insert this triple
            List<ACERTemporaryTriple> triples = buffers.get(eventType);
            triples.add(triple);

            // new version: support out-of-order insertion
            // if this buffer is timestamp-ordered, then we should update previous timestamp
            if(orderMarkMap.get(eventType)){
                long previousTs = previousTimestampMap.get(eventType);
                long currentTs = triple.timestamp();
                // if insert is out-of-order, then we need to update orderMarkMap
                if(currentTs < previousTs){
                    orderMarkMap.put(eventType, false);
                }else{
                    previousTimestampMap.put(eventType, currentTs);
                }
            }
        }else{
            // create a new buffer
            List<ACERTemporaryTriple> triples = new ArrayList<>(1024);
            triples.add(triple);
            buffers.put(eventType, triples);
            // new version: support out-of-order insertion
            orderMarkMap.put(eventType, true);
            previousTimestampMap.put(eventType, triple.timestamp());
        }
        recordNum++;

        // need to perform flush operation
        if(recordNum >= capability){
            // if reach the capability, then we generate custer information and update synopsis
            // create an index partition (contains bitmaps, timestamps, rids)
            HashMap<String, ClusterInfo> indexPartitionClusterInfo = new HashMap<>();

            List<RangeBitmap.Appender> appenderList = new ArrayList<>(indexNum);

            if(attrMaxRange != null){
                for(int i = 0; i < indexNum; ++i){
                    RangeBitmap.Appender curAppender = RangeBitmap.appender(attrMaxRange.get(i));
                    appenderList.add(curAppender);
                }
            }else{
                for(int i = 0; i < indexNum; ++i){
                    RangeBitmap.Appender curAppender = RangeBitmap.appender(Long.MAX_VALUE >> 1);
                    appenderList.add(curAppender);
                }
            }
            List<Long> timestampList = new ArrayList<>(capability);
            List<RID>  ridList = new ArrayList<>(capability);
            RoaringBitmap deletionMark = new RoaringBitmap();

            int startPos = 0;

            for(Map.Entry<String, List<ACERTemporaryTriple>> entry : buffers.entrySet()){
                String key = entry.getKey();
                List<ACERTemporaryTriple> value = entry.getValue();
                // if out-of-order
                if(!orderMarkMap.get(key)){
                    value.sort(Comparator.comparingLong(ACERTemporaryTriple::timestamp));
                }

                int size = value.size();
                long startTime = value.get(0).timestamp();
                long endTime = value.get(size - 1).timestamp();

                // initial
                List<Long> buffMinValues = new ArrayList<>(indexNum);
                List<Long> buffMaxValues = new ArrayList<>(indexNum);
                for(int i = 0 ; i < indexNum; ++i){
                    buffMinValues.add(Long.MAX_VALUE);
                    buffMaxValues.add(Long.MIN_VALUE);
                }

                for(int i = 0; i < size; ++i){
                    ACERTemporaryTriple curTriple = value.get(i);
                    long[] attrValues = curTriple.attrValues();

                    for(int k = 0; k < indexNum; ++k){
                        long attrValue = attrValues[k];
                        appenderList.get(k).add(attrValue);
                        // check max/min values
                        if(!curTriple.flag()){
                            if(attrValue > buffMaxValues.get(k)){
                                buffMaxValues.set(k, attrValue);
                            }
                            if(attrValue < buffMinValues.get(k)){
                                buffMinValues.set(k, attrValue);
                            }
                        }
                    }
                    timestampList.add(curTriple.timestamp());
                    ridList.add(curTriple.rid());
                    // if deletion
                    if(curTriple.flag()){
                        deletionMark.add(startPos + i);
                    }
                }
                ClusterInfo info = new ClusterInfo(blockId, startPos, size, startTime, endTime, buffMinValues, buffMaxValues);
                indexPartitionClusterInfo.put(key, info);
                startPos += size;
            }

            // clear the buffer pool
            buffers = new HashMap<>();
            // update synopsisTable
            synopsisTable.updateSynopsisTable(indexPartitionClusterInfo);
            // update event number
            recordNum = 0;
            return IndexBlock.serialize(deletionMark, appenderList, timestampList, ridList);
        }else{
            // if BufferPool does not flush, then return null
            return null;
        }
    }

    public final ByteBuffer insertRecord(String eventType, int blockId, ACERTemporaryTriple triple, SynopsisTable synopsisTable){
        // Add to the partitionTable based on the event type of the data partition
        if(buffers.containsKey(eventType)){
            // directly insert this triple
            List<ACERTemporaryTriple> triples = buffers.get(eventType);
            triples.add(triple);

            // new version: support out-of-order insertion
            // if this buffer is timestamp-ordered, then we should update previous timestamp
            if(orderMarkMap.get(eventType)){
                long previousTs = previousTimestampMap.get(eventType);
                long currentTs = triple.timestamp();
                // if insert is out-of-order, then we need to update orderMarkMap
                if(currentTs < previousTs){
                    orderMarkMap.put(eventType, false);
                }else{
                    previousTimestampMap.put(eventType, currentTs);
                }
            }
        }else{
            // create a new buffer
            List<ACERTemporaryTriple> triples = new ArrayList<>(1024);
            triples.add(triple);
            buffers.put(eventType, triples);
            // new version: support out-of-order insertion
            orderMarkMap.put(eventType, true);
            previousTimestampMap.put(eventType, triple.timestamp());
        }
        recordNum++;

        // need to perform flush operation
        if(recordNum >= capability){
            // if reach the capability, then we generate custer information and update synopsis
            // create an index partition (contains bitmaps, timestamps, rids)
            HashMap<String, ClusterInfo> indexPartitionClusterInfo = new HashMap<>();
            List<RangeBitmap.Appender> appenderList = new ArrayList<>(indexNum);

            if(attrMaxRange != null){
                for(int i = 0; i < indexNum; ++i){
                    RangeBitmap.Appender curAppender = RangeBitmap.appender(attrMaxRange.get(i));
                    appenderList.add(curAppender);
                }
            }else{
                for(int i = 0; i < indexNum; ++i){
                    RangeBitmap.Appender curAppender = RangeBitmap.appender(Long.MAX_VALUE >> 1);
                    appenderList.add(curAppender);
                }
            }
            List<Long> timestampList = new ArrayList<>(capability);
            List<RID>  ridList = new ArrayList<>(capability);
            RoaringBitmap deletionMark = new RoaringBitmap();

            int startPos = 0;

            for(Map.Entry<String, List<ACERTemporaryTriple>> entry : buffers.entrySet()){
                String key = entry.getKey();
                List<ACERTemporaryTriple> value = entry.getValue();
                // if out-of-order we need to sort
                if(!orderMarkMap.get(key)){
                    value.sort(Comparator.comparingLong(ACERTemporaryTriple::timestamp));
                }

                int size = value.size();
                long startTime = value.get(0).timestamp();
                long endTime = value.get(size - 1).timestamp();

                // initial
                List<Long> buffMinValues = new ArrayList<>(indexNum);
                List<Long> buffMaxValues = new ArrayList<>(indexNum);
                for(int i = 0 ; i < indexNum; ++i){
                    buffMinValues.add(Long.MAX_VALUE);
                    buffMaxValues.add(Long.MIN_VALUE);
                }

                for(int i = 0; i < size; ++i){
                    ACERTemporaryTriple curTriple = value.get(i);
                    long[] attrValues = curTriple.attrValues();

                    for(int k = 0; k < indexNum; ++k){
                        long attrValue = attrValues[k];
                        appenderList.get(k).add(attrValue);
                        // check max/min values
                        if(!curTriple.flag()){
                            if(attrValue > buffMaxValues.get(k)){
                                buffMaxValues.set(k, attrValue);
                            }
                            if(attrValue < buffMinValues.get(k)){
                                buffMinValues.set(k, attrValue);
                            }
                        }
                    }
                    timestampList.add(curTriple.timestamp());
                    ridList.add(curTriple.rid());
                    // if deletion
                    if(curTriple.flag()){
                        deletionMark.add(startPos + i);
                    }
                }
                ClusterInfo info = new ClusterInfo(blockId, startPos, size, startTime, endTime, buffMinValues, buffMaxValues);
                indexPartitionClusterInfo.put(key, info);
                startPos += size;
            }

            // clear the buffer pool
            buffers = new HashMap<>();
            // update synopsisTable
            synopsisTable.updateSynopsisTable(indexPartitionClusterInfo);
            // update event number
            recordNum = 0;
            return IndexBlock.serialize(deletionMark, appenderList, timestampList, ridList);
        }else{
            // if BufferPool does not flush, then return null
            return null;
        }
    }

    /**
     * filter out relevant records that meet the conditions in the buffer
     * @param eventType     event type
     * @param icQuads       independent predicate constraints
     * @return              <rid, timestamp> pair
     */
    public final List<IndexValuePair> query(String eventType, List<IndependentConstraintQuad> icQuads){
        List<IndexValuePair> ans = new ArrayList<>();
        List<ACERTemporaryTriple> triples = buffers.get(eventType);

        if(triples == null){
            triples = new ArrayList<>();
        }

        for(ACERTemporaryTriple triple : triples){
            boolean satisfy = true;
            long[] attrValues = triple.attrValues();
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                if(attrValues[idx] < min || attrValues[idx] > max){
                    satisfy = false;
                    break;
                }
            }
            // add no deletion
            if(satisfy && !triple.flag()){
                ans.add(new IndexValuePair(triple.timestamp(), triple.rid()));
            }
        }
        // new version: support out-of-order insertion
        // when we find it is out-of-order, we need to call sort
        if(!orderMarkMap.get(eventType)){
            ans.sort(Comparator.comparingLong(IndexValuePair::timestamp));
        }
        return ans;
    }

    public void print(){
        System.out.println("record number: " + recordNum);
    }
}

