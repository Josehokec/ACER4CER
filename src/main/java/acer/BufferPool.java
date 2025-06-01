package acer;

import common.IndexValuePair;
import compressor.*;
import condition.ICQueryQuad;
import org.roaringbitmap.RangeBitmap;

import java.nio.ByteBuffer;
import java.util.*;

public class BufferPool {
    private int recordNum = 0;                                  // number of stored event
    private final int indexAttrNum;                                 // number of indexed attribute
    private final HashMap<String, SingleBuffer> buffers;        // buffer events, key is event type, value is a single buffer

    public BufferPool(int indexAttrNum) {
        this.indexAttrNum = indexAttrNum;
        this.buffers = new HashMap<>();
    }

    /**
     * here we support insertion or deletion operations
     * @param orderedFlag       out-of-order or in-order
     * @param eventType         type
     * @param blockId           block id
     * @param triple            ACERTemporaryTriple
     * @param synopsisTable     synopsis table
     * @return if BufferPool does not flush, then return null; otherwise, we return a new block
     */
    public ByteBuffer insert(boolean orderedFlag, String eventType, int blockId, TemporaryTriple triple,
                             SynopsisTable synopsisTable, List<IdxBlkMetaInfo> idxBlkMetaInfoList){
        buffers.computeIfAbsent(eventType, k -> new SingleBuffer(indexAttrNum)).append(triple);
        recordNum++;
        ByteBuffer buffer = null;
        if(recordNum == Parameters.CAPACITY){
            buffer = generateIndexBlock(orderedFlag, blockId, synopsisTable, idxBlkMetaInfoList);
            // clear content
            buffers.clear();
            recordNum = 0;
        }
        return buffer;
    }

    /**
     * generate an index block
     * @param orderedFlag       in-order or out-of-order insertion
     * @param blockId           block id
     * @param synopsisTable     synopsis table
     * @return                  byte buffer
     */
    public ByteBuffer generateIndexBlock(boolean orderedFlag, int blockId, SynopsisTable synopsisTable, List<IdxBlkMetaInfo> idxBlkMetaInfoList){
        // step 1: initialization
        long[] maxRanges = new long[indexAttrNum];
        for(int i = 0; i< indexAttrNum; i++){
            maxRanges[i] = Long.MIN_VALUE;
        }
        for(SingleBuffer singleBuffer : buffers.values()){
            long[] curMaxRanges = singleBuffer.getRanges();
            for(int i = 0; i < indexAttrNum; i++){
                long range = curMaxRanges[i];
                if(range > maxRanges[i]){
                    maxRanges[i] = range;
                }
            }
        }
        RangeBitmap.Appender[] appenders = new RangeBitmap.Appender[indexAttrNum];
        for(int i = 0; i < indexAttrNum; ++i){
            appenders[i] = RangeBitmap.appender(maxRanges[i]);
        }

        // step 1: append transformed values
        int startPos = 0;
        int clusterId = 0;
        // note that when enable optimization of index block layout, sizeOf(bufferList) ==> 2
        List<long[]> bufferList = new ArrayList<>();
        List<Long> idxBlkTsList = null;
        List<Long> idxBlkRIDList = null;
        if(!Parameters.OPTIMIZED_LAYOUT){
            idxBlkTsList = new ArrayList<>(Parameters.CAPACITY);
            idxBlkRIDList = new ArrayList<>(Parameters.CAPACITY);
        }

        // step 2: read value from each event buffer
        for(HashMap.Entry<String, SingleBuffer> entry : buffers.entrySet()) {
            String curType = entry.getKey();
            SingleBuffer eventBuffer = entry.getValue();
            List<TemporaryTriple> triples = eventBuffer.getAllTriples();
            int size = triples.size();
            if(!orderedFlag){
                triples.sort(Comparator.comparingLong(TemporaryTriple::timestamp));
            }

            // collect cluster information
            long startTime = triples.get(0).timestamp();
            long endTime = triples.get(size - 1).timestamp();
            long[] minValues = new long[indexAttrNum];
            long[] maxValues = new long[indexAttrNum];
            System.arraycopy(eventBuffer.getMinValues(), 0, minValues, 0, indexAttrNum);
            System.arraycopy(eventBuffer.getMaxValues(), 0, maxValues, 0, indexAttrNum);

            List<Long> clusterTsList = new ArrayList<>(size);
            List<Long> clusterRIDList = new ArrayList<>(size);
            for(int i = 0; i < size; i++) {
                TemporaryTriple triple = triples.get(i);
                long[] curAttrValues = triple.attrValues();
                for (int k = 0; k < indexAttrNum; ++k) {
                    appenders[k].add(curAttrValues[k] - minValues[k]);
                }
                clusterTsList.add(triple.timestamp());
                clusterRIDList.add(triple.rid().getLongValue());
            }

            if(Parameters.OPTIMIZED_LAYOUT) {
                // start compression operation
                long[] compressedClusterTs;
                long[] compressedClusterRID;
                switch (Parameters.COMPRESSOR){
                    case DELTA_2 -> {
                        compressedClusterTs = DeltaOfDeltaCompressor.compress(clusterTsList);
                        compressedClusterRID = DeltaOfDeltaCompressor.compress(clusterRIDList);
                    }
                    case VAR_INT -> {
                        compressedClusterTs = DeltaVarIntCompressor.compress(clusterTsList);
                        compressedClusterRID = DeltaVarIntCompressor.compress(clusterRIDList);
                    }
                    case SIMPLE_8B -> {
                        compressedClusterTs = DeltaSimple8BCompressor.compress(clusterTsList);
                        compressedClusterRID = DeltaSimple8BCompressor.compress(clusterRIDList);
                    }
                    case DELTA -> {
                        compressedClusterTs = DeltaCompressor.compress(clusterTsList);
                        compressedClusterRID = DeltaCompressor.compress(clusterRIDList);
                    }
                    default -> throw new IllegalStateException("We cannot support " + Parameters.COMPRESSOR);
                }
                bufferList.add(compressedClusterTs);
                bufferList.add(compressedClusterRID);
            }else{
                idxBlkTsList.addAll(clusterTsList);
                idxBlkRIDList.addAll(clusterRIDList);
            }

            ClusterInfo info = new ClusterInfo(blockId, clusterId, startPos, size, startTime, endTime, minValues, maxValues);
            synopsisTable.updateSynopsisTable(curType, info);

            startPos += size;
            clusterId++;
            eventBuffer.clear();
        }

        // step 3: when disable optimization of index block, we need to compress entire list
        if(!Parameters.OPTIMIZED_LAYOUT){
            long[] compressedClusterTs;
            long[] compressedClusterRID;
            switch (Parameters.COMPRESSOR){
                case DELTA_2 -> {
                    compressedClusterTs = DeltaOfDeltaCompressor.compress(idxBlkTsList);
                    compressedClusterRID = DeltaOfDeltaCompressor.compress(idxBlkRIDList);
                }
                case VAR_INT -> {
                    compressedClusterTs = DeltaVarIntCompressor.compress(idxBlkTsList);
                    compressedClusterRID = DeltaVarIntCompressor.compress(idxBlkRIDList);
                }
                case SIMPLE_8B -> {
                    compressedClusterTs = DeltaSimple8BCompressor.compress(idxBlkTsList);
                    compressedClusterRID = DeltaSimple8BCompressor.compress(idxBlkRIDList);
                }
                case DELTA -> {
                    compressedClusterTs = DeltaCompressor.compress(idxBlkTsList);
                    compressedClusterRID = DeltaCompressor.compress(idxBlkRIDList);
                }
                default -> throw new IllegalStateException("We cannot support " + Parameters.COMPRESSOR);
            }
            bufferList.add(compressedClusterTs);
            bufferList.add(compressedClusterRID);
        }

        return serialize(appenders, bufferList, idxBlkMetaInfoList);
    }

    public List<IndexValuePair> query(boolean orderedFlag, String eventType, List<ICQueryQuad> icQuads){
        SingleBuffer buffer = buffers.get(eventType);
        if(buffer == null || buffer.getSize() == 0){
            return new ArrayList<>(8);
        }

        List<IndexValuePair> pairs = new ArrayList<>(buffer.getSize() * 2 /3);
        for(TemporaryTriple triple : buffer.getAllTriples()) {
            boolean satisfy = true;
            long[] attrValues = triple.attrValues();
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
                pairs.add(new IndexValuePair(triple.timestamp(), triple.rid()));
            }
        }

        if(!orderedFlag){
            pairs.sort(Comparator.comparingLong(IndexValuePair::timestamp));
        }

        return pairs;
    }


    public void print(){
        System.out.println("record number: " + recordNum);
    }

    /**
     * 1KB alignment, update idxBlkMetaInfoList
     * @param appenders                 appender array
     * @param buffers                   <ts, rid> buffers
     * @param idxBlkMetaInfoList        index block information list
     * @return                          byte buffer
     */
    public static ByteBuffer serialize(RangeBitmap.Appender[] appenders, List<long[]> buffers, List<IdxBlkMetaInfo> idxBlkMetaInfoList){
        int indexAttrNum = appenders.length;
        int bufferSize = buffers.size();
        int[] sizes = Parameters.OPTIMIZED_LAYOUT ? new int[indexAttrNum + bufferSize] : new int[indexAttrNum + 2];

        int totalSize = 0;
        for(int i = 0; i < indexAttrNum; ++i){
            sizes[i] = appenders[i].serializedSizeInBytes();
            totalSize += sizes[i];
        }

        if(Parameters.OPTIMIZED_LAYOUT){
            for(int i = 0; i < bufferSize; i++){
                sizes[indexAttrNum + i] = buffers.get(i).length * 8;
                totalSize += sizes[indexAttrNum + i];
            }
        }else{
            sizes[indexAttrNum] = buffers.get(0).length * 8;
            totalSize += sizes[indexAttrNum];
            sizes[indexAttrNum + 1] = buffers.get(1).length * 8;
            totalSize += sizes[indexAttrNum + 1];
        }

        // 1KB alignment
        int kBPageNum = (int) Math.ceil(totalSize / 1024.0);
        int blkSize = kBPageNum * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(blkSize);
        for(int i = 0; i < indexAttrNum; i++){
            int curSize = sizes[i];
            ByteBuffer rbBuffer = ByteBuffer.allocate(curSize);
            appenders[i].serialize(rbBuffer);
            // ensure to switch to write mode, if without flip, we cannot get answer
            rbBuffer.flip();
            buffer.put(rbBuffer);
        }

        for(int i = 0; i < bufferSize; i++){
            long[] values = buffers.get(i);
            for(long value : values){
                buffer.putLong(value);
            }
        }
        buffer.flip();
        long idxBlkStartPos;
        if(idxBlkMetaInfoList.isEmpty()){
            idxBlkStartPos = 0;
        }else{
            int ptr = idxBlkMetaInfoList.size() - 1;
            IdxBlkMetaInfo idxBlkMetaInfo = idxBlkMetaInfoList.get(ptr);
            idxBlkStartPos = idxBlkMetaInfo.blockSize() + idxBlkMetaInfo.storagePosition();
        }
        // append idxBlkMetaInfo to IdxBlkMetaInfo list
        IdxBlkMetaInfo idxBlkMetaInfo = new IdxBlkMetaInfo(idxBlkStartPos, blkSize, sizes);
        idxBlkMetaInfoList.add(idxBlkMetaInfo);

        return buffer;
    }
}
