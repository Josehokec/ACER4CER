package acer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import arrival.JsonMap;
import automaton.NFA;
import baselines.Index;
import baselines.NaiveIndex;
import common.IndexValuePair;
import common.ReservoirSampling;
import automaton.Tuple;
import condition.ICQueryQuad;
import condition.IndependentConstraint;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;
import systems.CrimesPatternQuery;

/**
 * [updated] new version bring a faster query performance
 * Note that:
 * new version of ACER does not read the entire content of the index block,
 * but reads the content of the index block as needed to avoid unnecessary IO operations
 * -------------------------------------------------------------------------------------
 * ACER mainly includes four components: BufferPool, IndexBlock, Page and SynopsisTable
 * ACER is a simple, but efficient method (the greatest truths is concise)
 * that uses bitmap index structures to Accelerate Complex Event Recognition
 * -------------------------------------------------------------------------------------
 */
public class ACER extends Index {
    private boolean orderedFlag = true;                     // in-order or out-of-order insertion operation
    private double sumArrival;                              // total event arrival rate
    private long previousTimestamp = -1;                    // we need it to judge whether events are ordered
    private BufferPool bufferPool;                          // bufferPool store event (in memory)
    private SynopsisTable synopsisTable;                    // store each event type synopsis information
    private final File file;                                // all index block will be written in this file

    private final List<IdxBlkMetaInfo> idxBlkMetaInfoList = new ArrayList<>();
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    public ACER(String indexName){
        super(indexName);
        String storePath = System.getProperty("user.dir") + File.separator + "store";
        String filename = indexName + "_INDEX.binary_file";
        String filePath = storePath + File.separator + filename;
        System.out.println("index storage file path: " + filePath);
        file = new File(filePath);
        // If this file has existed before, we clear the content
        if(file.exists()){
            if(file.delete()){
                System.out.println("file: '"+ filename + "' exists in disk, we clear the file content.");
            }
        }
        // raf = null;
        fileChannel = null;
    }

    @Override
    public void initial() {
        // step 1: new object
        synopsisTable = SynopsisTable.getInstance();
        // when create index, we know the number of indexed attributes (store in Index.indexAttrNum)
        bufferPool = new BufferPool(indexAttrNum);

        //  step 2: set the arrival rate for each event, which is later used to calculate the selection rate
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "arrival" + File.separator + schemaName + "_arrivals.json";
        // we need to know arrival rate for different event types
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);
        sumArrival = 0;
        for (double a : arrivals.values()) {
            sumArrival += a;
        }

        // step 3: create a reservoir (use reservoir sampling)
        reservoir = new ReservoirSampling(indexAttrNum);
    }

    @Override
    public boolean insertRecord(String record, boolean updatedFlag) {
        hasUpdated = updatedFlag || hasUpdated;
        fileChannel = null;

        // record: TYPE_10,127,246,854.27,534.69,1683169267388
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();
        String[] splits = record.split(",");

        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum];

        // put indexed attributes into attrValArray
        for(int i = 0; i < splits.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                eventType = splits[i];
            }else if(attrTypes[i].equals("TIMESTAMP")){
                timestamp = Long.parseLong(splits[i]);
                // we need to check whether out-of-order insertion
                if(previousTimestamp > timestamp){
                    orderedFlag = false;
                }
                previousTimestamp = timestamp;
            }else if(attrTypes[i].equals("INT") ){
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    attrValArray[idx] = Long.parseLong(splits[i]);
                }
            }else if(attrTypes[i].contains("FLOAT")) {
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Float.parseFloat(splits[i]) * magnification);
                }
            } else if(attrTypes[i].contains("DOUBLE")){
                if(indexAttrNameMap.containsKey(attrNames[i])){
                    int idx = indexAttrNameMap.get(attrNames[i]);
                    int magnification = (int) Math.pow(10, schema.getIthDecimalLens(i));
                    attrValArray[idx] = (long) (Double.parseDouble(splits[i]) * magnification);
                }
            }
            // currently, we do not support index string value
        }

        // sampling algorithm ==> estimate selectivity
        reservoir.sampling(attrValArray, autoIndices);

        // write event to disk
        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);

        // cache indexed attribute values
        int blockId = idxBlkMetaInfoList.size();
        TemporaryTriple triple = new TemporaryTriple(timestamp, rid, attrValArray);
        ByteBuffer buffer = bufferPool.insert(orderedFlag, eventType, blockId, triple, synopsisTable, idxBlkMetaInfoList);

        // update indices
        autoIndices++;

        if(buffer != null){
            storeIndexBlock(buffer);
            // create a new block, so change this value
            orderedFlag = true;
        }

        return true;
    }

    @Override
    public boolean insertBatchRecord(List<String[]> batchRecords, boolean updatedFlag) {
        hasUpdated = updatedFlag || hasUpdated;
        fileChannel = null;
        String[] attrNames = schema.getAttrNames();
        EventStore store = schema.getStore();

        if(Parameters.CAPACITY % batchRecords.size() != 0){
            throw new IllegalArgumentException("Batch records must have a size of " + Parameters.CAPACITY);
        }

        ByteBuffer buffer = null;
        int notNullCount = 0;
        for(String[] splits : batchRecords){
            String eventType = splits[0];

            long[] attrValArray = new long[indexAttrNum];
            int a1Pos = indexAttrNameMap.get(attrNames[1]);
            attrValArray[a1Pos] = Long.parseLong(splits[1]);

            int a2Pos = indexAttrNameMap.get(attrNames[2]);
            attrValArray[a2Pos] = Long.parseLong(splits[2]);

            int a3Pos = indexAttrNameMap.get(attrNames[3]);
            int a3Magnification = (int) Math.pow(10, schema.getIthDecimalLens(3));
            attrValArray[a3Pos] = (long) (Double.parseDouble(splits[3]) * a3Magnification);

            int a4Pos = indexAttrNameMap.get(attrNames[4]);
            int a4Magnification = (int) Math.pow(10, schema.getIthDecimalLens(4));
            attrValArray[a4Pos] = (long) (Double.parseDouble(splits[4]) * a4Magnification);

            long timestamp = Long.parseLong(splits[5]);
            if(previousTimestamp > timestamp){
                orderedFlag = false;
            }
            previousTimestamp = timestamp;

            // write event to disk
            byte[] bytesRecord = schema.convertToBytes(splits);
            RID rid = store.insertByteRecord(bytesRecord);

            // cache indexed attribute values
            int blockId = idxBlkMetaInfoList.size();
            TemporaryTriple triple = new TemporaryTriple(timestamp, rid, attrValArray);
            buffer = bufferPool.insert(orderedFlag, eventType, blockId, triple, synopsisTable, idxBlkMetaInfoList);
            if(buffer != null){
                notNullCount++;
            }

            reservoir.sampling(attrValArray, autoIndices);
            autoIndices++;
        }

        //if(notNullCount > 1){throw new RuntimeException("More than one batch record found");}

        if(buffer != null){
            storeIndexBlock(buffer);
            // create a new block, so change this value
            orderedFlag = true;
        }

        return true;
    }

    @Override
    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        long filterStartTime = System.nanoTime();
        List<IndexValuePair> pairs = twoPhaseFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = NaiveIndex.obtainEventsBasedPairs(pairs, schema.getStore());
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");

        long matchStartTime = System.nanoTime();

        int ans;
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, pattern.getStrategy());
        }
        ans = nfa.countTuple();

        // when you want to call flink, you can replace above 5 lines
        // ans = CrimesPatternQuery.crimesFirstQuery(events, schema);

        long matchEndTime = System.nanoTime();
        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");
        return ans;
    }

    @Override
    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        long filterStartTime = System.nanoTime();
        List<IndexValuePair> pairs = twoPhaseFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = NaiveIndex.obtainEventsBasedPairs(pairs, schema.getStore());
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        long matchStartTime = System.nanoTime();
        nfa.generateNFAUsingQueryPattern(pattern);

        for(byte[] event : events){
            nfa.consume(schema, event, pattern.getStrategy());
        }
        // debug...
        nfa.printMatchIds();

        List<Tuple> ans = nfa.getTuple(schema);
        long matchEndTime = System.nanoTime();
        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");
        return ans;
    }

    @Override
    public void print() {
        System.out.println("orderedFlag: " + orderedFlag);
        // block storage position information
        System.out.println("Block storage position information as follows: ");

        int blockNum = idxBlkMetaInfoList.size();
        for(int i = 0; i < blockNum; i++){
            IdxBlkMetaInfo blkMetaInfo = idxBlkMetaInfoList.get(i);
            System.out.println("BlockId: " + i + " startPos: " +
                    blkMetaInfo.storagePosition() + " offset: " + blkMetaInfo.blockSize());
        }
        bufferPool.print();
        synopsisTable.print();
    }

    /**
     * We refactored the code block to make it simple and easy to understand
     * [future work] in fact, we can add some code lines to support OR operator
     * @param pattern       query pattern (complex event pattern without OR operator)
     * @return              index value pairs
     */
    public final List<IndexValuePair> twoPhaseFiltering(QueryPattern pattern) {
        if(pattern.existOROperator()){
            System.out.println("this pattern exists `OR` operator, we do not support this operator");
            throw new RuntimeException("we can not process this pattern");
        }

        // step 1: estimate selectivity of each variable
        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        Set<String> varNameSet = varTypeMap.keySet();
        record SelectivityIndexPair(double selectivity, String varName){/*overall selectivity of single variable*/}
        int patternLen = varNameSet.size();
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);
        for(String curVarName : varNameSet){
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(curVarName);
            String curEventType = varTypeMap.get(curVarName);
            double sel = arrivals.get(curEventType) / sumArrival;
            for (IndependentConstraint ic : icList) {
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
            }
            varSelList.add(new SelectivityIndexPair(sel, curVarName));
            // System.out.println("VarName: " + curVarName + " selectivity: " + sel);
        }
        // sort based on selectivity
        varSelList.sort(Comparator.comparingDouble(SelectivityIndexPair::selectivity));

        // cache each variable's query result from ACER index
        Map<String, List<IndexValuePair>> varQueryResult = new HashMap<>();

        // step2: choose the variable with minimum selectivity to query
        String minVarName = varSelList.get(0).varName();
        String minVarType = varTypeMap.get(minVarName);
        List<IndexValuePair> minSelPairs = queryVariableResult(minVarType, minVarName, pattern);
        varQueryResult.put(minVarName, minSelPairs);        // store results

        // step 3 : generate SortedIntervalSet (for index blocks)
        long leftOffset = 0;
        long rightOffset = 0;
        long tau = pattern.getTau();
        if(pattern.isOnlyLeftMostNode(minVarName)){
            rightOffset = tau;
        }else if(pattern.isOnlyRightMostNode(minVarName)){
            leftOffset = -tau;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet intervalSet = generateIntervalSet(minSelPairs, leftOffset, rightOffset);

        // step 4: using time intervals to filter events
        for(int i = 1; i < patternLen; i++){
            String curVarName = varSelList.get(i).varName();
            String curVarType = varTypeMap.get(curVarName);
            List<IndexValuePair> curSelPairs = queryVariableResult(curVarType, curVarName, pattern, intervalSet);
            // here we need to update curSelPairs and interval set
            varQueryResult.put(curVarName, intervalSet.updateAndFilter(curSelPairs));
        }

        // step 5: filter again based on interval set
        List<IndexValuePair> ans = null;
        for(int i = 0; i < patternLen; i++){
            String curVarName = varSelList.get(i).varName();
            // since interval become shorter, we still can filter events
            List<IndexValuePair> curPairs = intervalSet.updateAndFilter(varQueryResult.get(curVarName));
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans =  (ans == null) ? curPairs : NaiveIndex.mergeIndexValuePair(ans, curPairs);
        }
        return ans;
    }

    public List<IndexValuePair> queryVariableResult(String type, String varName, QueryPattern pattern){
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
        List<ClusterInfo> clusterInfoList = synopsisTable.getClusterInfo(type);
        List<IndexValuePair> pairsFromDisk = getPairsFromDisk(icList, clusterInfoList);
        List<IndexValuePair> pairsFromBuffer = getPairsFromBuffer(type, icList);
        List<IndexValuePair> mergedPairs = NaiveIndex.mergeIndexValuePair(pairsFromDisk, pairsFromBuffer);
        // <type, timestamp> as primary key
        if (hasUpdated) {
            mergedPairs = NaiveIndex.getUniqueIndexValuePair(mergedPairs);
        }
        return mergedPairs;
    }

    public List<IndexValuePair> queryVariableResult(String type, String varName, QueryPattern pattern, SortedIntervalSet intervalSet){
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);

        List<Long> startTimeList = new ArrayList<>(512);
        List<Long> endTimeList = new ArrayList<>(512);
        synopsisTable.getStartEndTimestamp(type, startTimeList, endTimeList);
        List<Boolean> overlaps = intervalSet.checkOverlap(startTimeList, endTimeList);
        List<ClusterInfo> clusterInfoList = synopsisTable.getOverlappedClusterInfo(type, overlaps);

        List<IndexValuePair> pairsFromDisk = getPairsFromDisk(icList, clusterInfoList);
        List<IndexValuePair> pairsFromBuffer = getPairsFromBuffer(type, icList);
        List<IndexValuePair> mergedPairs = NaiveIndex.mergeIndexValuePair(pairsFromDisk, pairsFromBuffer);
        // <type, timestamp> as primary key
        if (hasUpdated) {
            mergedPairs = NaiveIndex.getUniqueIndexValuePair(mergedPairs);
        }
        return mergedPairs;
    }

    public List<IndexValuePair> getPairsFromDisk(List<IndependentConstraint> icList, List<ClusterInfo> clusterInfoList){
        List<IndexValuePair> pairs = new ArrayList<>(1024);
        for(ClusterInfo clusterInfo : clusterInfoList){
            long[] maxValues = clusterInfo.maxValues();
            long[] minValues = clusterInfo.minValues();

            boolean skip = false;
            int icNum = icList.size();
            List<ICQueryQuad> icQuads = new ArrayList<>(icNum);
            int[] idxs = new int[icNum];
            for(int i = 0; i < icNum; i++){
                IndependentConstraint ic = icList.get(i);
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                idxs[i] = idx;
                if(maxValues[idx] < ic.getMinValue() || minValues[idx] > ic.getMaxValue()){
                    skip = true;
                    break;
                }

                // Note that the range bitmap stores delta values instead of raw values,
                // and the minimum value for each cluster is different,
                int mark = ic.hasMinMaxValue();
                long curMinValue = minValues[idx];
                switch (mark){
                    // note that range bitmap has bug when query rb.gle(-1);
                    case 1 -> icQuads.add(new ICQueryQuad(idx, 1, Math.max(0, ic.getMinValue() - curMinValue), ic.getMaxValue()));
                    case 2 -> icQuads.add(new ICQueryQuad(idx, 2, ic.getMinValue(), ic.getMaxValue() - curMinValue));
                    case 3 -> icQuads.add(new ICQueryQuad(idx, 3, Math.max(0, ic.getMinValue() - curMinValue), ic.getMaxValue() - curMinValue));
                    default -> throw new RuntimeException("wrong mark, mark: " + mark);
                }
            }

            if(!skip){
                MappedIdxBlk indexBlock = getIndexBlock(clusterInfo.indexBlockId(), clusterInfo.clusterId(),
                        clusterInfo.startPos(), clusterInfo.offset(), idxs);
                List<IndexValuePair> curPairs = indexBlock.query(icQuads);

                if(!curPairs.isEmpty()){
                    if(pairs.isEmpty()){
                        pairs.addAll(curPairs);
                    }else if(pairs.get(pairs.size() - 1).timestamp() <= curPairs.get(0).timestamp()){
                        pairs.addAll(curPairs);
                    }else{
                        pairs = NaiveIndex.mergeIndexValuePair(pairs, curPairs);
                    }
                }

            }
        }

        return pairs;
    }

    public List<IndexValuePair> getPairsFromBuffer(String eventType, List<IndependentConstraint> icList){
        List<ICQueryQuad> icQuads = new ArrayList<>(icList.size());
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();
            icQuads.add(new ICQueryQuad(idx, mark, ic.getMinValue(), ic.getMaxValue()));
        }
        return bufferPool.query(orderedFlag, eventType, icQuads);
    }

    public SortedIntervalSet generateIntervalSet(List<IndexValuePair> pairs, long leftOffset, long rightOffset){
        SortedIntervalSet intervals = new SortedIntervalSet(pairs.size() * 2 /3);
        for(IndexValuePair pair : pairs){
            long ts = pair.timestamp();
            intervals.insert(ts + leftOffset, ts + rightOffset);
        }
        return intervals;
    }

    public boolean storeIndexBlock(ByteBuffer buffer){
        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
            // note that this page may not be full, but it doesn't matter
            byte[] array = buffer.array();
            out.write(array);
            out.flush();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    // [notice] you can choose read entire index block, or read index block on demand
    public MappedIdxBlk getIndexBlock(int blockId, int clusterId, int startPos, int offset, int[] idxs){
        IdxBlkMetaInfo idxBlkMetaInfo = idxBlkMetaInfoList.get(blockId);

        if(fileChannel == null){
            // whether we need to extract raf?
            try{
                raf = new RandomAccessFile(file, "r");
                fileChannel = raf.getChannel();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        MappedIdxBlk indexBlock = null;
        try{
            // choice 1: read index block on demand
            indexBlock = new MappedIdxBlk(idxBlkMetaInfo, clusterId, startPos, offset, indexAttrNum, idxs, fileChannel);

            // choice 2: read entire index block
            //indexBlock = new MappedIdxBlk(idxBlkMetaInfo, clusterId, startPos, offset, indexAttrNum, fileChannel);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return indexBlock;
    }
}
