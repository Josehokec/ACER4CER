package acer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import arrival.JsonMap;
import automaton.NFA;
import common.*;
import condition.IndependentConstraint;
import condition.IndependentConstraintQuad;
import join.AbstractJoinEngine;
import join.Tuple;
import method.Index;
import pattern.QueryPattern;
import store.*;

/**
 * Fast mainly includes three components
 * BufferPool, IndexPartition and SynopsisTable
 * ----
 * EventPattern is simple sequential event pattern
 * QueryPattern is complex event pattern
 */
public class ACER extends Index{
    record BlockPosition(long startPos, int offset){}
    private List<Long> attrMinValues;                       // minimum values for indexed attribute
    private List<Long> attrMaxRange;                        // maximum values for indexed attribute
    private BufferPool bufferPool;                          // bufferPool store event (in memory)
    private SynopsisTable synopsisTable;                    // store each event type synopsis information
    private final File file;                                // all index partition will be written in this file
    private final List<BlockPosition> blockPositions;       // block storage position information
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    public ACER(String indexName){
        super(indexName);

        String dir = System.getProperty("user.dir");
        // new File(dir).getParent()
        String storePath = dir + File.separator + "store";
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
        blockPositions = new ArrayList<>(1024);
        raf = null;
        fileChannel = null;
    }

    @Override
    public void initial() {
        setMinMaxArray();
        synopsisTable = new SynopsisTable();
        bufferPool = new BufferPool(indexAttrNum, attrMaxRange);

        // Set the arrival rate for each event, which is later used to calculate the selection rate
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "arrival" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);
        // sampling
        reservoir = new ReservoirSampling(indexAttrNum);
    }

    /**
     * fast index need to set the min value array and max range array
     */
    public final void setMinMaxArray(){
        attrMinValues = new ArrayList<>(indexAttrNum);
        attrMaxRange = new ArrayList<>(indexAttrNum);

        String[] indexAttrNames = getIndexAttrNames();
        for(int i = 0; i < indexAttrNum; ++i){
            String name = indexAttrNames[i];
            int idx = schema.getAttrNameIdx(name);
            attrMinValues.add(schema.getIthAttrMinValue(idx));
            attrMaxRange.add(schema.getIthAttrMaxValue(idx) - attrMinValues.get(i));
        }
    }

    @Override
    public boolean insertRecord(String record) {
        fileChannel = null;
        // record: TYPE_10,127,246,854.27,534.69,1683169267388
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();

        String[] splits = record.split(",");

        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum];

        // put indexed attribute into attrValArray
        for(int i = 0; i < splits.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                eventType = splits[i];
            }else if(attrTypes[i].equals("TIMESTAMP")){
                timestamp = Long.parseLong(splits[i]);
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
            }else{
                throw new RuntimeException("Don't support this data type: " + attrTypes[i]);
            }
        }

        // The reservoir stores unchanged values
        reservoir.sampling(attrValArray, autoIndices);

        // Since RangeBitmap can only store non-negative integers,
        // it is necessary to transform the transformed value to: y = x - min
        for(int i = 0; i < indexAttrNum; ++i){
            attrValArray[i] -= attrMinValues.get(i);
        }

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        ACERTemporaryTriple triple = new ACERTemporaryTriple(false, timestamp, rid, attrValArray);

        // insert triple to buffer, then store the index block
        int blockNum = blockPositions.size();
        ByteBuffer buff = bufferPool.insertRecord(eventType, blockNum, triple, synopsisTable);
        // buffer pool reaches its capability, then we need to write file
        if(buff != null){
            // if buff is not null. then it means generate a new index block, then we need to flush index block to disk
            int writeByteSize = storeIndexBlock(buff);
            // System.out.println(blockNum + " -th block size: " + writeByteSize);
            if(blockNum == 0){
                blockPositions.add(new BlockPosition(0, writeByteSize));
            }else {
                BlockPosition blockPosition = blockPositions.get(blockNum - 1);
                long newStartPosition = blockPosition.startPos + blockPosition.offset;
                blockPositions.add(new BlockPosition(newStartPosition, writeByteSize));
            }
        }
        // update indices
        autoIndices++;
        return true;
    }

    public final int storeIndexBlock(ByteBuffer buff){
        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
            // note that this page may not be full, but it doesn't matter
            byte[] array = buff.array();
            out.write(array);
            out.flush();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return buff.capacity();
    }

    public final IndexBlock getIndexBlock(int blockId){
        //long startTime = System.nanoTime();
        IndexBlock indexBlock = null;
        BlockPosition blockPosition = blockPositions.get(blockId);

        // Obtain the file channel and then send the data from the specified location to memory
        try{
            if(fileChannel == null){
                raf = new RandomAccessFile(file, "r");
                fileChannel = raf.getChannel();
            }

            MappedByteBuffer readMappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    blockPosition.startPos, blockPosition.offset);
            indexBlock = IndexBlock.deserialize(readMappedBuffer);
        }catch(Exception e){
            e.printStackTrace();
        }

        // long endTime = System.nanoTime(); System.out.println("read a block cost: " + (endTime - startTime) + "ns");
        return indexBlock;
    }

    @Override
    public boolean insertBatchRecord(String[] record) {
        // currently we do not support this function
        return false;
    }

    @Override
    public int processCountQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join) {
        long filterStartTime = System.nanoTime();
        List<RidVarIdPair> ridVarIdPairs = twoStageFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<List<byte[]>> buckets = getByteEventsBucket(ridVarIdPairs, pattern.getPatternLen());
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");

        int ans;
        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        switch(strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.countTupleUsingFollowBy(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.countTupleUsingFollowByAny(pattern, buckets);
            default -> {
                System.out.println("this strategy do not support, default choose S2");
                ans = join.countTupleUsingFollowBy(pattern, buckets);
            }
        }
        long matchEndTime = System.nanoTime();

        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");
        return ans;
    }

    @Override
    public List<Tuple> processTupleQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join) {
        long filterStartTime = System.nanoTime();
        List<RidVarIdPair> ridVarIdPairs = twoStageFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<List<byte[]>> buckets = getByteEventsBucket(ridVarIdPairs, pattern.getPatternLen());
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");

        List<Tuple> ans;
        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        switch(strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.getTupleUsingFollowBy(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.getTupleUsingFollowByAny(pattern, buckets);
            default -> {
                System.out.println("this strategy do not support, default choose S2");
                ans = join.getTupleUsingFollowBy(pattern, buckets);
            }
        }
        long matchEndTime = System.nanoTime();
        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");

        return ans;
    }

    @Override
    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        long filterStartTime = System.nanoTime();
        // twoStageFilteringPlus vs. twoStageFiltering
        List<RidVarNamePair> ridVarNamePairs = twoStageFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = getEventsUsingRidVarNamePair(ridVarNamePairs);
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        // System.out.println("filtered event number: " + events.size());

        // here we only support nfa to process complex event pattern
        long matchStartTime = System.nanoTime();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, pattern.getStrategy());
        }
        int ans = nfa.countTuple();
        long matchEndTime = System.nanoTime();
        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");
        return ans;
    }

    @Override
    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        long filterStartTime = System.nanoTime();
        List<RidVarNamePair> ridVarNamePairs = twoStageFiltering(pattern);
        // System.out.println("size： " + ridVarNamePairs.size());System.exit(0);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = getEventsUsingRidVarNamePair(ridVarNamePairs);

        //debug
//        for(byte[] event : events){
//            int timeIdx = schema.getTimestampIdx();
//            int typeIdx = schema.getTypeIdx();
//            String curType = schema.getTypeFromBytesRecord(event, typeIdx);
//            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(event, timeIdx));
//            if(timestamp == 1685410320){
//                System.out.println("final| type: " + curType + " ts: 1685410320");
//            }
//        }

        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");

        // here we only support nfa to process complex event pattern
        long matchStartTime = System.nanoTime();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, pattern.getStrategy());
        }
        List<Tuple> ans = nfa.getTuple(schema);
        long matchEndTime = System.nanoTime();
        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");
        return ans;
    }

    /**
     * here we use two stage filter algorithm to decrease disk IO cost
     * here has an assumption: matched tuple is far less than single events
     * *******************************************************
     * stage one aims to decrease index block IO
     * step 1: calculate each variable selectivity and sort based on selectivity
     * step 2: generate first interval set
     * step 3: filter intervals, decrease number of intervals
     * *******************************************************
     * step 4: based on firstIntervalSet, obtain RidTime Pairs
     * step5: generate second interval set
     * @param pattern   event pattern
     * @return          each variable event
     */
    public final List<RidVarIdPair> twoStageFiltering(EventPattern pattern) {
        // 1. calculate each variable selectivity, and sort
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqEventTypes.length;
        record SelectivityIndexPair(double sel, int varId){}
        // variable v_i overall selectivity
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);
        double sumArrival = 0;
        for (double a : arrivals.values()) {
            sumArrival += a;
        }
        // calculate the overall selection rate for each variable
        for (int i = 0; i < patternLen; ++i) {
            String varName = seqVarNames[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
            double sel = arrivals.get(seqEventTypes[i]) / sumArrival;
            if (icList != null) {
                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
                }
            }
            varSelList.add(new SelectivityIndexPair(sel, i));
        }
        // sort
        varSelList.sort(Comparator.comparingDouble(SelectivityIndexPair::sel));
        int minVarId = varSelList.get(0).varId();
        String minVarName = seqVarNames[minVarId];
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(minVarName);

        List<IndexValuePair> minPairs = getVarIndexValuePairsFromDisk(seqEventTypes[minVarId], icList);

        // 2.generate SortedIntervalSet (for index block)
        long leftOffset, rightOffset;
        long tau = pattern.getTau();
        if(minVarId == 0){
            leftOffset = 0;
            rightOffset = tau;
        }else if(minVarId == patternLen - 1){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet firstIntervalSet = generateIntervalSet(minPairs, leftOffset, rightOffset);

        // here we cache the results
        List<List<Long>> allVarStartTime = new ArrayList<>(patternLen);
        List<List<Long>> allVarEndTime = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            allVarStartTime.add(null);
            allVarEndTime.add(null);
        }

        // 3.filter index block using intervals
        for(int i = 1; i < patternLen; ++i){
            int varId = varSelList.get(i).varId();
            String curEventType = seqEventTypes[varId];
            List<Long> startTimeList = new ArrayList<>();
            List<Long> endTimeList = new ArrayList<>();

            synopsisTable.getStartEndTimestamp(curEventType, startTimeList, endTimeList);
            firstIntervalSet.checkOverlap(startTimeList, endTimeList);

            allVarStartTime.set(varId, startTimeList);
            allVarEndTime.set(varId, endTimeList);
        }

        // stage two: using interval to filter events
        // 4.based on firstIntervalSet, obtain RidTime Pairs
        List<List<IndexValuePair>> pairsList = new ArrayList<>(patternLen);
        record VariableEventNum(int recordNum, int varId){}
        List<VariableEventNum> varEventNumList = new ArrayList<>(patternLen);
        for(int i = 0; i < patternLen; ++i){
            List<IndexValuePair> curIndexValuePairs;
            if(i == minVarId){
                // filter minimum varId IndexValuePair
                curIndexValuePairs = new ArrayList<>(minPairs.size());
                // second arg (curIndexValuePairs) is return value
                firstIntervalSet.including(minPairs, curIndexValuePairs);
                // access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(seqEventTypes[i], icList);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }else{
                String curEventType = seqEventTypes[i];
                List<Long> startTimeList = allVarStartTime.get(i);
                List<Long> endTimeList = allVarEndTime.get(i);

                List<Boolean> overlaps = firstIntervalSet.checkOverlap(startTimeList, endTimeList);
                List<ClusterInfo> overlapClusterInfoList = synopsisTable.getOverlapClusterInfo(curEventType, overlaps);

                List<IndependentConstraint> curICList = pattern.getICListUsingVarName(seqVarNames[i]);

                // access events from disk
                curIndexValuePairs = getVarIndexValuePairsFromDisk(curICList, overlapClusterInfoList);

                // access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(seqEventTypes[i], curICList);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }

            pairsList.add(curIndexValuePairs);
            varEventNumList.add(new VariableEventNum(curIndexValuePairs.size(), i));
        }

        // 5.generate second interval set
        varEventNumList.sort(Comparator.comparingInt(VariableEventNum::recordNum));
        int minNumPos = varEventNumList.get(0).varId();
        if(minNumPos == 0){
            leftOffset = 0;
            rightOffset = tau;
        }else if(minNumPos == patternLen - 1){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet secondIntervalSet = generateIntervalSet(pairsList.get(minNumPos), leftOffset, rightOffset);

        // update interval set
        for(List<IndexValuePair> pairs : pairsList){
            secondIntervalSet.including(pairs);
        }

        List<RidVarIdPair> ans = new ArrayList<>();

        // update query rid list
        for(int i = 0; i < patternLen; ++i){
            List<IndexValuePair> curPairs = pairsList.get(i);
            // check overlap
            List<RidVarIdPair> curRidVarIdPair = secondIntervalSet.including(curPairs, i);
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans = mergeRidVarIdPair(ans, curRidVarIdPair);
        }

        return ans;
    }

    /**
     * @param pattern       query pattern (support complex event pattern)
     * @return              each variable event
     */
    public final List<RidVarNamePair> twoStageFiltering(QueryPattern pattern) {
        // 1. calculate each variable selectivity, and sort
        if(pattern.existOROperator()){
            System.out.println("this pattern exists `OR` operator, we do not support this operator");
            throw new RuntimeException("we can not process this pattern");
        }

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        Set<String> varNameSet = varTypeMap.keySet();
        record SelectivityIndexPair(double selectivity, String varName){}
        double sumArrival = 0;
        for (double a : arrivals.values()) {
            sumArrival += a;
        }

        // variable v_i overall selectivity
        int patternLen = varNameSet.size();
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);

        // calculate the overall selection rate for each variable
        for(String curVarName : varNameSet){
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(curVarName);
            String curEventType = varTypeMap.get(curVarName);
            double sel = arrivals.get(curEventType) / sumArrival;
            if (icList != null) {
                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
                }
            }
            varSelList.add(new SelectivityIndexPair(sel, curVarName));
            // System.out.println("VarName: " + curVarName + " selectivity: " + sel);

        }
        // sort based on selectivity
        varSelList.sort(Comparator.comparingDouble(SelectivityIndexPair::selectivity));
        String minVarName = varSelList.get(0).varName();

        String minVarType = varTypeMap.get(minVarName);
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(minVarName);

        List<IndexValuePair> minPairs = getVarIndexValuePairsFromDisk(minVarType, icList);

        // 2.generate SortedIntervalSet (for index block)
        long leftOffset, rightOffset;
        long tau = pattern.getTau();
        if(pattern.isOnlyLeftMostNode(minVarName)){
            leftOffset = 0;
            rightOffset = tau;
        }else if(pattern.isOnlyRightMostNode(minVarName)){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet firstIntervalSet = generateIntervalSet(minPairs, leftOffset, rightOffset);

        // here we cache the results
        Map<String, List<List<Long>>> varStartEndTime = new HashMap<>();

        // 3.filter index block using intervals
        for(int i = 1; i < patternLen; ++i){
            String curVarName = varSelList.get(i).varName();
            String curEventType = varTypeMap.get(curVarName);
            List<Long> startTimeList = new ArrayList<>();
            List<Long> endTimeList = new ArrayList<>();

            synopsisTable.getStartEndTimestamp(curEventType, startTimeList, endTimeList);
            firstIntervalSet.checkOverlap(startTimeList, endTimeList);

            List<List<Long>> startEndTime = new ArrayList<>(2);
            startEndTime.add(startTimeList);
            startEndTime.add(endTimeList);

            varStartEndTime.put(curVarName, startEndTime);
        }

        // stage two: using interval to filter events
        // 4.based on firstIntervalSet, obtain RidTime Pairs
        Map<String, List<IndexValuePair>> varQueryResult = new HashMap<>();

        record VariableEventNum(int recordNum, String varName){}
        List<VariableEventNum> varEventNumList = new ArrayList<>(patternLen);

        for(String curVarName : varNameSet){
            List<IndexValuePair> curIndexValuePairs;
            String curVarType = varTypeMap.get(curVarName);

            if(curVarName.equals(minVarName)){
                // filter minimum varName IndexValuePair
                curIndexValuePairs = new ArrayList<>(minPairs.size());
                // second argument (curIndexValuePairs) is return value
                firstIntervalSet.including(minPairs, curIndexValuePairs);
                // access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(curVarType, icList);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }else{
                List<Long> startTimeList = varStartEndTime.get(curVarName).get(0);
                List<Long> endTimeList = varStartEndTime.get(curVarName).get(1);

                List<Boolean> overlaps = firstIntervalSet.checkOverlap(startTimeList, endTimeList);
                List<ClusterInfo> overlapClusterInfoList = synopsisTable.getOverlapClusterInfo(curVarType, overlaps);

                List<IndependentConstraint> curICList = pattern.getICListUsingVarName(curVarName);
                // access events from disk
                curIndexValuePairs = getVarIndexValuePairsFromDisk(curICList, overlapClusterInfoList);

                // access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(curVarType, curICList);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }

            varQueryResult.put(curVarName, curIndexValuePairs);
            varEventNumList.add(new VariableEventNum(curIndexValuePairs.size(), curVarName));
        }

        // 5.generate second interval set
        varEventNumList.sort(Comparator.comparingInt(VariableEventNum::recordNum));

        minVarName = varEventNumList.get(0).varName();
        if(pattern.isOnlyLeftMostNode(minVarName)){
            leftOffset = 0;
            rightOffset = tau;
        }else if(pattern.isOnlyRightMostNode(minVarName)){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet secondIntervalSet = generateIntervalSet(varQueryResult.get(minVarName), leftOffset, rightOffset);

        // update interval set, greedy method
        for(int i = 1; i < patternLen; ++i){
            String curVarName = varEventNumList.get(i).varName();
            List<IndexValuePair> pairs = varQueryResult.get(curVarName);
            secondIntervalSet.including(pairs);
        }

        // 6.filter query rid list
        List<RidVarNamePair> ans = new ArrayList<>();
        for(String curVarName : varNameSet){
            List<IndexValuePair> curPairs = varQueryResult.get(curVarName);
            // check overlap
            List<RidVarNamePair> curRidVarIdPair = secondIntervalSet.including(curPairs, curVarName);
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans = mergeRidVarNamePair(ans, curRidVarIdPair);
        }

        return ans;
    }

    /**
     * query optimization: when variable v_i accesses one index block,
     * other variables also access this index block
     * @param pattern       query pattern
     * @return              query result
     */
    public final List<RidVarNamePair> twoStageFilteringPlus(QueryPattern pattern){
        // 1. calculate each variable selectivity, and sort
        if(pattern.existOROperator()){
            System.out.println("this pattern exists `OR` operator, we do not support this operator");
            throw new RuntimeException("we can not process this pattern");
        }

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        Set<String> varNameSet = varTypeMap.keySet();
        record SelectivityIndexPair(double selectivity, String varName){}
        double sumArrival = 0;
        for (double a : arrivals.values()) {
            sumArrival += a;
        }

        // variable v_i overall selectivity
        int patternLen = varNameSet.size();
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);
        // calculate the overall selection rate for each variable
        for(String curVarName : varNameSet){
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(curVarName);
            String curEventType = varTypeMap.get(curVarName);
            double sel = arrivals.get(curEventType) / sumArrival;
            if (icList != null) {
                for (IndependentConstraint ic : icList) {
                    String attrName = ic.getAttrName();
                    int idx = indexAttrNameMap.get(attrName);
                    sel *= reservoir.selectivity(idx, ic.getMinValue(), ic.getMaxValue());
                }
            }
            varSelList.add(new SelectivityIndexPair(sel, curVarName));
        }
        // sort based on selectivity
        varSelList.sort(Comparator.comparingDouble(SelectivityIndexPair::selectivity));
        String minVarName = varSelList.get(0).varName();

        // here we have optimized the process
        Set<Integer> accessedBlockId = new HashSet<>(1024);
        Map<String, List<IndexValuePair>> varQueryResult = getVarIndexValuePairsFromDisk(minVarName, pattern, accessedBlockId);

        // generate first SortedIntervalSet (for index block)
        long leftOffset, rightOffset;
        long tau = pattern.getTau();
        if(pattern.isOnlyLeftMostNode(minVarName)){
            leftOffset = 0;
            rightOffset = tau;
        }else if(pattern.isOnlyRightMostNode(minVarName)){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }

        SortedIntervalSet firstIntervalSet = generateIntervalSet(varQueryResult.get(minVarName), leftOffset, rightOffset);

        // here we cache the results
        Map<String, List<List<Long>>> varStartEndTime = new HashMap<>();

        // filter index block using intervals
        for(int i = 1; i < patternLen; ++i){
            String curVarName = varSelList.get(i).varName();
            String curEventType = varTypeMap.get(curVarName);
            List<Long> startTimeList = new ArrayList<>();
            List<Long> endTimeList = new ArrayList<>();

            synopsisTable.getStartEndTimestamp(curEventType, startTimeList, endTimeList);
            firstIntervalSet.checkOverlap(startTimeList, endTimeList);

            List<List<Long>> startEndTime = new ArrayList<>(2);
            startEndTime.add(startTimeList);
            startEndTime.add(endTimeList);

            varStartEndTime.put(curVarName, startEndTime);
        }

        // stage two: using interval to filter events
        record VariableEventNum(int recordNum, String varName){}
        List<VariableEventNum> varEventNumList = new ArrayList<>(patternLen);

        for(String curVarName : varNameSet){
            List<IndexValuePair> curIndexValuePairs;
            String curVarType = varTypeMap.get(curVarName);
            List<IndependentConstraint> curICList = pattern.getICListUsingVarName(curVarName);
            if(curVarName.equals(minVarName)){
                // this is the variable with minimum selectivity,
                // so we only need to access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(curVarType, curICList);
                curIndexValuePairs = varQueryResult.get(curVarName);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }else{
                List<Long> startTimeList = varStartEndTime.get(curVarName).get(0);
                List<Long> endTimeList = varStartEndTime.get(curVarName).get(1);

                List<Boolean> overlaps = firstIntervalSet.checkOverlap(startTimeList, endTimeList);
                // obtain overlap and do not access block info list
                List<ClusterInfo> filteredClusterInfo = synopsisTable.getOverlapAndNotAccessClusterInfo(curVarType, overlaps, accessedBlockId);

                // access events from disk
                curIndexValuePairs = getVarIndexValuePairsFromDisk(curICList, filteredClusterInfo);
                // merge result based on timestamp
                curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, varQueryResult.get(curVarName));
                // access events from buffer
                List<IndexValuePair> bufferPairs = getVarIndexValuePairFromBuffer(curVarType, curICList);
                // curIndexValuePairs.addAll(bufferPairs);
                // new version: support out-of-order insertion
                int curIndexValuePairsSize = curIndexValuePairs.size();
                int bufferPairsSize = bufferPairs.size();
                if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                    curIndexValuePairs.addAll(bufferPairs);
                }else{
                    if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < bufferPairs.get(0).timestamp()){
                        curIndexValuePairs.addAll(bufferPairs);
                    }else{
                        curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, bufferPairs);
                    }
                }
            }
            // greedy to filter
            varEventNumList.add(new VariableEventNum(curIndexValuePairs.size(), curVarName));
        }

        // 5.generate second interval set
        varEventNumList.sort(Comparator.comparingInt(VariableEventNum::recordNum));

        minVarName = varEventNumList.get(0).varName();
        if(pattern.isOnlyLeftMostNode(minVarName)){
            leftOffset = 0;
            rightOffset = tau;
        }else if(pattern.isOnlyRightMostNode(minVarName)){
            leftOffset = -tau;
            rightOffset = 0;
        }else{
            leftOffset = -tau;
            rightOffset = tau;
        }
        SortedIntervalSet secondIntervalSet = generateIntervalSet(varQueryResult.get(minVarName), leftOffset, rightOffset);

        // update interval set, greedy method
        for(int i = 1; i < patternLen; ++i){
            String curVarName = varEventNumList.get(i).varName();
            List<IndexValuePair> pairs = varQueryResult.get(curVarName);
            secondIntervalSet.including(pairs);
        }

        // 6.filter query rid list
        List<RidVarNamePair> ans = new ArrayList<>();
        for(String curVarName : varNameSet){
            List<IndexValuePair> curPairs = varQueryResult.get(curVarName);
            // check overlap
            List<RidVarNamePair> curRidVarIdPair = secondIntervalSet.including(curPairs, curVarName);
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans = mergeRidVarNamePair(ans, curRidVarIdPair);
        }

        return ans;
    }

    public final List<byte[]> getEventsUsingRidVarNamePair(List<RidVarNamePair> ridVarNamePairs){
        EventStore store = schema.getStore();

        int size = ridVarNamePairs.size();
        List<byte[]> ans = new ArrayList<>(size);

        RID preRid = null;
        for(RidVarNamePair pair : ridVarNamePairs){
            RID curRid = pair.rid();
            // remove duplicate events
            if(!curRid.equals(preRid)){
                byte[] bytesRecord = store.readByteRecord(curRid);
                ans.add(bytesRecord);
                preRid = curRid;
            }
        }

        return ans;
    }

    public final List<RidVarNamePair> mergeRidVarNamePair(List<RidVarNamePair> a, List<RidVarNamePair> b){
        if(a == null || b == null){
            return a == null ? b : a;
        }

        int size1 = a.size();
        int size2 = b.size();
        List<RidVarNamePair> ans = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while(i < size1 && j < size2) {
            RID rid1 = a.get(i).rid();
            RID rid2 = b.get(j).rid();
            if(rid1.compareTo(rid2) <= 0){
                ans.add(a.get(i++));
            }else{
                ans.add(b.get(j++));
            }
        }

        while(i < size1){
            ans.add(a.get(i++));
        }

        while(j < size2){
            ans.add(b.get(j++));
        }

        return ans;
    }

    public final SortedIntervalSet generateIntervalSet(List<IndexValuePair> pairs, long leftOffset, long rightOffset){
        SortedIntervalSet intervals = new SortedIntervalSet(pairs.size());
        for(IndexValuePair pair : pairs){
            if(pair != null){
                long ts = pair.timestamp();
                intervals.insert(ts + leftOffset, ts + rightOffset);
            }
        }
        return intervals;
    }

    // support update/deletion
    public final List<IndexValuePair> getVarIndexValuePairsFromDisk(String eventType, List<IndependentConstraint> icList){
        List<IndexValuePair> pairs = new ArrayList<>(1024);
        List<IndexValuePair> deletedPairs = new ArrayList<>(1024);
        int icNum = icList.size();
        List<IndependentConstraintQuad> icQuads = new ArrayList<>(icNum);
        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();

            IndependentConstraintQuad quad;
            switch (mark) {
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                case 1 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue());
                case 2 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx));
                case 3 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx));
                default -> {
                    System.out.println("mark value: " + mark + " , it is anomaly");
                    quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                            ic.getMaxValue() - attrMinValues.get(idx));
                }
            }
            icQuads.add(quad);
        }

        List<ClusterInfo> clusterInfoList = synopsisTable.getClusterInfo(eventType);

        for(ClusterInfo clusterInfo : clusterInfoList){
            List<Long> maxValues = clusterInfo.maxValues();
            List<Long> minValues = clusterInfo.minValues();
            // use maximum/minimum to filter
            boolean noResults = false;
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                // ensure has results
                if(maxValues.get(idx) < min || minValues.get(idx) > max){
                    noResults = true;
                    break;
                }
            }
            // if this block may have results, then query the block
            if(!noResults){
                IndexBlock indexBlock = getIndexBlock(clusterInfo.indexBlockId());
                // new version: support out-of-order insertion
                // List<IndexValuePair> curPairs = indexBlock.query(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                List<List<IndexValuePair>> twoPairs = indexBlock.query4Deletion(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                List<IndexValuePair> curPairs = twoPairs.get(0);


                List<IndexValuePair> curDeletedPairs = twoPairs.get(1);
                // pairs.addAll(curPairs);
                // new version: support out-of-order insertion
                int pairsSize = pairs.size();
                int curPairsSize = curPairs.size();
                if(pairsSize == 0 || curPairsSize == 0){
                    pairs.addAll(curPairs);
                }else{
                    IndexValuePair lastItem = pairs.get(pairsSize - 1);
                    IndexValuePair firstItem = curPairs.get(0);
                    if(firstItem.timestamp() >= lastItem.timestamp()){
                        pairs.addAll(curPairs);
                    }else{
                        // need to merge
                        pairs = mergeIndexValuePair(pairs, curPairs);
                    }
                }
                deletedPairs.addAll(curDeletedPairs);
            }
        }

        // new version: support out-of-order insertion
        // remove old events and deleted events
        List<IndexValuePair> ans = new ArrayList<>(pairs.size());
        Set<RID> deletedRidSet = new HashSet<>(512);
        for(IndexValuePair pair : deletedPairs){
            deletedRidSet.add(pair.rid());
        }

        long preTs = -1;
        RID preRid = new RID(-1, Short.MIN_VALUE);
        IndexValuePair prePair = null;
        // timestamp-ordered and rid-ordered
        for(IndexValuePair pair : pairs){
            long curTs = pair.timestamp();
            RID curRid = pair.rid();
            // debug
            if(curTs < preTs || curRid.compareTo(preRid) < 0){
                throw new RuntimeException("serious bug? curTs: " + curTs + " preTs: " + preTs + "curRid: " +curRid + "preRid: " + preRid);
            }
            if(curTs == preTs){
                prePair = pair;
            }else{
                // curTs > preTs
                if(prePair != null && !deletedRidSet.contains(curRid)){
                    ans.add(prePair);
                }
                prePair = pair;
                preTs = curTs;
                preRid = curRid;
            }

        }
        if(!pairs.isEmpty()){
            IndexValuePair lastPair = pairs.get(pairs.size()-1);
            if(!deletedRidSet.contains(lastPair.rid())){
                ans.add(lastPair);
            }
        }
        return ans;
    }

    // support update/deletion
    public final List<IndexValuePair> getVarIndexValuePairsFromDisk(List<IndependentConstraint> icList, List<ClusterInfo> overlapClusterInfoList){
        List<IndexValuePair> pairs = new ArrayList<>(1024);
        List<IndexValuePair> deletedPairs = new ArrayList<>(1024);
        int icNum = icList.size();
        List<IndependentConstraintQuad> icQuads = new ArrayList<>(icNum);
        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();

            IndependentConstraintQuad quad;
            switch (mark) {
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                case 1 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue());
                case 2 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx));
                case 3 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx));
                default -> {
                    System.out.println("mark value: " + mark + " , it is anomaly");
                    quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                            ic.getMaxValue() - attrMinValues.get(idx));
                }
            }
            icQuads.add(quad);
        }

        for(ClusterInfo clusterInfo : overlapClusterInfoList){
            List<Long> maxValues = clusterInfo.maxValues();
            List<Long> minValues = clusterInfo.minValues();
            // use maximum/minimum to filter
            boolean noResults = false;
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                // ensure has results
                if(maxValues.get(idx) < min || minValues.get(idx) > max){
                    noResults = true;
                    break;
                }
            }

            // if this block may have results, then query the block
            if(!noResults){
                IndexBlock indexBlock = getIndexBlock(clusterInfo.indexBlockId());
                // new version: support out-of-order insertion
                // List<IndexValuePair> curPairs = indexBlock.query(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                List<List<IndexValuePair>> twoPairs = indexBlock.query4Deletion(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                List<IndexValuePair> curPairs = twoPairs.get(0);


                List<IndexValuePair> curDeletedPairs = twoPairs.get(1);
                // pairs.addAll(curPairs);
                // new version: support out-of-order insertion
                int pairsSize = pairs.size();
                int curPairsSize = curPairs.size();
                if(pairsSize == 0 || curPairsSize == 0){
                    pairs.addAll(curPairs);
                }else{
                    IndexValuePair lastItem = pairs.get(pairsSize - 1);
                    IndexValuePair firstItem = curPairs.get(0);
                    if(firstItem.timestamp() >= lastItem.timestamp()){
                        pairs.addAll(curPairs);
                    }else{
                        // need to merge
                        pairs = mergeIndexValuePair(pairs, curPairs);
                    }
                }
                deletedPairs.addAll(curDeletedPairs);
            }
        }

        // new version: support out-of-order insertion
        // remove old events and deleted events
        List<IndexValuePair> ans = new ArrayList<>(pairs.size());
        Set<RID> deletedRidSet = new HashSet<>(512);
        for(IndexValuePair pair : deletedPairs){
            deletedRidSet.add(pair.rid());
        }

        long preTs = -1;
        RID preRid = new RID(-1, Short.MIN_VALUE);
        IndexValuePair prePair = null;
        // timestamp-ordered and rid-ordered
        for(IndexValuePair pair : pairs){
            long curTs = pair.timestamp();
            RID curRid = pair.rid();
            if(curTs == preTs){
                prePair = pair;
            }else{
                // curTs > preTs
                if(prePair != null && !deletedRidSet.contains(curRid)){
                    ans.add(prePair);
                }
                prePair = pair;
                preTs = curTs;
                preRid = curRid;
            }
        }
        if (!pairs.isEmpty()) {
            IndexValuePair lastPair = pairs.get(pairs.size()-1);
            if(!deletedRidSet.contains(lastPair.rid())){
                ans.add(lastPair);
            }
        }
        return ans;
    }

    public final Map<String, List<IndexValuePair>> getVarIndexValuePairsFromDisk(String minVarName, QueryPattern pattern, Set<Integer> accessedBlockId){
        // two stage filtering plus version
        Map<String, String> varTypeMap = pattern.getVarTypeMap();

        Map<String, List<IndependentConstraintQuad>> allVarICList = new HashMap<>();
        Map<String, Map<Integer, int[]>> allVarRelatedBlocks = new HashMap<>();
        for(String varName : varTypeMap.keySet()){
            List<IndependentConstraint> curICList = pattern.getICListUsingVarName(varName);
            int icNum = curICList.size();
            List<IndependentConstraintQuad> curICQuads = new ArrayList<>(icNum);
            for (IndependentConstraint ic : curICList) {
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                int mark = ic.hasMinMaxValue();

                IndependentConstraintQuad quad;
                // transform query max and min value
                switch (mark) {
                    // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                    case 1 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue());
                    case 2 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx));
                    case 3 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                            ic.getMaxValue() - attrMinValues.get(idx));
                    default -> {
                        System.out.println("mark value: " + mark + " , it is anomaly");
                        quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                                ic.getMaxValue() - attrMinValues.get(idx));
                    }
                }
                curICQuads.add(quad);
            }
            allVarICList.put(varName, curICQuads);
            String curType = varTypeMap.get(varName);
            allVarRelatedBlocks.put(varName, synopsisTable.getRelatedBlocks(curType));
        }

        String minVarType = varTypeMap.get(minVarName);
        List<ClusterInfo> minClusterInfo = synopsisTable.getClusterInfo(minVarType);

        List<IndependentConstraintQuad> icQuads = allVarICList.get(minVarName);

        Map<String, List<IndexValuePair>> varQueryResult = new HashMap<>();

        for(ClusterInfo clusterInfo : minClusterInfo){
            List<Long> maxValues = clusterInfo.maxValues();
            List<Long> minValues = clusterInfo.minValues();
            // use maximum/minimum to filter
            boolean noResults = false;
            for(IndependentConstraintQuad quad : icQuads){
                int idx = quad.idx();
                long min = quad.min();
                long max = quad.max();
                // ensure has results
                if(maxValues.get(idx) < min || minValues.get(idx) > max){
                    noResults = true;
                    break;
                }
            }
            // if this block may have results, then query the block
            // here we will query variables, to avoid multiple access this block
            if(!noResults){
                int blockId = clusterInfo.indexBlockId();
                IndexBlock indexBlock = getIndexBlock(blockId);

                List<IndexValuePair> minPairs = indexBlock.query(icQuads, clusterInfo.startPos(), clusterInfo.offset());

                if(varQueryResult.containsKey(minVarName)){
                    // varQueryResult.get(minVarName).addAll(minPairs);
                    // new version: support out-of-order insertion
                    List<IndexValuePair> pairs = varQueryResult.get(minVarName);
                    int pairsSize = pairs.size();
                    int minPairsSize = minPairs.size();
                    if(pairsSize == 0 | minPairsSize == 0){
                        pairs.addAll(minPairs);
                        varQueryResult.put(minVarName, pairs);
                    }else{
                        if(minPairs.get(0).timestamp() >= pairs.get(pairsSize - 1).timestamp()){
                            pairs.addAll(minPairs);
                            varQueryResult.put(minVarName, pairs);
                        }else{
                            pairs = mergeIndexValuePair(pairs, minPairs);
                            varQueryResult.put(minVarName, pairs);
                        }
                    }
                }else{
                    varQueryResult.put(minVarName, minPairs);
                }

                accessedBlockId.add(blockId);
                // for other variables, we also query this block, but not use min/max values
                for(String varName : varTypeMap.keySet()){
                    if(!varName.equals(minVarName)){
                        if(allVarRelatedBlocks.get(varName).containsKey(blockId)){
                            int[] region = allVarRelatedBlocks.get(varName).get(blockId);
                            List<IndependentConstraintQuad> curICQuads = allVarICList.get(varName);
                            List<IndexValuePair> curPairs = indexBlock.query(curICQuads, region[0], region[1]);
                            if(varQueryResult.containsKey(varName)){
                                // varQueryResult.get(varName).addAll(curPairs);
                                // new version: support out-of-order insertion
                                List<IndexValuePair> pairs = varQueryResult.get(varName);
                                int curPairsSize = curPairs.size();
                                int pairsSize = pairs.size();
                                if(curPairsSize == 0 || pairsSize == 0){
                                    pairs.addAll(curPairs);
                                }else{
                                    if(curPairs.get(0).timestamp() >= pairs.get(pairsSize - 1).timestamp()){
                                        pairs.addAll(curPairs);
                                    }else{
                                        pairs = mergeIndexValuePair(pairs, curPairs);
                                        varQueryResult.put(varName, pairs);
                                    }
                                }
                            }else{
                                varQueryResult.put(varName, curPairs);
                            }
                        }
                    }
                }
            }
        }
        return varQueryResult;
    }

    public List<IndexValuePair> getVarIndexValuePairFromBuffer(String eventType, List<IndependentConstraint> icList){

        int icNum = icList.size();
        List<IndependentConstraintQuad> icQuads = new ArrayList<>(icNum);
        // transform query max and min value
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();

            IndependentConstraintQuad quad;
            switch (mark) {
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                case 1 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx), ic.getMaxValue());
                case 2 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue(), ic.getMaxValue() - attrMinValues.get(idx));
                case 3 -> quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                        ic.getMaxValue() - attrMinValues.get(idx));
                default -> {
                    System.out.println("mark value: " + mark + " , it is anomaly");
                    quad = new IndependentConstraintQuad(idx, mark, ic.getMinValue() - attrMinValues.get(idx),
                            ic.getMaxValue() - attrMinValues.get(idx));
                }
            }
            icQuads.add(quad);
        }

        return bufferPool.query(eventType, icQuads);
    }

    public final List<RidVarIdPair> mergeRidVarIdPair(List<RidVarIdPair> a, List<RidVarIdPair> b){
        if(a == null || b == null){
            return a == null ? b : a;
        }

        int size1 = a.size();
        int size2 = b.size();
        List<RidVarIdPair> ans = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while(i < size1 && j < size2) {
            RID rid1 = a.get(i).rid();
            RID rid2 = b.get(j).rid();
            if(rid1.compareTo(rid2) <= 0){
                ans.add(a.get(i++));
            }else{
                ans.add(b.get(j++));
            }
        }

        while(i < size1){
            ans.add(a.get(i++));
        }

        while(j < size2){
            ans.add(b.get(j++));
        }

        return ans;
    }

    public final List<IndexValuePair> mergeIndexValuePair(List<IndexValuePair> a, List<IndexValuePair> b){
        if(a == null || b == null){
            return a == null ? b : a;
        }

        int size1 = a.size();
        int size2 = b.size();
        List<IndexValuePair> ans = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while(i < size1 && j < size2) {
            IndexValuePair pairs1 = a.get(i);
            IndexValuePair pairs2 = b.get(j);
            long ts1 = pairs1.timestamp();
            long ts2 = pairs2.timestamp();

            if(ts1 < ts2){
                ans.add(pairs1);
                i++;
            }else if(ts1 > ts2){
                ans.add(pairs2);
                j++;
            }else{
                if(pairs1.rid().compareTo(pairs2.rid()) < 0){
                    ans.add(pairs1);
                    i++;
                }else{
                    ans.add(pairs2);
                    j++;
                }
            }
        }

        while(i < size1){
            ans.add(a.get(i++));
        }

        while(j < size2){
            ans.add(b.get(j++));
        }

        return ans;
    }

    public final List<List<byte[]>> getByteEventsBucket(List<RidVarIdPair> ridVarIdPairs, int patternLen){
        EventStore store = schema.getStore();
        List<List<byte[]>> buckets = new ArrayList<>(patternLen);

        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        for(RidVarIdPair ridVarIdPair : ridVarIdPairs){
            int ith = ridVarIdPair.varId();
            RID rid = ridVarIdPair.rid();
            byte[] bytesRecord = store.readByteRecord(rid);
            buckets.get(ith).add(bytesRecord);
        }

        return buckets;
    }

    @Override
    public void print() {
        // block storage position information
        System.out.println("Block storage position information as follows: ");
        for(int i = 0; i < blockPositions.size(); ++i){
            BlockPosition curBlockPosition = blockPositions.get(i);
            System.out.println("BlockId: " + i + " startPos: " +
                    curBlockPosition.startPos() + " offset: " + curBlockPosition.offset());
        }
        // bufferPool
        bufferPool.print();
        synopsisTable.print();
    }

    @Override
    public void delete(String record) {
        fileChannel = null;
        // record: TYPE_10,127,246,854.27,534.69,1683169267388
        String[] attrTypes = schema.getAttrTypes();
        String[] attrNames = schema.getAttrNames();

        String[] splits = record.split(",");

        String eventType = null;
        long timestamp = 0;
        long[] attrValArray = new long[indexAttrNum];

        // put indexed attribute into attrValArray
        for(int i = 0; i < splits.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                eventType = splits[i];
            }else if(attrTypes[i].equals("TIMESTAMP")){
                timestamp = Long.parseLong(splits[i]);
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
            }else{
                throw new RuntimeException("Don't support this data type: " + attrTypes[i]);
            }
        }

        // The reservoir stores unchanged values
        reservoir.sampling(attrValArray, autoIndices);

        // Since RangeBitmap can only store non-negative integers,
        // it is necessary to transform the transformed value to: y = x - min
        for(int i = 0; i < indexAttrNum; ++i){
            attrValArray[i] -= attrMinValues.get(i);
        }

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        ACERTemporaryTriple triple = new ACERTemporaryTriple(true, timestamp, rid, attrValArray);

        // insert triple to buffer, then store the index block
        int blockNum = blockPositions.size();
        ByteBuffer buff = bufferPool.deleteRecord(eventType, blockNum, triple, synopsisTable);
        // buffer pool reaches its capability, then we need to write file
        if(buff != null){
            // if buff is not null. then it means generate a new index block, then we need to flush index block to disk
            int writeByteSize = storeIndexBlock(buff);
            // System.out.println(blockNum + " -th block size: " + writeByteSize);
            if(blockNum == 0){
                blockPositions.add(new BlockPosition(0, writeByteSize));
            }else {
                BlockPosition blockPosition = blockPositions.get(blockNum - 1);
                long newStartPosition = blockPosition.startPos + blockPosition.offset;
                blockPositions.add(new BlockPosition(newStartPosition, writeByteSize));
            }
        }
        // update indices
        autoIndices++;
    }
}