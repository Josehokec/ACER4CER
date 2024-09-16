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
import condition.*;
import method.Index;
import pattern.QueryPattern;
import store.*;

/**
 * ACER mainly includes four components: BufferPool, IndexBlock, Page and SynopsisTable
 * new version: we support out-of-order insertion
 */
public class ACER extends Index{
    // new version: add a flag to mark ordered timestamp
    private boolean orderedFlag;                            // in-order or out-of-order insertion operation
    private boolean hasDeletion;                            // with/without deletion operation
    private double sumArrival;                        // total event arrival rate
    private long previousTimestamp = -1;                    // we need it to judge whether events are ordered
    private BufferPool bufferPool;                          // bufferPool store event (in memory)
    private SynopsisTable synopsisTable;                    // store each event type synopsis information
    private final File file;                                // all index block will be written in this file
    record BlockPosition(long startPos, int offset){}
    private final List<BlockPosition> blockPositions;       // block storage position information
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    public ACER(String indexName){
        super(indexName);
        // at beginning, events are ordered
        orderedFlag = true;
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
        synopsisTable = new SynopsisTable();
        hasDeletion = false;
        bufferPool = new BufferPool(indexAttrNum);
        // Set the arrival rate for each event, which is later used to calculate the selection rate
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
        // create a reservoir (use reservoir sampling)
        reservoir = new ReservoirSampling(indexAttrNum);
    }

    @Override
    public boolean insertOrDeleteRecord(String record, boolean deleteFlag) {
        if(deleteFlag){
            hasDeletion = true;
        }
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
                // we need to check whether out-of-order insertion
                if(timestamp < previousTimestamp){
                    orderedFlag = false;
                    previousTimestamp = Long.MAX_VALUE;
                }else{
                    previousTimestamp = timestamp;
                }
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
            } else if(!attrTypes[i].contains("CHAR")){
                // we do not support index string value, so we do not read char.X value
                throw new RuntimeException("Don't support this data type: " + attrTypes[i]);
            }
        }

        // we need to sample, note that deletion operation may lead to inaccurate estimation
        reservoir.sampling(attrValArray, autoIndices);

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        RID rid = store.insertByteRecord(bytesRecord);
        ACERTemporaryQuad quad = new ACERTemporaryQuad(deleteFlag, timestamp, rid, attrValArray);

        // insert quad to buffer, then store the index block
        int blockNum = blockPositions.size();
        ByteBuffer buff = bufferPool.insertOrDeleteRecord(orderedFlag, eventType, blockNum, quad, synopsisTable);
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
    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        long filterStartTime = System.nanoTime();
        // twoStageFilteringPlus vs. twoStageFiltering
        List<IndexValuePair> indexValuePairs = twoStageFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = getEvents(indexValuePairs);
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
        // twoStageFilteringPlus vs. twoStageFiltering
        List<IndexValuePair> indexValuePairs = twoStageFiltering(pattern);
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        long scanStartTime = System.nanoTime();
        List<byte[]> events = getEvents(indexValuePairs);

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

    public List<byte[]> getEvents(List<IndexValuePair> indexValuePairs){
        EventStore store = schema.getStore();
        int size = indexValuePairs.size();
        List<byte[]> ans = new ArrayList<>(size);

        for(IndexValuePair pair : indexValuePairs){
            RID curRid = pair.rid();
            byte[] bytesRecord = store.readByteRecord(curRid);
            ans.add(bytesRecord);
        }

//        System.out.println("size of filtered events: " + size);
//        for(byte[] event : ans){
//            System.out.println(schema.byteEventToString(event));
//        }
        return ans;
    }

    /**
     * @param pattern - query pattern (complex event pattern without OR operator)
     * @return        - each variable event
     */
    public final List<IndexValuePair> twoStageFiltering(QueryPattern pattern) {
        //, boolean enableOptimization
        // calculate each variable selectivity, and sort
        if(pattern.existOROperator()){
            System.out.println("this pattern exists `OR` operator, we do not support this operator");
            throw new RuntimeException("we can not process this pattern");
        }

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        Set<String> varNameSet = varTypeMap.keySet();
        // 中文注释：记录每个变量的整体选择率
        record SelectivityIndexPair(double selectivity, String varName){}

        int patternLen = varNameSet.size();
        List<SelectivityIndexPair> varSelList = new ArrayList<>(patternLen);

        // 中文注释：缓存所有变量的查询结果
        Map<String, List<IndexValuePair>> varQueryResult = new HashMap<>();

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
        // choose the variable with minimum selectivity to query

        String minVarName = varSelList.get(0).varName();
        String minVarType = varTypeMap.get(minVarName);
        List<IndependentConstraint> icList = pattern.getICListUsingVarName(minVarName);
        // here minSelVarPairs are timestamp-ordered
        List<IndexValuePair> minSelVarPairs = getVarIndexValuePairsFromDisk(minVarType, icList);

        List<List<IndexValuePair>> twobufferPairList = getVarIndexValuePairFromBuffer(minVarType, icList);
        if(orderedFlag){
            minSelVarPairs.addAll(twobufferPairList.get(0));
        }else{
            minSelVarPairs = mergeIndexValuePair(minSelVarPairs, twobufferPairList.get(0));
        }
        // query buffer
        if(hasDeletion){
            // 中文注释：之所以需要这么做是因为删除的事件已经存储到index block里面了
            // 这里删除是假定时间类型和时间戳能够唯一定义一个事件，一旦相同类型的两个事件有相同时间戳，这个删除算法将会有问题
            minSelVarPairs = deleteEventsWithSameTimestamp(minSelVarPairs, twobufferPairList.get(1));
        }
        varQueryResult.put(minVarName, minSelVarPairs);

        // generate SortedIntervalSet (for index blocks)
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
        SortedIntervalSet intervalSet = generateIntervalSet(minSelVarPairs, leftOffset, rightOffset);

        // stage two: using time intervals to filter events
        // 中文注释：接下来按照变量选择率依次查询有关变量
        for(int i = 1; i < patternLen; i++){
            String curVarName = varSelList.get(i).varName();
            String curVarType = varTypeMap.get(curVarName);
            List<Long> startTimeList = new ArrayList<>(512);
            List<Long> endTimeList = new ArrayList<>(512);
            synopsisTable.getStartEndTimestamp(curVarType, startTimeList, endTimeList);
            // 这个地方有问题的，不应该把间隙更新的
            List<Boolean> overlaps = intervalSet.checkOverlap(startTimeList, endTimeList);
            // 中文注释：过滤掉不能相交的索引块
            List<ClusterInfo> overlapClusterInfoList = synopsisTable.getOverlapClusterInfo(curVarType, overlaps);
            List<IndependentConstraint> curICList = pattern.getICListUsingVarName(curVarName);
            // access events from disk
            List<IndexValuePair> curIndexValuePairs = getVarIndexValuePairsFromDisk(curICList, overlapClusterInfoList);
            // access events from buffer
            List<List<IndexValuePair>> curTwoBufferPairList = getVarIndexValuePairFromBuffer(curVarType, curICList);
            if(orderedFlag){
                curIndexValuePairs.addAll(curTwoBufferPairList.get(0));
            }else{
                curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, curTwoBufferPairList.get(0));
            }
            // query buffer
            if(hasDeletion){
                // 中文注释：之所以需要这么做是因为删除的事件已经存储到index block里面了
                // 这里删除是假定时间类型和时间戳能够唯一定义一个事件，一旦相同类型的两个事件有相同时间戳，这个删除算法将会有问题
                curIndexValuePairs = deleteEventsWithSameTimestamp(curIndexValuePairs, curTwoBufferPairList.get(1));
            }
            // 中文注释：更新时间区间，把不在匹配区间的结果过滤掉；这个地方可以更新间隙，没被击中的肯定可以删除
            varQueryResult.put(curVarName, intervalSet.updateAndFilter(curIndexValuePairs));
        }

        // generate second interval set
        // 中文注释：经过一轮查询，时间区间会不断缩小（updateAndFilter函数完成的功能），然后我们再调用filterPairs
        // filter query rid list, we remove tree engine, so we
        List<IndexValuePair> ans = new ArrayList<>();
        for(String curVarName : varNameSet){
            List<IndexValuePair> curPairs = varQueryResult.get(curVarName);
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans = mergeIndexValuePair(ans, intervalSet.updateAndFilter(curPairs));
        }
        // 中文注释：注意到ans可能包含相同的rid，因为两个变量的时间类型可能相同，之后函数我们需要再次过滤
        return ans;
    }

    //query optimization: when variable v_i accesses one index block, other variables also access this index block
    @SuppressWarnings("unused")
    public final List<IndexValuePair> twoStageFilteringPlus(QueryPattern pattern){
        if(hasDeletion){
            throw new RuntimeException("This function does not support scenarios with deletion operations");
        }

        // 1. calculate each variable selectivity, and sort
        if(pattern.existOROperator()){
            System.out.println("this pattern exists `OR` operator, we do not support this operator");
            throw new RuntimeException("we can not process this pattern");
        }

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        Set<String> varNameSet = varTypeMap.keySet();
        record SelectivityIndexPair(double selectivity, String varName){}

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
                    // here we assume that each attribute is independent
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

        List<IndexValuePair> minBufferPairs = getVarIndexValuePairFromBuffer(varTypeMap.get(minVarName), pattern.getICListUsingVarName(minVarName)).get(0);
        if(orderedFlag){
            varQueryResult.get(minVarName).addAll(minBufferPairs);
        }else{
            varQueryResult.put(minVarName, mergeIndexValuePair(varQueryResult.get(minVarName), minBufferPairs));
        }
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

        SortedIntervalSet intervalSet = generateIntervalSet(varQueryResult.get(minVarName), leftOffset, rightOffset);

        // stage two: using time intervals to filter events
        // 中文注释：接下来按照变量选择率依次查询有关变量
        for(int i = 1; i < patternLen; i++){
            String curVarName = varSelList.get(i).varName();
            String curVarType = varTypeMap.get(curVarName);
            List<Long> startTimeList = new ArrayList<>(512);
            List<Long> endTimeList = new ArrayList<>(512);
            synopsisTable.getStartEndTimestamp(curVarType, startTimeList, endTimeList);
            List<Boolean> overlaps = intervalSet.checkOverlap(startTimeList, endTimeList);
            // obtain overlap and do not access block info list
            List<ClusterInfo> filteredClusterInfo = synopsisTable.getOverlapAndNotAccessClusterInfo(curVarType, overlaps, accessedBlockId);

            List<IndependentConstraint> curICList = pattern.getICListUsingVarName(minVarName);
            // access events from disk
            List<IndexValuePair >curIndexValuePairs = getVarIndexValuePairsFromDisk(curICList, filteredClusterInfo);
            // merge result based on timestamp
            curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, varQueryResult.get(curVarName));
            // access events from buffer
            List<IndexValuePair> curBufferPairs = getVarIndexValuePairFromBuffer(curVarType, curICList).get(0);
            // curIndexValuePairs.addAll(bufferPairs);
            // new version: support out-of-order insertion
            int curIndexValuePairsSize = curIndexValuePairs.size();
            int bufferPairsSize = curBufferPairs.size();
            if(curIndexValuePairsSize == 0 || bufferPairsSize == 0){
                curIndexValuePairs.addAll(curBufferPairs);
            }else{
                if(curIndexValuePairs.get(curIndexValuePairsSize - 1).timestamp() < curBufferPairs.get(0).timestamp()){
                    curIndexValuePairs.addAll(curBufferPairs);
                }else{
                    curIndexValuePairs = mergeIndexValuePair(curIndexValuePairs, curBufferPairs);
                    varQueryResult.put(curVarName, curIndexValuePairs);
                }
            }

            // 中文注释：更新时间区间，把不在匹配区间的结果过滤掉
            varQueryResult.put(curVarName, intervalSet.updateAndFilter(curIndexValuePairs));
        }

        // generate second interval set
        // 中文注释：经过一轮查询，时间区间会不断缩小（updateAndFilter函数完成的功能），然后我们再调用filterPairs
        // filter query rid list, we remove tree engine, so we
        List<IndexValuePair> ans = new ArrayList<>();
        for(String curVarName : varNameSet){
            List<IndexValuePair> curPairs = varQueryResult.get(curVarName);
            // merge all curRidVarIdPair, aims to sequentially access disk
            ans = mergeIndexValuePair(ans, intervalSet.updateAndFilter(curPairs));
        }
        // 中文注释：注意到ans可能包含相同的rid，因为两个变量的时间类型可能相同，之后函数我们需要再次过滤
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

    // support update/deletion [finish]
    public final List<IndexValuePair> getVarIndexValuePairsFromDisk(String eventType, List<IndependentConstraint> icList){
        // 中文注释：这里先得到每个索引快的集群一些摘要信息
        List<ClusterInfo> clusterInfoList = synopsisTable.getClusterInfo(eventType);
        return getVarIndexValuePairsFromDisk(icList, clusterInfoList);
    }

    // support update/deletion
    public final List<IndexValuePair> getVarIndexValuePairsFromDisk(List<IndependentConstraint> icList, List<ClusterInfo> overlapClusterInfoList){
        List<IndexValuePair> pairs = new ArrayList<>(1024);
        List<IndexValuePair> deletedPairs = hasDeletion ? new ArrayList<>(512) : null;

        for(ClusterInfo clusterInfo : overlapClusterInfoList){
            List<Long> maxValues = clusterInfo.maxValues();
            List<Long> minValues = clusterInfo.minValues();
            // use maximum/minimum to filter
            boolean noResults = false;

            List<ICQueryQuad> icQuads = new ArrayList<>(icList.size());
            // the attributes we need to query
            for(IndependentConstraint ic : icList){
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                // ensure attribute range can overlap
                if(maxValues.get(idx) < ic.getMinValue() || minValues.get(idx) > ic.getMaxValue()){
                    noResults = true;
                    break;
                }
                // 中文注释：存储的时候存的是集群的最大最小值,不是索引快的整体的最大最小值
                // 这里我们要把icList转换一下
                int mark = ic.hasMinMaxValue();
                long curMinValue = minValues.get(idx);
                switch (mark){
                    case 0:
                        icQuads.add(new ICQueryQuad(idx, 0, Long.MIN_VALUE, Long.MAX_VALUE));
                        break;
                    case 1:
                        icQuads.add(new ICQueryQuad(idx, 1, ic.getMinValue() - curMinValue, ic.getMaxValue()));
                        break;
                    case 2:
                        icQuads.add(new ICQueryQuad(idx, 2, ic.getMinValue(), ic.getMaxValue() - curMinValue));
                        break;
                    case 3:
                        icQuads.add(new ICQueryQuad(idx, 3, ic.getMinValue() - curMinValue, ic.getMaxValue() - curMinValue));
                }
            }

            // if current index block may have results, then query the block
            if(!noResults){
                IndexBlock indexBlock = getIndexBlock(clusterInfo.indexBlockId());
                if(hasDeletion){
                    // new version: support deletion and out-of-order insertion
                    List<List<IndexValuePair>> twoPairs = indexBlock.query4Deletion(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                    assert(twoPairs.size() == 2);
                    List<IndexValuePair> curPairs = twoPairs.get(0);
                    List<IndexValuePair> curDeletedPairs = twoPairs.get(1);
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
                }else{
                    List<IndexValuePair> curPairs = indexBlock.query(icQuads, clusterInfo.startPos(), clusterInfo.offset());
                    //IndexValuePair lastItem = pairs.get(pairs.size() - 1);
                    //IndexValuePair firstItem = curPairs.get(0);
                    //firstItem.timestamp() >= lastItem.timestamp()
                    if(orderedFlag){
                        pairs.addAll(curPairs);
                    }else{
                        // need to merge
                        pairs = mergeIndexValuePair(pairs, curPairs);
                    }
                }
            }
        }

        if(deletedPairs != null && !deletedPairs.isEmpty()){
            return deleteEventsWithSameTimestamp(pairs, deletedPairs);
        }else{
            return pairs;
        }
    }

    // this is a complex optimization, and it cannot always improve query speed
    // note that this function cannot support deletion operation
    public final Map<String, List<IndexValuePair>> getVarIndexValuePairsFromDisk(String minVarName, QueryPattern pattern, Set<Integer> accessedBlockId){
        if(hasDeletion){
            throw new RuntimeException("This function does not support scenarios with deletion operations");
        }
        Map<String, String> varTypeMap = pattern.getVarTypeMap();

        Map<String, List<ICQueryQuad>> withoutTransformQuadMap = new HashMap<>();
        Map<String, Map<Integer, int[]>> allVarRelatedBlocks = new HashMap<>();

        for(String varName : varTypeMap.keySet()){
            List<IndependentConstraint> curICList = pattern.getICListUsingVarName(varName);
            int icNum = curICList.size();
            List<ICQueryQuad> curICQuads = new ArrayList<>(icNum);
            for (IndependentConstraint ic : curICList) {
                String attrName = ic.getAttrName();
                int idx = indexAttrNameMap.get(attrName);
                int mark = ic.hasMinMaxValue();
                curICQuads.add(new ICQueryQuad(idx, mark, ic.getMinValue(), ic.getMaxValue()));
            }
            withoutTransformQuadMap.put(varName, curICQuads);
            String curType = varTypeMap.get(varName);
            allVarRelatedBlocks.put(varName, synopsisTable.getRelatedBlocks(curType, curICQuads));
        }

        String minVarType = varTypeMap.get(minVarName);
        List<ClusterInfo> minClusterInfo = synopsisTable.getClusterInfo(minVarType);
        List<ICQueryQuad> icQuads = withoutTransformQuadMap.get(minVarName);
        Map<String, List<IndexValuePair>> varQueryResult = new HashMap<>();

        // 先把它们访问过的
        // 注意 这个cliu
        for(ClusterInfo clusterInfo : minClusterInfo) {
            List<Long> maxValues = clusterInfo.maxValues();
            List<Long> minValues = clusterInfo.minValues();

            int blockId = clusterInfo.indexBlockId();
            IndexBlock indexBlock = getIndexBlock(blockId);

            // we need to convert IC quads because we use minvalues to compress original values
            List<ICQueryQuad> convertedICQuads = new ArrayList<>(icQuads.size());
            for(ICQueryQuad quad : icQuads){
                int mark = quad.mark();
                // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                switch (mark){
                    case 1:
                        convertedICQuads.add(new ICQueryQuad(quad.idx(), mark, quad.min() - minValues.get(quad.idx()), Long.MAX_VALUE));
                        break;
                    case 2:
                        convertedICQuads.add(new ICQueryQuad(quad.idx(), mark, Long.MIN_VALUE, quad.max() - maxValues.get(quad.idx())));
                        break;
                    case 3:
                        convertedICQuads.add(new ICQueryQuad(quad.idx(), mark, quad.min() - minValues.get(quad.idx()), quad.max() - maxValues.get(quad.idx())));
                        break;
                    default:
                        throw new AssertionError();
                }
            }

            List<IndexValuePair> minPairs = indexBlock.query(convertedICQuads, clusterInfo.startPos(), clusterInfo.offset());
            List<IndexValuePair> pairs = varQueryResult.get(minVarName);
            if(pairs == null){
                varQueryResult.put(minVarName, minPairs);
            }else if(!minPairs.isEmpty()){
                // 中文注释：如果大于0，我们就要判断是不是有序的 然后合并
                if(minPairs.get(0).timestamp() >= pairs.get(pairs.size() - 1).timestamp()) {
                    // 中文注释：有序直接丢后面就行
                    pairs.addAll(minPairs);
                }else{
                    // 中文注释: 乱序了需要合并
                    pairs = mergeIndexValuePair(pairs, minPairs);
                    varQueryResult.put(minVarName, pairs);
                }
            }

            // for other variables, we also query this block, but not use min/max values
            accessedBlockId.add(blockId);
            for(String varName : varTypeMap.keySet()){
                if(!varName.equals(minVarName)){
                    if(allVarRelatedBlocks.get(varName).containsKey(blockId)){
                        int[] region = allVarRelatedBlocks.get(varName).get(blockId);
                        List<ICQueryQuad> varICQuads = withoutTransformQuadMap.get(varName);
                        // we need to convert/transform ic quads
                        List<ICQueryQuad> newVarQuads = new ArrayList<>(varICQuads.size());
                        for(ICQueryQuad quad : varICQuads){
                            int mark = quad.mark();
                            // mark = 1, only have minimum value. mark = 2, only have maximum value. mark = 3, have minimum and maximum values
                            switch (mark){
                                case 1:
                                    newVarQuads.add(new ICQueryQuad(quad.idx(), mark, quad.min() - minValues.get(quad.idx()), Long.MAX_VALUE));
                                    break;
                                case 2:
                                    newVarQuads.add(new ICQueryQuad(quad.idx(), mark, Long.MIN_VALUE, quad.max() - maxValues.get(quad.idx())));
                                    break;
                                case 3:
                                    newVarQuads.add(new ICQueryQuad(quad.idx(), mark, quad.min() - minValues.get(quad.idx()), quad.max() - maxValues.get(quad.idx())));
                                    break;
                                default:
                                    throw new AssertionError();
                            }
                        }
                        List<IndexValuePair> curPairs = indexBlock.query(newVarQuads, region[0], region[1]);
                        // new version: support out-of-order insertion
                        List<IndexValuePair> varPairs = varQueryResult.get(varName);
                        if(varPairs == null){
                            varQueryResult.put(varName, curPairs);
                        }else if(!curPairs.isEmpty()){
                            if(curPairs.get(0).timestamp() >= varPairs.get(varPairs.size() - 1).timestamp()){
                                varPairs.addAll(curPairs);
                            }else{
                                varPairs = mergeIndexValuePair(varPairs, curPairs);
                                varQueryResult.put(varName, varPairs);
                            }
                        }
                    }
                }
            }
        }
        return varQueryResult;
    }

    /**
     * we do not convert query range because attribute values in buffer do not be compressed
     * @param eventType event type
     * @param icList independent predicate constraints
     * @return satisfied pairs and deleted pairs
     */
    public List<List<IndexValuePair>> getVarIndexValuePairFromBuffer(String eventType, List<IndependentConstraint> icList){
        List<ICQueryQuad> icQuads = new ArrayList<>(icList.size());
        for (IndependentConstraint ic : icList) {
            String attrName = ic.getAttrName();
            int idx = indexAttrNameMap.get(attrName);
            int mark = ic.hasMinMaxValue();
            icQuads.add(new ICQueryQuad(idx, mark, ic.getMinValue(), ic.getMaxValue()));
        }
        return bufferPool.query(orderedFlag, eventType, icQuads);
    }

    // list1 and list2 are ordered by timestamp, respectively
    public final List<IndexValuePair> mergeIndexValuePair(List<IndexValuePair> list1, List<IndexValuePair> list2){
        if(list1 == null || list2 == null){
            return list1 == null ? list2 : list1;
        }

        int size1 = list1.size();
        int size2 = list2.size();
        List<IndexValuePair> ans = new ArrayList<>(size1 + size2);
        int i = 0;
        int j = 0;

        while(i < size1 && j < size2) {
            IndexValuePair pairs1 = list1.get(i);
            IndexValuePair pairs2 = list2.get(j);
            long ts1 = pairs1.timestamp();
            long ts2 = pairs2.timestamp();

            if(ts1 < ts2){
                ans.add(pairs1);
                i++;
            }else if(ts1 > ts2){
                ans.add(pairs2);
                j++;
            }else{
                // ts1 == ts2, check rid1 & rid2
                int compare = pairs1.rid().compareTo(pairs2.rid());
                if(compare < 0){
                    ans.add(pairs1);
                    i++;
                }else if(compare > 0){
                    ans.add(pairs2);
                    j++;
                }else{
                    // same rid will remove
                    ans.add(pairs1);
                    i++;
                    j++;
                }
            }
        }

        while(i < size1){
            ans.add(list1.get(i++));
        }

        while(j < size2){
            ans.add(list2.get(j++));
        }

        return ans;
    }

    /**
     * here we need to remove events' timestamp within timestamps of deletedPairs
     * @param originalPairs - original satisfied pairs
     * @param deletedPairs -  pairs need to delete
     * @return originalPairs - deletedPairs
     */
    List<IndexValuePair> deleteEventsWithSameTimestamp(List<IndexValuePair> originalPairs, List<IndexValuePair> deletedPairs){
        if(originalPairs == null || originalPairs.isEmpty()){
            return originalPairs;
        }
        Set<Long> deletedTimestamps = new HashSet<>(deletedPairs.size() << 1);
        for(IndexValuePair pair : deletedPairs){
            deletedTimestamps.add(pair.timestamp());
        }
        List<IndexValuePair> ans = new ArrayList<>(originalPairs.size() - deletedPairs.size());
        for(IndexValuePair pair : originalPairs){
            if(!deletedTimestamps.contains(pair.timestamp())){
                ans.add(pair);
            }
        }
        return ans;
    }

    @Override
    public void print() {
        System.out.println("orderedFlag: " + orderedFlag);
        System.out.println("hasDeletion: " + hasDeletion);
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
}