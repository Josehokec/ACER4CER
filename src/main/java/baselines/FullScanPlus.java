package baselines;

import arrival.JsonMap;
import automaton.MatchStrategy;
import automaton.NFA;
import automaton.Tuple;
import common.*;

import condition.IndependentConstraint;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.io.*;
import java.util.*;

/**
 * fullscan-based method
 * please see [VLDB'23] high performance row pattern recognition using join
 * notice: each variable has many bucket, the bucketId = timestamp / queryWindow
 * here we use all variables to filter
 * [new] FullScanPlus supports update operation
 */
public class FullScanPlus {
    private boolean inOrder = true;
    private long previousTs = -1;
    private boolean hasUpdate = false;                      // logic update
    private int eventIndices = 0;                           // count event number
    private final EventSchema schema;                       // event schema
    private final int batchSize = 4 * 1024;                 // batch size

    private final Map<String, Integer> typeCountMap;        // calculate arrival rate
    private long startTime = -1;
    private long endTime = -1;

    // define a struct variable TsIdxPair, i.e., timestamp + index
    private record TsIdxPair(long ts, int ptr) implements Comparable<TsIdxPair>{
        @Override
        public int compareTo(TsIdxPair other) {
            if(this.ts() < other.ts()) {
                return -1;
            }else if(this.ts() > other.ts()) {
                return 1;
            }
            return 0;
        }
    }

    public FullScanPlus(EventSchema schema){
        this.schema = schema;
        typeCountMap = new HashMap<>();
    }

    public void insertOrDeleteRecord(String record, boolean updatedFlag){
        hasUpdate = updatedFlag || hasUpdate;
        String[] splits = record.split(",");

        // count type, here we will generate arrival rate json file
        int typeIdx = schema.getTypeIdx();
        String curType = splits[typeIdx];
        typeCountMap.put(curType, typeCountMap.getOrDefault(curType,0) + 1);
        int tsIdx = schema.getTimestampIdx();
        long ts = Long.parseLong(splits[tsIdx]);
        if(startTime == -1){
            startTime = ts;
        }

        if(inOrder && ts >= previousTs){
            previousTs = ts;
        }else{
            inOrder = false;
        }

        endTime = ts;

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        store.insertByteRecord(bytesRecord);
        eventIndices++;
    }

    public void insertBatchRecord(List<String[]> batchRecords, boolean updatedFlag, boolean inOrder){
        this.inOrder = inOrder;
        hasUpdate = updatedFlag || hasUpdate;
        EventStore store = schema.getStore();
        for(String[] splits : batchRecords){
            byte[] bytesRecord = schema.convertToBytes(splits);
            store.insertByteRecord(bytesRecord);
            eventIndices++;
        }
    }

    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa){
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);

        List<byte[]> filteredEvents = getEventsByPredicates(pattern);

        long matchStartTime = System.nanoTime();
        for(byte[] event : filteredEvents){
            nfa.consume(schema, event, strategy);
        }
        int ans = nfa.countTuple();
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
    }

    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa){
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);

        List<byte[]> filteredEvents = getEventsByPredicates(pattern);

        long matchStartTime = System.nanoTime();
        for(byte[] event : filteredEvents){
            nfa.consume(schema, event, strategy);
        }
        List<Tuple> ans = nfa.getTuple(schema);
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
    }

    /**
     * [updated] this function is used for supporting deletion operation and out-of-order insertion
     * @param pattern       query pattern
     * @return              filtered events
     */
    public List<byte[]> getEventsByPredicates(QueryPattern pattern){
        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        int eventPtr = 0;

        // when enable deletion, we need to use typePairMap to delete old values
        Map<String, List<TsIdxPair>> varIndexListMap = new HashMap<>();

        // read entire dataset
        int curPage = 0;
        short curOffset = 0;
        short recordSize = schema.getFixedRecordSize();
        EventStore store = schema.getStore();
        int pageSize = store.getPageSize();
        int typeIdx = schema.getTypeIdx();

        long scanCost = 0;
        long filterCost = 0;

        // --> first round filtering
        List<byte[]> oneRoundFilteredEvents = new ArrayList<>(1024);
        // the reason why we read event in batch is to avoid out-of-memory exception
        for(int i = 0; i < eventIndices; i = i + batchSize){
            // scan pages...
            long curScanStart = System.nanoTime();

            // keep original events
            long maxSize = Math.min(eventIndices - i, batchSize);
            List<byte[]> originalEvents = new ArrayList<>(batchSize);

            for(int batchOffset = 0; batchOffset < maxSize; batchOffset++){
                if (curOffset + recordSize > pageSize) {
                    curPage++;
                    curOffset = 0;
                }
                RID rid = new RID(curPage, curOffset);
                curOffset += recordSize;
                byte[] event = store.readByteRecord(rid);
                originalEvents.add(event);
            }
            long curScanEnd = System.nanoTime();
            scanCost += (curScanEnd - curScanStart);

            // filter events based on predicate conditions
            long curFilterStart = System.nanoTime();
            for(byte[] event : originalEvents){
                String curType = schema.getTypeFromBytesRecord(event, typeIdx);
                boolean appendFlag = false;

                // we first check ic and type conditions
                for(Map.Entry<String, String> entry : varTypeMap.entrySet()){
                    String varName= entry.getKey();
                    String type = entry.getValue();
                    if(curType.equals(type)){
                        // check independent constraints
                        List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                        boolean satisfyAllIC = true;
                        if(icList != null){
                            for (IndependentConstraint ic : icList) {
                                String name = ic.getAttrName();
                                long min = ic.getMinValue();
                                long max = ic.getMaxValue();
                                // obtain the corresponding column for storage based on the attribute name
                                int col = schema.getAttrNameIdx(name);
                                long value = schema.getValueFromBytesRecord(event, col);
                                if (value < min || value > max) {
                                    satisfyAllIC = false;
                                    break;
                                }
                            }
                        }
                        if(satisfyAllIC){
                            appendFlag = true;
                            long ts = schema.getTimestampFromRecord(event);
                            varIndexListMap.computeIfAbsent(varName, k -> new ArrayList<>()).add(new TsIdxPair(ts, eventPtr));
                        }
                    }
                }

                if(appendFlag){
                    oneRoundFilteredEvents.add(event);
                    eventPtr++;
                }
            }
            long curFilterEnd = System.nanoTime();
            filterCost += (curFilterEnd - curFilterStart);
        }

        long filterStartTime = System.nanoTime();
        // --> second round filtering
        long windowSize = pattern.getTau();
        // we first get all bucket ids
        Set<Long> bucketIdSet = null;
        for(String varName : varTypeMap.keySet()){
            List<TsIdxPair> oldTsIdxPairs = varIndexListMap.getOrDefault(varName, new ArrayList<>());
            oldTsIdxPairs.sort(TsIdxPair::compareTo);

            // we need to remove events with old timestamp
            if(hasUpdate){
                int curSize = oldTsIdxPairs.size();
                List<TsIdxPair> newTsIdxPairs = new ArrayList<>(curSize * 2 /3);
                TsIdxPair previousTsIdxPair = oldTsIdxPairs.get(0);
                for(TsIdxPair curPair : oldTsIdxPairs){
                    if(previousTsIdxPair.ts() != curPair.ts()){
                        newTsIdxPairs.add(previousTsIdxPair);
                    }
                    // update pair
                    previousTsIdxPair = curPair;
                }
                // add the last item
                newTsIdxPairs.add(previousTsIdxPair);
                varIndexListMap.put(varName, newTsIdxPairs);
                oldTsIdxPairs = newTsIdxPairs;
            }

            // generate bucket ids...
            Set<Long> curBucketIdSet = new HashSet<>(1024);

            for(TsIdxPair curPair : oldTsIdxPairs){
                long bucketId = curPair.ts() / windowSize;
                if(pattern.isOnlyLeftMostNode(varName)){
                    curBucketIdSet.add(bucketId);
                    curBucketIdSet.add(bucketId + 1);
                }else if(pattern.isOnlyRightMostNode(varName)){
                    curBucketIdSet.add(bucketId - 1);
                    curBucketIdSet.add(bucketId);
                }else{
                    curBucketIdSet.add(bucketId - 1);
                    curBucketIdSet.add(bucketId);
                    curBucketIdSet.add(bucketId + 1);
                }
            }

            if(bucketIdSet != null){
                bucketIdSet.retainAll(curBucketIdSet);
            }else{
                bucketIdSet = curBucketIdSet;
            }
        }

        // then filter events
        List<TsIdxPair> mergedTsIdxPairs = new ArrayList<>();
        for(String varName : varTypeMap.keySet()){
            List<TsIdxPair> oldTsIdxPairs = varIndexListMap.getOrDefault(varName, new ArrayList<>());
            List<TsIdxPair> newTsIdxPairs = new ArrayList<>(oldTsIdxPairs.size() * 2 /3 + 1);
            for(TsIdxPair curPair : oldTsIdxPairs){
                long bucketId = curPair.ts() / windowSize;
                if(bucketIdSet.contains(bucketId)){
                    newTsIdxPairs.add(curPair);
                }
            }
            // then call merge function
            mergedTsIdxPairs = mergePairs(mergedTsIdxPairs, newTsIdxPairs);
        }

        List<byte[]> filteredEvents = new ArrayList<>(mergedTsIdxPairs.size());
        for(TsIdxPair curPair : mergedTsIdxPairs){
            int ptr = curPair.ptr;
            filteredEvents.add(oneRoundFilteredEvents.get(ptr));
        }

        if(!inOrder){
            filteredEvents.sort((o1, o2) -> {
                long ts1 = schema.getTimestampFromRecord(o1);
                long ts2 = schema.getTimestampFromRecord(o2);
                if(ts1 < ts2){
                    return -1;
                }else if(ts2 < ts1){
                    return 1;
                }
                return 0;
            });
        }

        long filterEndTime = System.nanoTime();
        filterCost += (filterEndTime - filterStartTime);

        String scanOutput = String.format("%.3f", (scanCost + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        String filterOutput = String.format("%.3f", (filterCost + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        return filteredEvents;
    }

    /**
     * [updated] merge function, remove redundancy
     * @param pairs1 first ordered index value pairs
     * @param pairs2 second ordered index value pairs
     * @return merged index value pairs
     */
    private List<TsIdxPair> mergePairs(List<TsIdxPair> pairs1, List<TsIdxPair> pairs2){
        if(pairs1.isEmpty()){
            return pairs2;
        }

        int size1 = pairs1.size();
        int size2 = pairs2.size();

        List<TsIdxPair> mergedPairs = new ArrayList<>(size1 + size2);
        int idx1 = 0;
        int idx2 = 0;

        while(idx1 < size1 && idx2 < size2) {
            long ts1 = pairs1.get(idx1).ts();
            long ts2 = pairs2.get(idx2).ts();
            if(ts1 < ts2){
                mergedPairs.add(pairs1.get(idx1++));
            }else if(ts1 > ts2){
                mergedPairs.add(pairs2.get(idx2++));
            }else{
                int ptr1 = pairs1.get(idx1).ptr();
                int ptr2 = pairs2.get(idx2).ptr();

                if(ptr1 < ptr2){
                    mergedPairs.add(pairs1.get(idx1++));
                }else if(ptr1 > ptr2){
                    mergedPairs.add(pairs2.get(idx2++));
                }else{
                    mergedPairs.add(pairs1.get(idx1++));
                    idx2++;
                }
            }
        }
        while(idx1 < size1){
            mergedPairs.add(pairs1.get(idx1++));
        }

        while(idx2 < size2){
            mergedPairs.add(pairs2.get(idx2++));
        }
        return mergedPairs;
    }

    public void updateArrivalJson(){
        // store the arrivals to json file
        HashMap<String, Double> arrivals = new HashMap<>(typeCountMap.size() * 2);

        long span = endTime - startTime;
        for (Map.Entry<String, Integer> entry : typeCountMap.entrySet()) {
            String key = entry.getKey();
            double value = ((double) entry.getValue()) / span;
            arrivals.put(key, value);
        }
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "arrival" + File.separator + schemaName + "_arrivals.json";
        JsonMap.arrivalMapToJson(arrivals, jsonFilePath);
    }
}

/*
removed code lines:
------------------------------------------------------------------------------------------------
    // if events arrival in-order, you can call this function as it has a more low latency
    public List<byte[]> getAllFilteredEvents(QueryPattern pattern){
        List<byte[]> filteredEvents = new ArrayList<>();

        // read entire dataset
        int curPage = 0;
        short curOffset = 0;
        short recordSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();
        int pageSize = store.getPageSize();
        int typeIdx = schema.getTypeIdx();

        long scanCost = 0;
        long scanFilterStartTime = System.nanoTime();
        // variableName, eventType
        Map<String, String> varTypeMap = pattern.getVarTypeMap();

        // we know timestamp is ordered, then we can check timestamp
        // varName, windowSet
        HashMap<String, HashSet<Long>> varBuckets = new HashMap<>();
        int timeIdx = schema.getTimestampIdx();

        for(int i = 0; i < eventIndices; ++i) {
            if (curOffset + recordSize > pageSize) {
                curPage++;
                curOffset = 0;
            }
            RID rid = new RID(curPage, curOffset);
            curOffset += recordSize;
            // read record from store
            long scanStartTime = System.nanoTime();
            byte[] event = store.readByteRecord(rid);
            long scanEndTime = System.nanoTime();
            scanCost += (scanEndTime - scanStartTime);

            String curType = schema.getTypeFromBytesRecord(event, typeIdx);
            // we first check ic and type then consume
            // only if satisfy a variable, we add it to filterEvents
            boolean canAdd = false;
            for(Map.Entry<String, String> entry : varTypeMap.entrySet()){
                String varName= entry.getKey();
                String type = entry.getValue();
                if(curType.equals(type)){
                    // check independent constraints
                    List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                    boolean satisfyAllIC = true;
                    if(icList != null){
                        for (IndependentConstraint ic : icList) {
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // obtain the corresponding column for storage based on the attribute name
                            int col = schema.getAttrNameIdx(name);
                            long value = schema.getValueFromBytesRecord(event, col);
                            if (value < min || value > max) {
                                satisfyAllIC = false;
                                break;
                            }
                        }
                    }
                    if(satisfyAllIC){
                        canAdd = true;
                        long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(event, timeIdx));
                        long bucketId = timestamp / pattern.getTau();

                        if(varBuckets.get(varName) == null){
                            HashSet<Long> set = new HashSet<>();

                            if(pattern.isOnlyLeftMostNode(varName)){
                                set.add(bucketId);
                                set.add(bucketId + 1);
                                varBuckets.put(varName, set);
                            }else if(pattern.isOnlyRightMostNode(varName)){
                                set.add(bucketId - 1);
                                set.add(bucketId);
                                varBuckets.put(varName, set);
                            }else{
                                set.add(bucketId - 1);
                                set.add(bucketId);
                                set.add(bucketId + 1);
                                varBuckets.put(varName, set);
                            }
                        }else{
                            if(pattern.isOnlyLeftMostNode(varName)){
                                varBuckets.get(varName).add(bucketId);
                                varBuckets.get(varName).add(bucketId + 1);

                            }else if(pattern.isOnlyRightMostNode(varName)){
                                varBuckets.get(varName).add(bucketId - 1);
                                varBuckets.get(varName).add(bucketId);
                            }else{
                                varBuckets.get(varName).add(bucketId - 1);
                                varBuckets.get(varName).add(bucketId);
                                varBuckets.get(varName).add(bucketId + 1);
                            }
                        }
                        // break;
                    }
                }
            }
            if(canAdd){
                filteredEvents.add(event);
            }
        }

        // use matched window to filter unrelated events
        // find the variable with minimum events
        String minVar = null;
        int minValue = Integer.MAX_VALUE;
        HashSet<Long> matchedBucketIds = null;
        for(Map.Entry<String, HashSet<Long>> entry : varBuckets.entrySet()){
            String varName= entry.getKey();
            HashSet<Long> curBuckets = entry.getValue();
            int size = curBuckets.size();
            if(size < minValue){
                minVar = varName;
                minValue = size;
                matchedBucketIds = curBuckets;
            }
        }

        // delete the window that cannot contain matched tuples
        for(Map.Entry<String, HashSet<Long>> entry : varBuckets.entrySet()){
            String varName= entry.getKey();

            if(!varName.equals(minVar)){
                HashSet<Long> updatedBucketIdSet = new HashSet<>();
                HashSet<Long> curBuckets = entry.getValue();
                for(long bucketId : curBuckets){
                    if(matchedBucketIds != null && matchedBucketIds.contains(bucketId)){
                        updatedBucketIdSet.add(bucketId);
                    }
                }
                // update the bucketId set
                matchedBucketIds = updatedBucketIdSet;
            }
        }

        // if events do not exist in matchedBucketIds, then we can remove it
        List<byte[]> ans = new ArrayList<>(filteredEvents.size());
        for(byte[] event : filteredEvents){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(event, timeIdx));
            long bucketId = timestamp / pattern.getTau();
            if(matchedBucketIds != null && matchedBucketIds.contains(bucketId)){
                ans.add(event);
            }
        }

        long scanFilterEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanCost + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        String filterOutput = String.format("%.3f", (scanFilterEndTime - scanFilterStartTime - scanCost + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        return ans;
    }
 */
