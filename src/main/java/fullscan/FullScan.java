package fullscan;

import arrival.JsonMap;
import automaton.NFA;
import common.EventPattern;
import common.EventSchema;
import common.MatchStrategy;
import condition.IndependentConstraint;
import join.AbstractJoinEngine;
import join.Tuple;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * reading records from the byte file is faster than reading data from strings file.
 * Step 1: read file from string file
 * Step 2: then we store the record into binary file
 * note that FullScan only uses independent predicate constraints to filter
 */
public class FullScan {
    private int eventIndices;
    private final EventSchema schema;

    private Map<String, Integer> typeCountMap;
    private long startTime = -1;
    private long endTime = -1;

    private boolean updateArrival;

    public FullScan(EventSchema schema) {
        eventIndices = 0;
        this.schema = schema;
        typeCountMap = new HashMap<>();
        updateArrival = true;
    }

    public boolean insertRecord(String record){
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
        endTime = ts;
        updateArrival = true;

        byte[] bytesRecord = schema.convertToBytes(splits);
        EventStore store = schema.getStore();
        store.insertByteRecord(bytesRecord);
        eventIndices++;

        return true;
    }

    public int getRecordIndices(){
        return eventIndices;
    }

    public int processCountQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join){
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        List<List<byte[]>> buckets = getRecordUsingIC(pattern);

        int ans;
        long matchStartTime = System.nanoTime();
        switch (strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.countTupleUsingFollowBy(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.countTupleUsingFollowByAny(pattern, buckets);
            default -> {
                System.out.println("do not support this strategy, default is SKIP_TILL_ANY_MATCH");
                ans = join.countTupleUsingFollowBy(pattern, buckets);
            }
        }
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
    }

    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa){
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        // read entire dataset
        int curPage = 0;
        short curOffset = 0;
        short recordSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();
        int pageSize = store.getPageSize();
        int typeIdx = schema.getTypeIdx();

        long scanCost = 0;
        long scanFilterStartTime = System.nanoTime();

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        List<byte[]> filteredEvents = new ArrayList<>();

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
                        break;
                    }
                }
            }
            if(canAdd){
                filteredEvents.add(event);
            }
        }

        long scanFilterEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanCost + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        String filterOutput = String.format("%.3f", (scanFilterEndTime - scanFilterStartTime - scanCost + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

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

    public List<Tuple> processTupleQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join){
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        List<List<byte[]>> buckets = getRecordUsingIC(pattern);
        List<Tuple> ans;
        long matchStartTime = System.nanoTime();
        switch (strategy){
            case SKIP_TILL_NEXT_MATCH -> ans = join.getTupleUsingFollowBy(pattern, buckets);
            case SKIP_TILL_ANY_MATCH -> ans = join.getTupleUsingFollowByAny(pattern, buckets);
            default -> {
                System.out.println("do not support this strategy, default is SKIP_TILL_ANY_MATCH");
                ans = join.getTupleUsingFollowBy(pattern, buckets);
            }
        }
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
    }

    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa){
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        // read entire dataset
        int curPage = 0;
        short curOffset = 0;
        short recordSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();
        int pageSize = store.getPageSize();
        int typeIdx = schema.getTypeIdx();

        long scanCost = 0;
        long scanFilterStartTime = System.nanoTime();

        Map<String, String> varTypeMap = pattern.getVarTypeMap();
        List<byte[]> filteredEvents = new ArrayList<>();

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
                        break;
                    }
                }
            }
            if(canAdd){
                filteredEvents.add(event);
            }
        }

        long scanFilterEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanCost + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        String filterOutput = String.format("%.3f", (scanFilterEndTime - scanFilterStartTime - scanCost + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

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

    public List<List<byte[]>> getRecordUsingIC(EventPattern pattern){
        String[] seqEventTypes = pattern.getSeqEventTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int patternLen = seqVarNames.length;

        List<List<byte[]>> buckets = new ArrayList<>();
        for(int i = 0; i < patternLen; ++i){
            buckets.add(new ArrayList<>());
        }

        short eventSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();
        int typeIdx = schema.getTypeIdx();
        int curPage = 0;
        short curOffset = 0;
        int pageSize = store.getPageSize();

        // access all events
        long scanCost = 0;
        long scanFilterStartTime = System.nanoTime();
        for(int i = 0; i < eventIndices; ++i){
            if(curOffset + eventSize > pageSize){
                curPage++;
                curOffset = 0;
            }
            RID rid = new RID(curPage, curOffset);
            curOffset += eventSize;
            // read event from store
            long scanStartTime = System.nanoTime();
            byte[] event = store.readByteRecord(rid);
            long scanEndTime = System.nanoTime();
            scanCost += (scanEndTime - scanStartTime);

            String curType = schema.getTypeFromBytesRecord(event, typeIdx);
            for(int j = 0; j < patternLen; ++j) {
                // Firstly, the event types must be equal.
                // Once equal, check if the predicate satisfies the constraint conditions.
                // If so, place it in the i-th bucket
                if (curType.equals(seqEventTypes[j])) {
                    // check independent constraints
                    String varName = seqVarNames[j];
                    List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);
                    boolean satisfy = true;
                    if(icList != null){
                        for (IndependentConstraint ic : icList) {
                            String name = ic.getAttrName();
                            long min = ic.getMinValue();
                            long max = ic.getMaxValue();
                            // obtain the corresponding column for storage based on the attribute name
                            int col = schema.getAttrNameIdx(name);
                            long value = schema.getValueFromBytesRecord(event, col);
                            if (value < min || value > max) {
                                satisfy = false;
                                break;
                            }
                        }
                    }
                    if (satisfy) {
                        buckets.get(j).add(event);
                    }
                }
            }

        }
        long scanFilterEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanCost + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");
        String filterOutput = String.format("%.3f", (scanFilterEndTime - scanFilterStartTime - scanCost + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        return buckets;
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
