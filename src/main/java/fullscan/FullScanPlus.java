package fullscan;

import arrival.JsonMap;
import automaton.NFA;
import common.Converter;

import common.EventSchema;
import common.MatchStrategy;
import common.Tuple;
import condition.IndependentConstraint;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.io.File;
import java.util.*;

/**
 * We noticed a mistake in the experimental section of our paper:
 *     the running result was for the FullScan method instead of FullScanPlus
 * -------------------------------
 * fullscan-based method
 * vldb'23-high performance row pattern recognition using join
 * in our view, this algorithm is very similar to KDD'12
 * notice: each variable has many bucket, the bucketId = timestamp / queryWindow
 * due to this method is fullscan-based methods, we use all variables to filter
 */
public class FullScanPlus {
    private int eventIndices;
    private final EventSchema schema;

    private Map<String, Integer> typeCountMap;
    private long startTime = -1;
    private long endTime = -1;

    private boolean updateArrival;

    public FullScanPlus(EventSchema schema){
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

    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa){
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);

        List<byte[]> filteredEvents = getAllFilteredEvents(pattern);

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
        // first generate arrival rate json file
        if(updateArrival){
            updateArrivalJson();
            updateArrival = false;
        }

        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);

        List<byte[]> filteredEvents = getAllFilteredEvents(pattern);

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
