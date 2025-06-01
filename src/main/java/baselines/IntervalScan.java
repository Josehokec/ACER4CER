package baselines;

import arrival.JsonMap;
import automaton.MatchStrategy;
import automaton.NFA;
import automaton.Tuple;
import btree.BPlusTree4Int;
import btree.BPlusTree4Long;
import btree.BPlusTreeInterface;
import btree.PrimaryIndex4Time;
import common.*;
import condition.IndependentConstraint;
import acer.SortedIntervalSet;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.io.File;
import java.util.*;

/**
 * Before running this method,
 * you need to run CostArgument.java to estimate constant argument
 * --> This method is come from
 * Index accelerated pattern matching in event stores, In SIGMOD'21
 * We do not use the source code from paper since it use LSM-Tree as primary index
 * LSM-Tree has a slow query performance compared with B+Tree
 * In the following implementation, we unify use b+ tree as primary index and secondary index
 */
public class IntervalScan extends baselines.Index {
    private double sumArrival;                              // arrival rate of all events
    private PrimaryIndex4Time primaryIndex;                 // key is timestamp, value is <timestamp, rid>
    private List<BPlusTreeInterface> secondaryIndexes;      // key is index attribute value, value is <timestamp, rid>

    public IntervalScan(String indexName){
        super(indexName);
    }

    @Override
    public void initial() {
        // Initializing the secondary index assumes that there are d attributes in the index,
        // and a total of d+1 indices need to be constructed because the attribute type needs to be constructed
        secondaryIndexes = new ArrayList<>(indexAttrNum + 1);
        for(int i = 0; i < indexAttrNum; ++i){
            // your code is here...
            // bPlusTrees.add(new BPlusTree4Int("attribute_" + i, schema.getSchemaName()));
            // bPlusTrees.add(new BPlusTree4Long("attribute_" + i, schema.getSchemaName()));
        }
        secondaryIndexes.add(new BPlusTree4Int("attribute_type", schema.getSchemaName()));
        // Initialize primary index
        primaryIndex = new PrimaryIndex4Time("timestamp", schema.getSchemaName());

        // Initialize Reservoir
        reservoir = new ReservoirSampling(indexAttrNum);

        // here we need to obtain event arrival rate
        String schemaName = schema.getSchemaName();
        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "arrival" + File.separator + schemaName + "_arrivals.json";
        arrivals = JsonMap.jsonToArrivalMap(jsonFilePath);

        sumArrival = 0;
        for(double arrival : arrivals.values()){
            sumArrival += arrival;
        }
    }

    @Override
    public boolean insertRecord(String record, boolean updatedFlag) {
        // your code is here...
        return true;
    }

    @Override
    public boolean insertBatchRecord(List<String[]> batchRecords, boolean updatedFlag) {
        // your code is here...
        return true;
    }

    @Override
    // [updated]
    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        List<byte[]> events = obtainEventsBasedQuery(pattern);

        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, strategy);
        }
        int ans = nfa.countTuple();
        long matchEndTime = System.nanoTime();

        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");

        return ans;
    }

    @Override
    // [updated]
    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        List<byte[]> events = obtainEventsBasedQuery(pattern);

        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, strategy);
        }
        List<Tuple> tuples = nfa.getTuple(schema);
        long matchEndTime = System.nanoTime();

        String output = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + output + "ms");

        return tuples;
    }

    @Override
    public void print() {
        System.out.println("----------------Index Information----------------");
        System.out.println("Index name: '" + getIndexName() + "'" + " schema name: '" + schema.getSchemaName() + "'");
        System.out.print("Index attribute name:" );
        for(String indexAttrName : getIndexAttrNames()){
            System.out.print(" '" + indexAttrName + "'");
        }
        System.out.println("\nrecordNum: " + autoIndices);
    }

    public final List<long[]> seqJoinTimePair(List<IndexValuePair> pairs1, List<IndexValuePair> pairs2, boolean containHead, long tau){
        // the reason use containHead is to ensure insert interval start timestamp is  monotonically increasing
        List<long[]> ans = new ArrayList<>(128);

        int size1 = pairs1.size();
        int size2 = pairs2.size();

        if(containHead){
            // triples1 joins triples2
            int startPos2 = 0;
            for(IndexValuePair triple : pairs1){
                for(int j = startPos2; j < size2; ++j){
                    long leftTime = triple.timestamp();
                    long rightTime = pairs2.get(j).timestamp();
                    if(leftTime > rightTime){
                        startPos2++;
                    }else if(rightTime - leftTime > tau){
                        break;
                    }else{
                        long[] timePair = {leftTime, rightTime};
                        ans.add(timePair);
                    }
                }
            }
        }else{
            // triples2 joins triples1
            int startPos1 = 0;
            for(IndexValuePair triple : pairs2){
                for(int i = startPos1; i < size1; ++i){
                    long leftTime = pairs1.get(i).timestamp();
                    long rightTime = triple.timestamp();
                    if(leftTime > rightTime){
                        break;
                    }else if(rightTime - leftTime > tau){
                        startPos1++;
                    }else{
                        long[] timePair = {leftTime, rightTime};
                        ans.add(timePair);
                    }
                }
            }
        }

        return ans;
    }

    /**
     * [updated] this function is used by processCountQueryUsingNFA and processTupleQueryUsingNFA
     * note that naive index can support OR operator
     * interval scan has a bit complex to process OR operator
     * due to OR operator can split multiple queries to process
     * here we assume that the processed pattern without OR operator
     * @param pattern query pattern
     * @return event list
     */
    public List<byte[]> obtainEventsBasedQuery(QueryPattern pattern){
        List<byte[]> events = new ArrayList<>(1024);

        //List<byte[]> events = obtainEventsBasedRIDList(...);
        return events;
    }

    public List<IndexValuePair> getPairsBasedSingleVar(List<SelectivityTriple> selectivityList, QueryPattern pattern){
        // your code is here...
        List<IndexValuePair> indexValuePairs = null;

        return indexValuePairs;
    }

    public SortedIntervalSet getIntervalsBasedSingleVar(List<SelectivityTriple> selectivityList, QueryPattern pattern){
        SortedIntervalSet intervals = new SortedIntervalSet();
        // your code is here...
        return intervals;
    }

    /**
     * [updated] read primary index to obtain rids
     * @param replayIntervals   sorted interval set
     * @return                  all rids within the sorted interval set
     */
    public List<RID> getRIDListBasedInterval(SortedIntervalSet replayIntervals){
        List<RID> ridList = new ArrayList<>(1024);

        List<long[]> replayIntervalList = replayIntervals.getAllReplayIntervals();
        for (long[] curInterval : replayIntervalList) {
            List<RID> curRIDList = primaryIndex.rangeQuery(curInterval[0], curInterval[1]);
            // sort rids
            sortRIDList(curRIDList);
            ridList.addAll(curRIDList);
        }

        return ridList;
    }

    /**
     * [updated] we need filter again
     * besides, when enable deletion operation, we need to delete same key
     * @param pattern       query pattern
     * @param events        events within multiple time ranges
     * @return              filtered events
     */
    public List<byte[]> filterAgain(QueryPattern pattern, List<byte[]> events){
        List<byte[]> filteredEvents = new ArrayList<>();
        // your code is here...
        return filteredEvents;
    }

    /**
     * [updated] sort rid
     * @param ridList           rid list
     */
    public static void sortRIDList(List<RID> ridList){
        ridList.sort((rid1, rid2) ->{
            if(rid1.page() == rid2.page()){
                return rid1.offset() - rid2.offset();
            }else{
                return rid1.page() - rid2.page();
            }
        });
    }

    /**
     * [updated] obtain events based on rids
     * @param ridList   rids
     * @param store     event store
     * @return          events
     */
    public static List<byte[]> obtainEventsBasedRIDList(List<RID> ridList, EventStore store){
        List<byte[]> events = new ArrayList<>(ridList.size());
        for(RID rid : ridList){
            byte[] event = store.readByteRecord(rid);
            events.add(event);
        }
        return events;
    }
}