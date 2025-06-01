package baselines;

import acer.SortedIntervalSet;
import automaton.NFA;
import btree.BPlusTree4Int;
import btree.BPlusTree4Long;

import btree.BPlusTreeInterface;
import common.IndexValuePair;
import automaton.MatchStrategy;
import common.ReservoirSampling;
import condition.IndependentConstraint;
import automaton.Tuple;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.util.ArrayList;
import java.util.List;

/**
 * this method aims to use tree indexes to query all independent predicates
 * Please read the paper to implement the following code
 * Michael Körber, Nikolaus Glombiewski, Bernhard Seeger. Index-accelerated pattern matching in event stores. In SIGMOD, 2021.
 */
public class NaiveIndex extends baselines.Index {
    // attribute value should be int or long values, otherwise we need to convert them
    // bPlusTrees have BPlusTree4Int or BPlusTree4Long
    List<BPlusTreeInterface> bPlusTrees;

    public NaiveIndex(String indexName) {
        super(indexName);
    }

    @Override
    public void initial() {
        bPlusTrees = new ArrayList<>(indexAttrNum + 1);
        reservoir = new ReservoirSampling(indexAttrNum);
        for(int i = 0; i < indexAttrNum; ++i){
            // your code is here...
            // bPlusTrees.add(new BPlusTree4Int("attribute_" + i, schema.getSchemaName()));
            // bPlusTrees.add(new BPlusTree4Long("attribute_" + i, schema.getSchemaName()));
        }
        // [optimization] maybe change to short?
        bPlusTrees.add(new BPlusTree4Int("attribute_type", schema.getSchemaName()));
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
    public int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        List<byte[]> events = obtainEventsBasedQuery(pattern);

        // match based on nfa...
        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, strategy);
        }
        int ans = nfa.countTuple();
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
    }

    @Override
    public List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa) {
        List<byte[]> events = obtainEventsBasedQuery(pattern);

        // match based on nfa...
        long matchStartTime = System.nanoTime();
        MatchStrategy strategy = pattern.getStrategy();
        nfa.generateNFAUsingQueryPattern(pattern);
        for(byte[] event : events){
            nfa.consume(schema, event, strategy);
        }
        List<Tuple> ans = nfa.getTuple(schema);
        long matchEndTime = System.nanoTime();
        String matchOutput = String.format("%.3f", (matchEndTime - matchStartTime + 0.0) / 1_000_000);
        System.out.println("match cost: " + matchOutput + "ms");

        return ans;
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

    // [updated]
    public final List<IndexValuePair> getAllPairs(String eventType, List<IndependentConstraint> icList){
        // step 1: using event type to filter
        int typeId = schema.getTypeId(eventType);
        List<IndexValuePair> ans = bPlusTrees.get(indexAttrNum).rangeQuery(typeId, typeId);
        // System.out.println("event type: " + eventType + " id:" + typeId + " cnt: " + ans.size());

        sortIndexValuePair(ans);                    // sort based on timestamp and RID
        if(icList != null){
            // step 2: using independent constraints to filter
            for (IndependentConstraint ic : icList) {
                String attrName = ic.getAttrName();
                // idx represents the storage position corresponding to the index attribute name
                int idx = -1;
                if (indexAttrNameMap.containsKey(attrName)) {
                    idx = indexAttrNameMap.get(attrName);
                } else {
                    System.out.println("This attribute do not have an index.");
                }

                int mark = ic.hasMinMaxValue();
                if (idx == -1 || mark == 0) {
                    System.out.print("this attribute does not have index.");
                }else{
                    List<IndexValuePair> temp = bPlusTrees.get(idx).rangeQuery(ic.getMinValue(), ic.getMaxValue());
                    sortIndexValuePair(temp);       // sort based on timestamp and RID
                    ans = intersect(ans, temp);
                }
            }
        }
        return ans;
    }

    /**
     * [updated] Intersection of two lists, here two lists are ordered
     * we first compare timestamp, then compare rid
     * @param list1 1-th list
     * @param list2 2-th list
     * @return Intersection of two lists
     */
    public static List<IndexValuePair> intersect(List<IndexValuePair> list1, List<IndexValuePair> list2){
        if(list1 == null || list2 == null){
            return new ArrayList<>();
        }else{
            List<IndexValuePair> ans = new ArrayList<>();
            int idx1 = 0;
            int idx2 = 0;
            // here we first compare timestamp, then compare rid
            while(idx1 < list1.size() && idx2 < list2.size()){
                long ts1 = list1.get(idx1).timestamp();
                long ts2 = list2.get(idx2).timestamp();
                if(ts1 == ts2){
                    // we need check rid
                    RID rid1 = list1.get(idx1).rid();
                    RID rid2 = list2.get(idx2).rid();
                    if(rid1.page() == rid2.page() && rid1.offset() == rid2.offset()){
                        ans.add(list1.get(idx1));
                        idx1++;
                        idx2++;
                    }else if(rid1.compareTo(rid2) < 0){
                        idx1++;
                    }else{
                        idx2++;
                    }
                }else if(ts1 < ts2){
                    idx1++;
                }else{
                    idx2++;
                }
            }
            return ans;
        }
    }

    /**
     * [updated] this function is used by processCountQueryUsingNFA and processTupleQueryUsingNFA
     * @param pattern query pattern
     * @return event list
     */
    public List<byte[]> obtainEventsBasedQuery(QueryPattern pattern){
        // updated code lines...
        int patternLen = pattern.getVarTypeMap().size();
        String[] varNames = new String[patternLen];
        String[] eventTypes = new String[patternLen];
        pattern.getAllVarType(varNames, eventTypes);

        // use index to filter...
        long filterStartTime = System.nanoTime();
        List<IndexValuePair> mergedPairs = new ArrayList<>();

        // here we use window
        SortedIntervalSet intervals = new SortedIntervalSet();

        for(int i = 0; i < patternLen; ++i){
            String varName = varNames[i];
            String eventType = eventTypes[i];
            List<IndependentConstraint> icList = pattern.getICListUsingVarName(varName);

            // given a variable, query the <timestamp, RID> pairs
            List<IndexValuePair> pairs = getAllPairs(eventType, icList);

            // given a type, enable deletion, here primary key is <type, timestamp>
            pairs = hasUpdated ? getUniqueIndexValuePair(pairs) : pairs;

            // filtering based on window condition
            if(i == 0){
                long leftOffset, rightOffset;
                long w = pattern.getTau();
                if(pattern.isOnlyLeftMostNode(varName)){
                    leftOffset = 0;
                    rightOffset = w;
                }else if(pattern.isOnlyRightMostNode(varName)){
                    leftOffset = -w;
                    rightOffset = 0;
                }else{
                    leftOffset = -w;
                    rightOffset = w;
                }
                for(IndexValuePair pair : pairs){
                    long ts = pair.timestamp();
                    intervals.insert(ts + leftOffset, ts + rightOffset);
                }
            }else{
                pairs = intervals.updateAndFilter(pairs);   // filtering pairs and update interval set
            }

            // then, we merge all pairs
            mergedPairs = mergeIndexValuePair(mergedPairs, pairs);
        }
        long filterEndTime = System.nanoTime();
        String filterOutput = String.format("%.3f", (filterEndTime - filterStartTime + 0.0) / 1_000_000);
        System.out.println("filter cost: " + filterOutput + "ms");

        // obtain events...
        long scanStartTime = System.nanoTime();
        List<byte[]> events = obtainEventsBasedPairs(mergedPairs, schema.getStore());
        long scanEndTime = System.nanoTime();
        String scanOutput = String.format("%.3f", (scanEndTime - scanStartTime + 0.0) / 1_000_000);
        System.out.println("scan cost: " + scanOutput + "ms");

        return events;
    }

    /**
     * [updated] sort function
     * @param unorderedList unordered index value pairs
     */
    public static void sortIndexValuePair(List<IndexValuePair> unorderedList){
        // unorderedList cannot be null
        unorderedList.sort((o1, o2) -> {
            long ts1 = o1.timestamp();
            RID rid1 = o1.rid();

            long ts2 = o2.timestamp();
            RID rid2 = o2.rid();

            if(ts1 == ts2){
                if(rid1.page() == rid2.page()){
                    return rid1.offset() - rid2.offset();
                }else{
                    return rid1.page() - rid2.page();
                }
            }else{
                return ts1 < ts2 ? -1 : 1;
            }
        });
        // return unorderedList;
    }

    /**
     * [updated] primary key is <type, timestamp>,
     * you must ensure pairs belong to same type, pairs are not null
     * old keys mean they are be deleted
     * @param pairs an index value list has same keys
     * @return a new list without same timestamp
     */
    public static List<IndexValuePair> getUniqueIndexValuePair(List<IndexValuePair> pairs){
        if(pairs.isEmpty()){ return pairs;}

        // expand at most once
        int estimateSize = pairs.size() / 3 * 2;
        List<IndexValuePair> uniquePairs = new ArrayList<>(estimateSize);

        IndexValuePair previousPair = pairs.get(0);
        for(IndexValuePair curPair : pairs){
            if(curPair.timestamp() != previousPair.timestamp()){
                uniquePairs.add(previousPair);
            }
            previousPair = curPair;
        }
        uniquePairs.add(previousPair);
        return uniquePairs;
    }

    /**
     * [updated] merge function, remove redundancy
     * @param pairs1 first ordered index value pairs
     * @param pairs2 second ordered index value pairs
     * @return merged index value pairs
     */
    public static List<IndexValuePair> mergeIndexValuePair(List<IndexValuePair> pairs1, List<IndexValuePair> pairs2){
        int size1 = pairs1.size();
        int size2 = pairs2.size();

        List<IndexValuePair> mergedPairs = new ArrayList<>(size1 + size2);
        int idx1 = 0;
        int idx2 = 0;

        while(idx1 < size1 && idx2 < size2) {
            long ts1 = pairs1.get(idx1).timestamp();
            long ts2 = pairs2.get(idx2).timestamp();
            if(ts1 < ts2){
                mergedPairs.add(pairs1.get(idx1++));
            }else if(ts1 > ts2){
                mergedPairs.add(pairs2.get(idx2++));
            }else{
                // two rids maybe same
                RID rid1 = pairs1.get(idx1).rid();
                RID rid2 = pairs2.get(idx2).rid();

                if(rid1.compareTo(rid2) < 0){
                    mergedPairs.add(pairs1.get(idx1++));
                }else if(rid1.compareTo(rid2) > 0){
                    mergedPairs.add(pairs2.get(idx2++));
                }else{
                    // we need to remove a same rid
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

    /**
     * [updated] obtain events based on pairs
     * @param pairs note that pairs without same rid
     * @param store event store
     * @return events
     */
    public static List<byte[]> obtainEventsBasedPairs(List<IndexValuePair> pairs, EventStore store){
        List<byte[]> events = new ArrayList<>(pairs.size());
        for(IndexValuePair pair : pairs){
            RID rid = pair.rid();
            byte[] singleEvent = store.readByteRecord(rid);
            events.add(singleEvent);
        }
        return events;
    }
}