package method;

import automaton.NFA;
import common.EventPattern;
import common.EventSchema;
import common.ReservoirSampling;
import join.AbstractJoinEngine;
import join.Tuple;
import pattern.QueryPattern;

import java.util.HashMap;
import java.util.List;

/**
 * indexNUm         -> number of index
 * indexName        -> name of index
 * schema           -> table schema
 * reservoir        -> sampling
 * indexAttrNameMap -> attribute storage map
 *
 */
public abstract class Index {
    public int indexAttrNum;
    public int autoIndices;                             // number of events
    private String indexName;                           // name of index
    public EventSchema schema;                          // table schema
    public HashMap<String, Double> arrivals;            // event type arrival
    public ReservoirSampling reservoir;                 // attribute sampling
    public HashMap<String, Integer> indexAttrNameMap;   // index attribute name

    public Index(String indexName){
        this.indexName = indexName;
        indexAttrNum = 0;
        autoIndices = 0;
        indexAttrNameMap = new HashMap<>();
    }

    public String getIndexName() {
        return indexName;
    }

    public EventSchema getSchema() {
        return schema;
    }

    public void setSchema(EventSchema schema) {
        this.schema = schema;
    }

    public void addIndexAttrNameMap(String attrName){
        indexAttrNameMap.put(attrName, indexAttrNum++);
    }

    public int getIndexNameId(String attrName){
        return indexAttrNameMap.get(attrName);
    }

    /**
     * get all attribute name
     * @return index name array
     */
    public String[] getIndexAttrNames(){
        String[] indexNames = new String[indexAttrNum];
        indexAttrNameMap.forEach((k, v) -> indexNames[v] = k);
        return indexNames;
    }

    public abstract void initial();
    public abstract boolean insertRecord(String record);

    public abstract boolean insertBatchRecord(String[] record);

    // this function only used for sequential pattern
    public abstract int processCountQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join);

    // this function only used for sequential pattern
    public abstract List<Tuple> processTupleQueryUsingJoin(EventPattern pattern, AbstractJoinEngine join);

    public abstract int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract void print();

    // new version: support deletion

    /**
     * we require the record really exists
     * otherwise, it will have a serious bug!!
     * @param record - record
     */
    public abstract void delete(String record);
}
