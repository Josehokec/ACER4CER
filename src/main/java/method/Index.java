package method;

import automaton.NFA;
import common.EventSchema;
import common.ReservoirSampling;
import common.Tuple;
import pattern.QueryPattern;

import java.util.HashMap;
import java.util.List;

public abstract class Index {
    public int indexAttrNum;
    public int autoIndices;                             // number of events
    private final String indexName;                     // name of index
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

    public abstract boolean insertOrDeleteRecord(String record, boolean deleteFlag);

    public abstract boolean insertBatchRecord(String[] record);

    public abstract int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract void print();

}
