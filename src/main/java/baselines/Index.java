package baselines;

import automaton.NFA;
import common.EventSchema;
import common.ReservoirSampling;
import automaton.Tuple;
import pattern.QueryPattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class Index {
    protected boolean hasUpdated = false;               // support update [logic update]
    public int indexAttrNum;                            // number of indexes attributes
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

    public abstract boolean insertRecord(String record, boolean updatedFlag);

    public abstract boolean insertBatchRecord(List<String[]> batchRecords, boolean updatedFlag);

    public abstract int processCountQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract List<Tuple> processTupleQueryUsingNFA(QueryPattern pattern, NFA nfa);

    public abstract void print();

    /**
     * this function aims to delete events with null value
     * @param events        events
     * @return              filtered events
     */
    public List<byte[]> deleteNullEvents(List<byte[]> events){
        // you should ensure storage format is: type_id, attribute_1, ..., attribute_N, timestamp
        // 4, xxx ,8
        List<byte[]> filteredEvents = new ArrayList<>();

        for(byte[] event : events){
            boolean canDelete = true;
            for(int i = 4; i < event.length - 8; i++){
                if(event[i] != 0){
                    canDelete = false;
                    break;
                }
            }
            if(!canDelete){
                filteredEvents.add(event);
            }
        }
        return filteredEvents;
    }
}
