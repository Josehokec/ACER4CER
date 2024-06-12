package acer;

import java.util.ArrayList;
import java.util.List;

public class SingleBuffer {
    private int indexAttrNum;                       // number of index attributes
    private final int partitionRecordNum;           // optimal record number store in a partition
    private List<Long> minValues;                   // Attribute synopsis -> minimum values
    private List<Long> maxValues;                   // Attribute synopsis -> maximum values
    private final List<ACERTemporaryTriple> triples;  // Temporary Triple

    public SingleBuffer(int indexAttrNum, int partitionRecordNum){
        this.indexAttrNum = indexAttrNum;
        this.partitionRecordNum = partitionRecordNum;
        minValues = new ArrayList<>(indexAttrNum);
        maxValues = new ArrayList<>(indexAttrNum);
        triples = new ArrayList<>(partitionRecordNum);

        for(int i = 0; i < indexAttrNum; ++i){
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
        }
    }

    public List<Long> getMinValues() {
        List<Long> ans = new ArrayList<>(indexAttrNum);
        ans.addAll(minValues);
        return ans;
    }

    public List<Long> getMaxValues() {
        List<Long> ans = new ArrayList<>(indexAttrNum);
        ans.addAll(maxValues);
        return ans;
    }

    public List<ACERTemporaryTriple> getTriples() {
        return triples;
    }

    /**
     * if buffer full, then return true
     * else return false;
     * @param triple      quad
     * @return          full -> true, otherwise -> false
     */
    public boolean append(ACERTemporaryTriple triple){
        triples.add(triple);
        final long[] attrValues = triple.attrValues();
        final int indexAttrNum = attrValues.length;

        // update minimum and maximum values
        for(int i = 0; i < indexAttrNum; ++i){
            long v = attrValues[i];
            if(minValues.get(i) > v){
                minValues.set(i, v);
            }
            if(maxValues.get(i) < v){
                maxValues.set(i, v);
            }
        }

        return triples.size() == partitionRecordNum;
    }

    /**
     * clear buffer
     * set min values and maxValues, clear the list
     * @return true
     */
    public boolean clearBuffer(){
        final int indexAttrNum = minValues.size();
        triples.clear();
        minValues.clear();
        maxValues.clear();

        for(int i = 0; i < indexAttrNum; ++i){
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
        }
        return true;
    }
}
