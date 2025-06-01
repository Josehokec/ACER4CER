package acer;

import java.util.ArrayList;
import java.util.List;

/**
 * [updated] here we support deletion operation and out-of-order insertion
 * a single buffer binds an event type
 */
public class SingleBuffer {
    private boolean hasUpdateMinMax = false;            // update flag
    private final long[] minValues;                     // attribute synopsis -> minimum values
    private final long[] maxValues;                     // attribute synopsis -> maximum values
    private final List<TemporaryTriple> triples;    // all quads have same type

    public SingleBuffer(int indexAttrNum){
        minValues = new long[indexAttrNum];
        maxValues = new long[indexAttrNum];
        triples = new ArrayList<>(512);
    }

    public void append(TemporaryTriple triple){
        triples.add(triple);
    }

    public void setMinMaxValues(){
        int indexAttrNum = minValues.length;
        // initialization
        for(int i = 0; i < indexAttrNum; i++){
            minValues[i] = Long.MAX_VALUE;
            maxValues[i] = Long.MIN_VALUE;
        }

        for(TemporaryTriple quad : triples){
            long[] attrValues = quad.attrValues();
            for(int i = 0; i < indexAttrNum; i++){
                long attrValue = attrValues[i];
                if(attrValue < minValues[i]){
                    minValues[i] = attrValue;
                }
                if(attrValue > maxValues[i]){
                    maxValues[i] = attrValue;
                }
            }
        }
        hasUpdateMinMax = true;
    }

    public long[] getMinValues(){
        if(!hasUpdateMinMax){
            setMinMaxValues();
        }
        return minValues;
    }

    public long[] getMaxValues(){
        if(!hasUpdateMinMax){
            setMinMaxValues();
        }
        return maxValues;
    }

    public long[] getRanges(){
        if(!hasUpdateMinMax){
            setMinMaxValues();
        }
        int indexAttrNum = minValues.length;
        long[] ranges = new long[indexAttrNum];
        for(int i = 0; i < indexAttrNum; i++){
            ranges[i] = maxValues[i] - minValues[i];
        }
        return ranges;
    }

    public List<TemporaryTriple> getAllTriples(){
        return triples;
    }

    public int getSize(){
        return triples.size();
    }

    public void clear(){
        hasUpdateMinMax = false;
        triples.clear();
    }
}
