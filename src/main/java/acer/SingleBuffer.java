package acer;

import java.util.ArrayList;
import java.util.List;

/**
 * a single buffer binds an event type
 */
public class SingleBuffer {
    private int indexAttrNum;                       // number of index attributes
    private List<Long> minValues;                   // Attribute synopsis -> minimum values
    private List<Long> maxValues;                   // Attribute synopsis -> maximum values
    private List<ACERTemporaryQuad> quads;          // all quads have same type

    public SingleBuffer(int indexAttrNum) {
        this.indexAttrNum = indexAttrNum;
        minValues = new ArrayList<>(indexAttrNum);
        maxValues = new ArrayList<>(indexAttrNum);

        for(int i = 0; i < indexAttrNum; i++) {
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
        }

        quads = new ArrayList<>(512);
    }

    public List<Long> getMinValues() {
        return minValues;
    }

    public List<Long> getMaxValues() {
        return maxValues;
    }

    public List<Long> getRanges() {
        List<Long> ranges = new ArrayList<>(indexAttrNum);
        for(int i = 0; i < indexAttrNum; i++) {
            ranges.add(maxValues.get(i) - minValues.get(i));
        }
        return ranges;
    }

    public void append(ACERTemporaryQuad quad) {
        // update min/max values
        long[] values = quad.attrValues();
        for(int i = 0; i < indexAttrNum; i++) {
            if(values[i] < minValues.get(i)) {
                minValues.set(i, values[i]);
            }
            if(values[i] > maxValues.get(i)) {
                maxValues.set(i, values[i]);
            }
        }
        quads.add(quad);
    }

    public List<ACERTemporaryQuad> getAllQuads(){
        return quads;
    }

    public int getSize(){
        return quads.size();
    }

    public void clear(){
        quads.clear();
        minValues = new ArrayList<>(indexAttrNum);
        maxValues = new ArrayList<>(indexAttrNum);

        for(int i = 0; i < indexAttrNum; i++) {
            minValues.add(Long.MAX_VALUE);
            maxValues.add(Long.MIN_VALUE);
        }
    }
}


