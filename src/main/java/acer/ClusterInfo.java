package acer;

import java.util.List;

public record ClusterInfo(int indexBlockId,
                          int startPos, int offset,
                          long startTime, long endTime,
                          List<Long> minValues,
                          List<Long> maxValues) {
    @Override
    public String toString(){
        StringBuffer str = new StringBuffer(256);

        str.append("|startPos: ").append(startPos);
        str.append("|offset: ").append(offset);
        str.append("|startTime: ").append(startTime);
        str.append("|endTime: ").append(endTime);

        for(int i = 0; i < minValues.size(); ++i){
            str.append("|<minValue: ").append(minValues.get(i));
            str.append(", maxValue: ").append(maxValues.get(i)).append(">");
        }
        str.append("|");

        return str.toString();
    }
}
