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
        str.append("|blockId: ").append(indexBlockId);
        str.append("|storageRange: [").append(startPos);
        str.append(",").append(startPos + offset).append("]");
        str.append("|timeRange: [").append(startTime);
        str.append(",").append(endTime).append("]");

        for(int i = 0; i < minValues.size(); ++i){
            str.append("|attr").append(i + 1).append(" range: [").append(minValues.get(i));
            str.append(",").append(maxValues.get(i)).append("]");
        }
        str.append("|");

        return str.toString();
    }
}
