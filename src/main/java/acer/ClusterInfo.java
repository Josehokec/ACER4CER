package acer;

// fine-granularity filtering and read content from disk
public record ClusterInfo(int indexBlockId, int clusterId,
                          int startPos, int offset,                 // bitmap position range
                          long startTime, long endTime,
                          long[] minValues, long[] maxValues) {
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(160);
        str.append("|indexBlockId: ").append(indexBlockId);
        str.append("|clusterId: ").append(clusterId);
        str.append("|storageRange: [").append(startPos).append(",").append(startPos + offset).append("]");
        str.append("|timeRange: [").append(startTime).append(",").append(endTime).append("]");

        for(int i = 0; i < minValues.length; i++){
            str.append("|attr").append(i + 1).append(" range: [").append(minValues[i]);
            str.append(",").append(maxValues[i]).append("]");
        }
        str.append("|");

        return str.toString();
    }
}
