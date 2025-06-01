package acer;

import condition.ICQueryQuad;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * SynopsisTable storage format:
 * | event_type_1 | cluster_information_list_1 |
 * | event_type_2 | cluster_information_list_2 |
 * | event_type_3 | cluster_information_list_3 |
 * ...
 */
public class SynopsisTable {
    private static final SynopsisTable st = new SynopsisTable();

    private final HashMap<String, List<ClusterInfo>> synopsisTable;

    private SynopsisTable(){
        synopsisTable = new HashMap<>(32);
    }

    public static SynopsisTable getInstance(){
        return st;
    }

    /**
     * after completing the serialization of an index block,
     * we need to update all cluster information of this index block to SynopsisTable
     * @param synopsisFromIndexBlock    cluster information
     */
    public void updateSynopsisTable(HashMap<String, ClusterInfo> synopsisFromIndexBlock){
        for (HashMap.Entry<String, ClusterInfo> entry : synopsisFromIndexBlock.entrySet()) {
            String type = entry.getKey();
            ClusterInfo clusterInfo = entry.getValue();
            synopsisTable.computeIfAbsent(type, k -> new ArrayList<>(1024)).add(clusterInfo);
        }
    }

    public void updateSynopsisTable(String eventType, ClusterInfo clusterInfo){
        synopsisTable.computeIfAbsent(eventType, k -> new ArrayList<>()).add(clusterInfo);
    }

    public  List<ClusterInfo> getClusterInfo(String eventType){
        // avoid return null value
        return synopsisTable.getOrDefault(eventType, new ArrayList<>());
    }

    /**
     * [updated] this function is called by the first processed variable
     * @param eventType         variable's event type
     * @param icQuads           independent conditions
     * @return                  cluster information list
     */
    public List<ClusterInfo> getRelatedBlocks(String eventType, List<ICQueryQuad> icQuads){
        List<ClusterInfo> clusterInfoList = getClusterInfo(eventType);
        // answer
        List<ClusterInfo> relatedBlocks = new ArrayList<>(clusterInfoList.size() * 2/ 3);
        for(ClusterInfo clusterInfo : clusterInfoList){
            long[] miValues = clusterInfo.minValues();
            long[] maxValues = clusterInfo.maxValues();
            for(ICQueryQuad quad : icQuads){
                int idx = quad.idx();
                if(maxValues[idx] >= quad.min() && miValues[idx] <= quad.max()){
                    relatedBlocks.add(clusterInfo);
                }
            }
        }
        return relatedBlocks;
    }

    /**
     * [updated] please ensure that overlaps.size() = clusterInfoList.size()
     * @param eventType         variable's event type
     * @param overlaps          boolean list
     * @return                  cluster information list
     */
    public List<ClusterInfo> getOverlappedClusterInfo(String eventType, List<Boolean> overlaps){
        List<ClusterInfo> clusterInfoList = getClusterInfo(eventType);
        int size = clusterInfoList.size();
        List<ClusterInfo> overlappedClusterInfo = new ArrayList<>(size * 2/ 3);
        for(int i = 0 ; i < size; ++i){
            if(overlaps.get(i)){
                overlappedClusterInfo.add(clusterInfoList.get(i));
            }
        }
        return overlappedClusterInfo;
    }

    /**
     * return values lie in args
     * @param eventType         variable's event type
     * @param startTimeList     start timestamp list
     * @param endTimeList       end timestamp
     */
    public void getStartEndTimestamp(String eventType, List<Long> startTimeList, List<Long> endTimeList){
        List<ClusterInfo> clusterInfoList = getClusterInfo(eventType);
        for(ClusterInfo clusterInfo : clusterInfoList){
            startTimeList.add(clusterInfo.startTime());
            endTimeList.add(clusterInfo.endTime());
        }
    }

    public void print(){
        for (HashMap.Entry<String, List<ClusterInfo>> entry : synopsisTable.entrySet()) {
            String eventType = entry.getKey();
            List<ClusterInfo> clusterInfoList = entry.getValue();
            System.out.println("event type: " + eventType + ", cluster information as follows:");
            for(ClusterInfo info : clusterInfoList){
                System.out.println(info);
            }
        }
    }
}
