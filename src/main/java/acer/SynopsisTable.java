package acer;

import java.util.*;

/**
 * SynopsisTable storage format
 * | event type | cluster information list |
 */
public class SynopsisTable {
    private final HashMap<String, List<ClusterInfo>> synopsisTable;

    public SynopsisTable(){
        synopsisTable = new HashMap<>(128);
    }

    public final void updateSynopsisTable(HashMap<String, ClusterInfo> indexPartitionClusterInfo){
        for (Map.Entry<String, ClusterInfo> entry : indexPartitionClusterInfo.entrySet()) {
            String eventType = entry.getKey();
            ClusterInfo clusterInfo = entry.getValue();

            if(synopsisTable.containsKey(eventType)){
                // insert clusterInfo
                synopsisTable.get(eventType).add(clusterInfo);
            }else{
                List<ClusterInfo> clusterInfoList = new ArrayList<>(1024);
                clusterInfoList.add(clusterInfo);
                synopsisTable.put(eventType, clusterInfoList);
            }
        }
    }

    public final List<ClusterInfo> getClusterInfo(String eventType){
        if(synopsisTable.containsKey(eventType)){
            return synopsisTable.get(eventType);
        }else{
            // return null
            return new ArrayList<>();
        }
    }

    public final Map<Integer, int[]> getRelatedBlocks(String eventType){
        Map<Integer, int[]> ans = new HashMap<>(1024);
        if(synopsisTable.containsKey(eventType)){
            List<ClusterInfo> clusterInfoList =  synopsisTable.get(eventType);

            for(ClusterInfo clusterInfo : clusterInfoList){
                int[] positionRegion = new int[2];
                positionRegion[0] = clusterInfo.startPos();
                positionRegion[1] = clusterInfo.offset();
                ans.put(clusterInfo.indexBlockId(), positionRegion);
            }
        }
        return ans;
    }

    public List<ClusterInfo> getOverlapClusterInfo(String eventType, List<Boolean> overlaps){
        if(synopsisTable.containsKey(eventType)){
            List<ClusterInfo> clusterInfoList =  synopsisTable.get(eventType);
            int size = clusterInfoList.size();
            if(size != overlaps.size()){
                throw new RuntimeException("synopsisTable length do not match");
            }
            List<ClusterInfo> ans = new ArrayList<>();
            for(int i = 0 ; i < size; ++i){
                if(overlaps.get(i)){
                    ans.add(clusterInfoList.get(i));
                }
            }
            return ans;
        }else{
            // or return null
            return new ArrayList<>();
        }
    }

    public List<ClusterInfo> getOverlapAndNotAccessClusterInfo(String eventType, List<Boolean> overlaps, Set<Integer> accessBlockId){
        if(synopsisTable.containsKey(eventType)){
            List<ClusterInfo> clusterInfoList =  synopsisTable.get(eventType);
            int size = clusterInfoList.size();
            if(size != overlaps.size()){
                throw new RuntimeException("synopsisTable length do not match");
            }
            List<ClusterInfo> ans = new ArrayList<>();
            for(int i = 0 ; i < size; ++i){
                ClusterInfo clusterInfo = clusterInfoList.get(i);
                // if overlap and without access
                if(overlaps.get(i) && !accessBlockId.contains(clusterInfo.indexBlockId())){
                    ans.add(clusterInfo);
                }
            }
            return ans;
        }else{
            // or return null
            return new ArrayList<>();
        }
    }

    /**
     * here we use args to return
     * @param eventType         event type
     * @param startTimeList     start timestamp
     * @param endTimeList       end timestamp
     */
    public void getStartEndTimestamp(String eventType, List<Long> startTimeList, List<Long> endTimeList){
        List<ClusterInfo> clusterInfoList = synopsisTable.get(eventType);
        if(clusterInfoList == null){
            return;
        }
        for(ClusterInfo clusterInfo : clusterInfoList){
            startTimeList.add(clusterInfo.startTime());
            endTimeList.add(clusterInfo.endTime());
        }
    }

    public void print(){
        for (Map.Entry<String, List<ClusterInfo>> entry : synopsisTable.entrySet()) {
            String eventType = entry.getKey();
            List<ClusterInfo> clusterInfoList = entry.getValue();
            System.out.println("event type: " + eventType + ", cluster information as follows:");
            for(ClusterInfo info : clusterInfoList){
                System.out.println(info);
            }
        }
    }
}
