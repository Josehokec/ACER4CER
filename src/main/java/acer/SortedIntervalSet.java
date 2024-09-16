package acer;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import common.IndexValuePair;

/**
 * SortedIntervalSet stores time interval
 * an interval has start time and end time
 */

public class SortedIntervalSet{
    private List<Long> startList;
    private List<Long> endList;
    private List<Boolean> hitMarkers;
    private int intervalNum;

    public SortedIntervalSet(){
        startList = new ArrayList<>(16);
        endList = new ArrayList<>(16);
        hitMarkers = new ArrayList<>(16);
        intervalNum = 0;
    }

    public SortedIntervalSet(int size){
        startList = new ArrayList<>(size);
        endList = new ArrayList<>(size);
        hitMarkers = new ArrayList<>(size);
        intervalNum = 0;
    }

    /**
     * insert a new interval, note that we will merge time interval
     */
    public final boolean insert(long s, long e){
        if(e < s){
            throw new RuntimeException( "interval illegal.");
        }

        long previousStart = (intervalNum == 0) ? Long.MIN_VALUE : startList.get(intervalNum - 1);
        long previousEnd = (intervalNum == 0) ? Long.MIN_VALUE : endList.get(intervalNum - 1);

        if(s < previousStart){
            // only support monotonic insertion
            throw new RuntimeException("insertion illegal.");
        }else if(s <= previousEnd){
            // new interval and old interval overlap, then we merge them
            // here we only need to update the previousEnd value
            endList.set(intervalNum - 1, Math.max(previousEnd, e));
        }else{
            // add new interval to SortedIntervalSet
            startList.add(s);
            endList.add(e);
            intervalNum++;
            hitMarkers.add(true);
        }
        return true;
    }

    /**
     * check a timestamp whether include in SortedIntervalSet
     * this function does not change the interval set
     */
    @SuppressWarnings("unused")
    public final boolean include(long t){
        for(int i = 0; i < intervalNum; ++i){
            long s = startList.get(i);
            long e = endList.get(i);
            if(t >= s && t <= e){
                return true;
            }else{
                if(t < s){
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * this function will update (remove) these intervals that cannot generate any matched results,
     * and filtering these events whose timestamps do not within time intervals
     * @param pairs - pairs
     * @return filtered pairs
     */
    public final List<IndexValuePair> updateAndFilter(List<IndexValuePair> pairs){
        if(intervalNum == 0) {
            return new ArrayList<>(8);
        }
        int len = pairs.size();
        List<IndexValuePair> filteredPairs = new ArrayList<>((int) (len * 0.65));
        // initial hit array
        List<Boolean> hit = new ArrayList<>(Collections.nCopies(intervalNum, false));

        // filtering algorithm
        int cursor = 0;
        for(int checkIdx = 0; checkIdx < len; ){
            long t = pairs.get(checkIdx).timestamp();

            while(t < startList.get(cursor)){
                ++checkIdx;
                if(checkIdx >= len){
                    break;
                }
                t = pairs.get(checkIdx).timestamp();
            }

            if(t >= startList.get(cursor)){
                if(t <= endList.get(cursor)){
                    hit.set(cursor, true);
                    filteredPairs.add(pairs.get(checkIdx));
                    checkIdx++;
                }else{
                    cursor++;
                }
            }

            if(cursor >= intervalNum){
                break;
            }
        }

        // update hitMarkers
        hitMarkers = hit;
        // reconstruction
        reconstruction();
        return filteredPairs;
    }

    /**
     * Check which timestamp are included in SortedIntervalSet.
     * here timestamps are sorted
     * this function may change the interval set
     * If timestamp[i] is included in SortedIntervalSet,
     * then set containList[i] = true.
     */
    public final List<Boolean> checkOverlap(List<Long> timestamps){
        int len = timestamps.size();
        // initial hit array
        List<Boolean> hit = new ArrayList<>(Collections.nCopies(intervalNum, false));
        // initial containList
        List<Boolean> containList = new ArrayList<>(Collections.nCopies(len, false));

        // early stop
        if(intervalNum == 0){ return containList; }

        // filtering algorithm
        int cursor = 0;
        for(int checkIndex = 0; checkIndex < len; ){
            long t = timestamps.get(checkIndex);

            while(t < startList.get(cursor)){
                ++checkIndex;
                if(checkIndex >= len){
                    break;
                }
                t = timestamps.get(checkIndex);
            }

            if(t >= startList.get(cursor)){
                if(t <= endList.get(cursor)){
                    hit.set(cursor, true);
                    containList.set(checkIndex, true);
                    ++checkIndex;
                }else{
                    cursor++;
                }
            }

            if(cursor >= intervalNum){
                break;
            }
        }

        // update hitMarkers
        hitMarkers = hit;
        // reconstruction
        reconstruction();

        return containList;
    }

    /**
     * new version: we only require starts[i] <= ends[i], allow out-of-order
     * please note that intervals always keep ordered
     * this function cannot update/change interval set
     * -----------------------------------------------------
     * input an interval list, check which elements can intersect with this sortedIntervalSet.
     * For example:
     * SortedIntervalSet: [5,8] [10,15] [23,30], our input is:
     * starts: [6, 17, 29, 40]
     * ends:   [9, 20, 35, 45]
     * which means => input interval [6, 9] [17, 20] [29, 35] [40, 45]
     * -----------------------------------------------------
     * [6, 9], [29, 35] can intersect with this SortedIntervalSet
     * [17, 20], [40, 45] cannot intersect with this SortedIntervalSet
     * then this function output is [true, false, true, false]
     * @param starts        start timestamp list
     * @param ends          end timestamp list
     * @return              position x in list can overlap sorted intervals?
     */
    public final List<Boolean> checkOverlap(List<Long> starts, List<Long> ends){
        int len = starts.size();
        // initial containList
        List<Boolean> containList = new ArrayList<>(Collections.nCopies(len, false));
        if(intervalNum == 0) {return containList; }

        // initial hit array
        // List<Boolean> hit = new ArrayList<>(Collections.nCopies(intervalNum, false));

        int cursor = 0;

        // to support out-of-order insertion, we need previous start and end timestamp
        long previousStart = -2;
        long previousEnd = -1;
        int previousCursor = 0;

        for(int checkIdx = 0; checkIdx < len; ++checkIdx){
            long start = starts.get(checkIdx);
            long end = ends.get(checkIdx);

            // to support out-of-order insertion, we need previous start and end timestamp
            if(start < previousEnd && start >= previousStart){
                //System.out.println("cursor reset previous one.");
                cursor = previousCursor;
            }else if(start < previousStart){
                //System.out.println("cursor reset 0.");
                cursor = 0;
            }

            // old version: its complexity is $O(n^2)$
//            for(int checkIdx = 0; checkIdx < len; ++checkIdx){
//                long s = starts.get(checkIdx);
//                long e = ends.get(checkIdx);
//                    for(int pos = cursor; pos < intervalNum; ++pos){
//                        if(overlap(s, e, startList.get(pos), endList.get(pos))){
//                            hit.set(pos, true);
//                            containList.set(checkIdx, true);
//                        }
//                    }
//            }

            // new version: more fast, because its complexity is $O(n)$
            // reason (see an example)
            // input:    [2,      10]          [16,20]
            // intervals [2.3] [4,8]   [13,15]     [19,23]   [28,30]
            boolean earlyStop = false;

            for(int pos = cursor; pos < intervalNum; ++pos){
                if(overlap(start, end, startList.get(pos), endList.get(pos))){
                    //hit.set(pos, true);
                    containList.set(checkIdx, true);
                    earlyStop = true;
                }else if(earlyStop){
                    // to support out-of-order insertion, we need previous cursor
                    previousCursor = cursor;
                    // if start not overlap, early stop and update cursor
                    cursor = Math.max(0, pos - 1);
                    break;
                }
            }

            // support out-of-order insertion
            previousStart = start;
            previousEnd = end;
        }
        // update hitMarkers
        // hitMarkers = hit;
        // reconstruction
        // reconstruction();
        return containList;
    }

    public final boolean overlap(long s1, long e1, long s2, long e2){
        if(s1 > e1 || s2 > e2){
            throw new RuntimeException("input cannot generate legal interval," +
                    " two intervals is : [" + s1 + ", " + e1 + "] and [" + s2 + ", " + e2 + "]");
        }
        return e1 >= s2 && e2 >= s1;
    }

    public final boolean overlap(long start, long end){
        for(int i = 0; i < intervalNum; ++i){
            if(overlap(start, end, startList.get(i), endList.get(i))){
                return true;
            }
        }
        return false;
    }

    /**
     * delete the intervals that do not be hit
     */
    public final void reconstruction(){
        List<Long> newStartList = new ArrayList<>(intervalNum);
        List<Long> newEndList = new ArrayList<>(intervalNum);
        List<Boolean> newHitMarkers = new ArrayList<>(intervalNum);
        // number of overlapped interval
        int cnt = 0;

        for(int i = 0; i < intervalNum; ++i){
            if(hitMarkers.get(i)){
                newStartList.add(startList.get(i));
                newEndList.add(endList.get(i));
                newHitMarkers.add(true);
                cnt++;
            }
        }

        // update startList, endList, hitMarkers, intervalNum
        startList = newStartList;
        endList = newEndList;
        hitMarkers = newHitMarkers;
        intervalNum = cnt;

        if(intervalNum == 0){
            System.out.println("interval set size is 0");
        }
    }

    @SuppressWarnings("unused")
    public final int getIntervalNum(){
        return intervalNum;
    }

    @SuppressWarnings("unused")
    public final List<Long> getStartList() { return startList; }

    @SuppressWarnings("unused")
    public final List<Long> getEndList() { return endList;}

    public void print(){
        System.out.println("Number of Intervals: " + intervalNum);
        System.out.print("Intervals:");
        for(int i = 0; i < intervalNum; ++i){
            System.out.print(" [" + startList.get(i) + "," + endList.get(i) + "]");
        }
        System.out.println(".");
    }

    @SuppressWarnings("unused")
    public final List<long[]> getAllReplayIntervals(){
        List<long[]> ans = new ArrayList<>(intervalNum);
        for(int i = 0; i < intervalNum; ++i){
            long[] curInterval = {startList.get(i), endList.get(i)};
            ans.add(curInterval);
        }
        return ans;
    }

    public void clear(){
        startList = new ArrayList<>(16);
        endList = new ArrayList<>(16);
        hitMarkers = new ArrayList<>(16);
        intervalNum = 0;
    }

    public SortedIntervalSet copy(){
        SortedIntervalSet clone = new SortedIntervalSet(intervalNum);
        for(int i = 0; i < intervalNum; ++i){
            clone.insert(startList.get(i), endList.get(i));
        }
        return clone;
    }
}
