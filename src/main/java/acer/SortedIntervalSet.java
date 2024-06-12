package acer;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import common.IndexValuePair;
import store.RidVarNamePair;

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
     * insert a new interval
     * we will merge time interval
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

    public final void including(List<IndexValuePair> pairs, List<IndexValuePair> ans){
        if(intervalNum == 0) { return; }
        if(ans == null){ ans = new ArrayList<>();}
        int len = pairs.size();

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
                    ans.add(pairs.get(checkIdx));
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
    }

    /**
     * this function is similar 'checkOverlap' function
     * @param pairs             <rid, timestamp> pair
     * @param varId             variable id
     * @return                  include this interval set <rid, varId> pairs
     */
    public final List<RidVarIdPair> including(List<IndexValuePair> pairs, int varId){
        List<RidVarIdPair> ans = new ArrayList<>();
        if(intervalNum == 0){ return ans; }

        int len = pairs.size();
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
                    IndexValuePair curPair = pairs.get(checkIdx);
                    ans.add(new RidVarIdPair(curPair.rid(), varId));
                    ++checkIdx;
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

        return ans;
    }

    /**
     * this function is similar 'checkOverlap' function
     * @param pairs             <rid, timestamp> pair
     * @param varName            variable name
     * @return                  include this interval set <rid, varId> pairs
     */
    public final List<RidVarNamePair> including(List<IndexValuePair> pairs, String varName){
        List<RidVarNamePair> ans = new ArrayList<>();
        if(intervalNum == 0){ return ans; }

        int len = pairs.size();
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
                    IndexValuePair curPair = pairs.get(checkIdx);
                    ans.add(new RidVarNamePair(curPair.rid(), varName));
                    ++checkIdx;
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

        return ans;
    }


    /**
     * according to hit marker, update interval set
     * @param pairs         pairs should be sorted
     */
    public final void including(List<IndexValuePair> pairs){
        if(intervalNum == 0){ return ; }
        int len = pairs.size();
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
                    ++checkIdx;
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
    }

    /**
     * Check which timestamp are included in SortedIntervalSet.
     * this function may change the interval set
     * If timestamp[i] is included in SortedIntervalSet,
     * then set containList[i] = true.
     * timestamps are sorted
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
     * input an interval list, check which elements can intersect with this sortedIntervalSet
     * -----------------------------------------------------
     * for example:
     * SortedIntervalSet: [5,8] [10,15] [23,30]
     * -----------------------------------------------------
     * input (should be sorted):
     * starts: [6, 17, 29, 40]
     * ends:   [9, 20, 35, 45]
     * => input interval [6, 9] [17, 20] [29, 35] [40, 45]
     * -----------------------------------------------------
     * [6, 9], [29, 35] can intersect with this SortedIntervalSet
     * [17, 20], [40, 45] cannot intersect with this SortedIntervalSet
     * then this function output is [true, false, true, false]
     * @param starts        minimum start timestamp
     * @param ends          maximum end timestamp
     * @return              this position overlap?
     */
    public final List<Boolean> checkOverlap(List<Long> starts, List<Long> ends){
        int len = starts.size();
        // initial containList
        List<Boolean> containList = new ArrayList<>(Collections.nCopies(len, false));
        if(intervalNum == 0) {return containList; }

        // initial hit array
        List<Boolean> hit = new ArrayList<>(Collections.nCopies(intervalNum, false));

        int cursor = 0;

        for(int checkIdx = 0; checkIdx < len; ++checkIdx){
            long s = starts.get(checkIdx);
            long e = ends.get(checkIdx);

            boolean earlyStop = false;
            for(int pos = cursor; pos < intervalNum; ++pos){
                if(overlap(s, e, startList.get(pos), endList.get(pos))){
                    hit.set(pos, true);
                    containList.set(checkIdx, true);
                    earlyStop = true;
                }else if(earlyStop){
                    // if start not overlap, early stop and update cursor
                    cursor = Math.max(0, pos - 1);
                    break;
                }
            }

        }
        // update hitMarkers
        hitMarkers = hit;
        // reconstruction
        reconstruction();
        return containList;
    }

    /**
     * this function is 'checkOverlap' old version
     */
    public final List<Boolean> check(List<Long> starts, List<Long> ends){
        int len = starts.size();
        // initial containList
        List<Boolean> containList = new ArrayList<>(Collections.nCopies(len, false));
        if(intervalNum == 0) {return containList; }

        // initial hit array
        List<Boolean> hit = new ArrayList<>(Collections.nCopies(intervalNum, false));

        int cursor = 0;
        for(int checkIdx = 0; checkIdx < len; ++checkIdx){
            long s = starts.get(checkIdx);
            long e = ends.get(checkIdx);
            for(int pos = cursor; pos < intervalNum; ++pos){
                if(overlap(s, e, startList.get(pos), endList.get(pos))){
                    hit.set(pos, true);
                    containList.set(checkIdx, true);
                }
            }

        }
        // update hitMarkers
        hitMarkers = hit;
        // reconstruction
        reconstruction();
        return containList;
    }

    public final boolean overlap(long s1, long e1, long s2, long e2){
        if(s1 > e1 || s2 > e2){
            throw new RuntimeException("input cannot generate legal interval," +
                    " two intervals is : [" + s1 + ", " + e1 + "] and [" + s2 + ", " + e2 + "]");
        }
        return e1 >= s2 && e2 >= s1;
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

    public final List<Long> getStartList() { return startList; }

    public final List<Long> getEndList() { return endList;}

    public void print(){
        // reconstruction();
        System.out.println("Number of Intervals: " + intervalNum);
        System.out.print("Intervals:");
        for(int i = 0; i < intervalNum; ++i){
            System.out.print(" [" + startList.get(i) + "," + endList.get(i) + "]");
        }
        System.out.println(".");
    }

    /**
     * Return all replay intervals
     * @return  intervals list (start, end)
     */
    public final List<long[]> getAllReplayIntervals(){
        List<long[]> ans = new ArrayList<>(intervalNum);
        for(int i = 0; i < intervalNum; ++i){
            long[] curInterval = {startList.get(i), endList.get(i)};
            ans.add(curInterval);
        }
        return ans;
    }

    public static void main(String[] args){
        System.out.println("test...");
        // SortedIntervalSet.testTimestampList();
        SortedIntervalSet.testInterval1();
        SortedIntervalSet.testInterval2();
    }

    public static void testTimestampList(){
        SortedIntervalSet intervals = new SortedIntervalSet();

        int w = 3;
        List<Long> timePoints = new ArrayList<>(Arrays.asList(4L, 7L, 15L, 30L, 35L));
        // start:
        // [1, 7]
        //   [4, 10]
        //           [12,  18]
        //                         [27, 33]
        //                              [32, 38]
        // merge:
        // [1, 10]   [12,18]       [27,38]
        for (long t : timePoints) {
            intervals.insert(t - w, t + w);
        }

        intervals.print();

        List<Long> checkTimestamps = new ArrayList<>(Arrays.asList(0L, 1L, 8L, 10L, 11L, 12L, 15L, 18L, 25L, 27L, 30L, 38L, 40L));
        List<Boolean> exists = intervals.checkOverlap(checkTimestamps);

        // test include function
        for (Boolean exist : exists) {
            if (exist) {
                System.out.println("timestamp: "  + " true.");
            } else {
                System.out.println("timestamp: "  + " false.");
            }
        }
    }

    public static void testInterval0(){
        SortedIntervalSet intervals = new SortedIntervalSet();

        int w = 3;
        List<Long> timePoints = new ArrayList<>(Arrays.asList(4L, 7L, 15L, 30L, 35L));
        // start:
        // [1, 7]
        //   [4, 10]
        //           [12,  18]
        //                         [27, 33]
        //                              [32, 38]
        // merge:
        // [1, 10]   [12,18]       [27,38]
        for (long t : timePoints) {
            intervals.insert(t - w, t + w);
        }

        intervals.print();

        List<Long> starts = new ArrayList<>(Arrays.asList(10L, 22L, 37L));
        List<Long> ends = new ArrayList<>(Arrays.asList(14L, 25L, 40L));

        List<Boolean> exists = intervals.checkOverlap(starts, ends);
        for(int i = 0; i < exists.size(); ++i){
            if(exists.get(i)){
                System.out.println("intervals: [" + starts.get(i) + ", " + ends.get(i) + "] overlap.");
            }else{
                System.out.println("intervals: [" + starts.get(i) + ", " + ends.get(i) +  "] does not overlap.");
            }
        }

        intervals.print();
    }

    public static void testInterval1(){
        List<Long> starts1 = new ArrayList<>(Arrays.asList(1L, 4L, 7L, 13L, 17L, 19L, 34L));
        List<Long> ends1 = new ArrayList<>(Arrays.asList(2L, 6L, 11L, 15L, 18L, 28L, 38L));

        List<Long> starts2 = new ArrayList<>(Arrays.asList(3L, 12L, 20L, 32L));
        List<Long> ends2 = new ArrayList<>(Arrays.asList(7L, 16L, 25L, 35L));

        {
            SortedIntervalSet intervalSet1 = new SortedIntervalSet();
            for(int i = 0; i < starts1.size(); ++i){
                intervalSet1.insert(starts1.get(i), ends1.get(i));
            }

            List<Boolean> overlapMark2 = intervalSet1.checkOverlap(starts2, ends2);
            List<Boolean> ans2 = new ArrayList<>(Arrays.asList(true, true, true, true));

            for(int i = 0; i< ans2.size(); ++i){
                if(ans2.get(i) != overlapMark2.get(i)){
                    System.out.println("bug position: " + i + " interval: [" + starts2.get(i) + ", " + ends2.get(i) + "].");
                }
            }
        }

        {
            SortedIntervalSet intervalSet2 = new SortedIntervalSet();
            for(int i = 0; i < starts2.size(); ++i){
                intervalSet2.insert(starts2.get(i), ends2.get(i));
            }

            List<Boolean> overlapMark1 = intervalSet2.checkOverlap(starts1, ends1);
            List<Boolean> ans1 = new ArrayList<>(Arrays.asList(false, true, true, true, false, true, true));

            for(int i = 0; i< ans1.size(); ++i){
                if(ans1.get(i) != overlapMark1.get(i)){
                    System.out.println("bug position: " + i + " interval: [" + starts1.get(i) + ", " + ends1.get(i) + "].");
                }
            }
        }
    }

    /**
     * this test aims to report bug
     */
    public static void testInterval2(){
        List<Long> starts1 = new ArrayList<>(Arrays.asList(1L, 3L, 6L, 10L, 13L, 17L, 27L, 31L));
        List<Long> ends1 = new ArrayList<>(Arrays.asList(2L, 4L, 9L, 11L, 15L, 26L, 29L, 37L));

        List<Long> starts2 = new ArrayList<>(Arrays.asList(5L, 9L, 18L, 22L, 25L, 32L, 36L, 39L));
        List<Long> ends2 = new ArrayList<>(Arrays.asList(8L, 15L, 20L, 24L, 26L, 34L, 38L, 40L));

        {
            SortedIntervalSet intervalSet1 = new SortedIntervalSet();
            for(int i = 0; i < starts1.size(); ++i){
                intervalSet1.insert(starts1.get(i), ends1.get(i));
            }

            List<Boolean> overlapMark2 = intervalSet1.checkOverlap(starts2, ends2);
            List<Boolean> ans2 = new ArrayList<>(Arrays.asList(true, true, true, true, true, true, true, false));

            for(int i = 0; i< ans2.size(); ++i){
                if(ans2.get(i) != overlapMark2.get(i)){
                    System.out.println("bug position: " + i + " interval: [" + starts2.get(i) + ", " + ends2.get(i) + "].");
                }
            }
        }

        {
            SortedIntervalSet intervalSet2 = new SortedIntervalSet();
            for(int i = 0; i < starts2.size(); ++i){
                intervalSet2.insert(starts2.get(i), ends2.get(i));
            }

            List<Boolean> overlapMark1 = intervalSet2.checkOverlap(starts1, ends1);
            List<Boolean> ans1 = new ArrayList<>(Arrays.asList(false, false, true, true, true, true, false, true));

            for(int i = 0; i< ans1.size(); ++i){
                if(ans1.get(i) != overlapMark1.get(i)){
                    System.out.println("bug position: " + i + " interval: [" + starts1.get(i) + ", " + ends1.get(i) + "].");
                }
            }
        }
    }
}
