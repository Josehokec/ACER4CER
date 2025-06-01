package acer;

import common.IndexValuePair;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SortedIntervalSetTest {

    @org.junit.jupiter.api.Test
    void demoTest(){
        // insertion may trigger merge operation
        SortedIntervalSet intervals = new SortedIntervalSet();
        int w = 3;
        List<Long> timePoints = new ArrayList<>(Arrays.asList(4L, 7L, 15L, 30L, 35L));
        // start:
        // [1, 7]
        //   [4, 10]
        //           [12,  18]
        //                         [27, 33]
        //                              [32, 38]
        // merge:[1, 10]   [12,18]       [27,38]
        for (long t : timePoints) {
            intervals.insert(t - w, t + w);
        }

        // check intervals:[1, 10]   [12,18]       [27,38]
        List<Long> targetStartList = new ArrayList<>(Arrays.asList(1L, 12L, 27L));
        List<Long> targetEndList = new ArrayList<>(Arrays.asList(10L, 18L, 38L));
        List<Long> realStartList = intervals.getStartList();
        List<Long> realEndList = intervals.getEndList();
        assertEquals(targetStartList.size(), realStartList.size());
        for (int i = 0; i < targetStartList.size(); i++) {
            assertEquals(targetStartList.get(i), realStartList.get(i));
            assertEquals(targetEndList.get(i), realEndList.get(i));
        }

        for(int i = 0; i < 100; i++){
            if(i >= 1 && i <= 10){
                assertTrue(intervals.include(i));
            }else if(i >= 12 && i <= 18){
                assertTrue(intervals.include(i));
            }else if(i >= 27 && i <= 38){
                assertTrue(intervals.include(i));
            }else{
                assertFalse(intervals.include(i));
            }
        }

        // intervals: [1,10]   [12,18]         [27,38]
        // check:          [10,14]     [22,25]      [37,40]
        List<Long> infoListStarts = new ArrayList<>(Arrays.asList(10L, 22L, 37L));
        List<Long> infoListEnds = new ArrayList<>(Arrays.asList(14L, 25L, 40L));
        boolean[] targetResults = {true, false, true};
        List<Boolean> exists = intervals.checkOverlap(infoListStarts, infoListEnds);
        for (int i = 0; i < exists.size(); i++) {
            assertEquals(exists.get(i), intervals.overlap(infoListStarts.get(i), infoListEnds.get(i)));
            assertEquals(exists.get(i), targetResults[i]);
        }
    }

    @org.junit.jupiter.api.Test
    void checkOverlap() {
        SortedIntervalSet intervals = new SortedIntervalSet();

        // -----------------------Test 2-----------------------
        List<Long> insertStarts = new ArrayList<>(Arrays.asList(3L, 8L, 12L, 16L, 20L, 24L, 35L, 49L));
        List<Long> insertEnds = new ArrayList<>(Arrays.asList(6L, 10L, 14L, 18L, 21L, 29L, 40L, 53L));
        intervals.clear();
        for(int i = 0; i < insertStarts.size(); ++i){
            intervals.insert(insertStarts.get(i), insertEnds.get(i));
        }

        List<IndexValuePair> pairs = new ArrayList<>(8);
        pairs.add(new IndexValuePair(1, null));
        pairs.add(new IndexValuePair(9, null));
        pairs.add(new IndexValuePair(13, null));
        pairs.add(new IndexValuePair(19, null));
        pairs.add(new IndexValuePair(24, null));
        pairs.add(new IndexValuePair(49, null));
        pairs.add(new IndexValuePair(52, null));
        pairs = intervals.updateAndFilter(pairs);

        // target intervals: [8, 10], [12, 14], [24, 29], [49, 53]
        List<Long> targetStartList = new ArrayList<>(Arrays.asList(8L, 12L, 24L, 49L));
        List<Long> targetEndList = new ArrayList<>(Arrays.asList(10L, 14L, 29L, 53L));
        List<Long> realStartList = intervals.getStartList();
        List<Long> realEndList = intervals.getEndList();
        assertEquals(targetStartList.size(), realStartList.size());
        for (int i = 0; i < targetStartList.size(); i++) {
            assertEquals(targetStartList.get(i), realStartList.get(i));
            assertEquals(targetEndList.get(i), realEndList.get(i));
        }
        assertEquals(5, pairs.size());

        long[] targetTs = {9L, 13L, 24L, 49L, 52L};
        for(int i = 0; i < 5; i++){
            assertEquals(targetTs[i], pairs.get(i).timestamp());
        }

//        // -----------------------Test 3-----------------------
//        Random random = new Random(5);
//        intervals.clear();
//        int intervalNum = 10_000;
//        for (int i = 1; i <= intervalNum; i++) {
//            long start = i * 10L;
//            int width = random.nextInt(5, 15);
//            intervals.insert(start, start + width);
//        }
//
//        SortedIntervalSet copiedIntervals = intervals.copy();
//        record QueryRange(long start, long end) {}
//
//        int testNum = 20_000;
//        List<QueryRange> outOfOrderRanges = new ArrayList<>(testNum);
//        for (int i = 0; i < testNum; i++) {
//            // 10, 10 * intervalNUm
//            int start = random.nextInt(10, 10 * intervalNum);
//            outOfOrderRanges.add(new QueryRange(start, start + 8));
//        }
//
//        List<QueryRange> inOrderRanges = new ArrayList<>(testNum);
//        for(QueryRange range : outOfOrderRanges){
//            inOrderRanges.add(new QueryRange(range.start, range.end));
//        }
//        inOrderRanges.sort(Comparator.comparingLong(QueryRange::start));
//
//        infoListStarts = new ArrayList<>(testNum);
//        infoListEnds = new ArrayList<>(testNum);
//
//        List<Long> comparedStarts = new ArrayList<>(testNum);
//        List<Long> comparedEnds = new ArrayList<>(testNum);
//        for(int i = 0; i < testNum; i++){
//            infoListStarts.add(outOfOrderRanges.get(i).start);
//            infoListEnds.add(outOfOrderRanges.get(i).end);
//            comparedStarts.add(inOrderRanges.get(i).start);
//            comparedEnds.add(inOrderRanges.get(i).end);
//        }
//        intervals.checkOverlap(infoListStarts, infoListEnds);
//        copiedIntervals.checkOverlap(comparedStarts, comparedEnds);
//
//        finalStarts = intervals.getStartList();
//        finalEnds = intervals.getEndList();
//        checkStarts = copiedIntervals.getStartList();
//        checkEnds = copiedIntervals.getEndList();
//        int newIntervalNum = intervals.getIntervalNum();
//        for(int i = 0; i < newIntervalNum; ++i){
//            assertEquals(finalStarts.get(i), checkStarts.get(i));
//            assertEquals(finalEnds.get(i), checkEnds.get(i));
//        }
    }
}