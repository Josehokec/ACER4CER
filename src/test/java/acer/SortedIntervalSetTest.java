package acer;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SortedIntervalSetTest {

    @org.junit.jupiter.api.Test
    void checkOverlap() {
        // insertion may trigger merge operation
        // -----------------------Test 1-----------------------
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
        //intervals.print();
        List<Long> infoListStarts = new ArrayList<>(Arrays.asList(10L, 22L, 37L));
        List<Long> infoListEnds = new ArrayList<>(Arrays.asList(14L, 25L, 40L));

        List<Boolean> exists = intervals.checkOverlap(infoListStarts, infoListEnds);
        for (int i = 0; i < exists.size(); i++) {
            assertEquals(exists.get(i), intervals.overlap(infoListStarts.get(i), infoListEnds.get(i)));
        }
        // here sorted interval set: [1,10] [12,18] [27,38]
        List<Long> checkStarts = new ArrayList<>(Arrays.asList(1L, 12L, 27L));
        List<Long> checkEnds = new ArrayList<>(Arrays.asList(10L, 18L, 38L));
        List<Long> finalStarts = intervals.getStartList();
        List<Long> finalEnds = intervals.getEndList();
        assertEquals(checkStarts.size(), finalStarts.size());
        for (int i = 0; i < checkStarts.size(); i++) {
            assertEquals(checkStarts.get(i), finalStarts.get(i));
            assertEquals(checkEnds.get(i), finalEnds.get(i));
        }

        // -----------------------Test 2-----------------------
        List<Long> insertStarts = new ArrayList<>(Arrays.asList(3L, 8L, 12L, 16L, 20L, 24L, 35L, 49L));
        List<Long> insertEnds = new ArrayList<>(Arrays.asList(6L, 10L, 14L, 18L, 21L, 29L, 40L, 53L));
        intervals.clear();
        for(int i = 0; i < insertStarts.size(); ++i){
            intervals.insert(insertStarts.get(i), insertEnds.get(i));
        }

        infoListStarts = new ArrayList<>(Arrays.asList(1L, 5L, 8L, 10L, 9L, 22L, 27L, 31L, 28L, 36L, 55L));
        infoListEnds = new ArrayList<>(Arrays.asList(2L, 9L, 11L, 14L, 17L, 23L, 28L, 34L, 39L, 42L, 57L));
        exists = intervals.checkOverlap(infoListStarts, infoListEnds);
        for (int i = 0; i < exists.size(); i++) {
            assertEquals(exists.get(i), intervals.overlap(infoListStarts.get(i), infoListEnds.get(i)));
        }

        checkStarts = new ArrayList<>(Arrays.asList(3L, 8L, 12L, 16L, 24L, 35L));
        checkEnds = new ArrayList<>(Arrays.asList(6L, 10L, 14L, 18L, 29L, 40L));
        finalStarts = intervals.getStartList();
        finalEnds = intervals.getEndList();
        assertEquals(checkStarts.size(), finalStarts.size());
        for (int i = 0; i < checkStarts.size(); i++) {
            assertEquals(checkStarts.get(i), finalStarts.get(i));
            assertEquals(checkEnds.get(i), finalEnds.get(i));
        }

        // -----------------------Test 3-----------------------
        Random random = new Random(5);
        intervals.clear();
        int intervalNum = 10_000;
        for (int i = 1; i <= intervalNum; i++) {
            long start = i * 10L;
            int width = random.nextInt(5, 15);
            intervals.insert(start, start + width);
        }

        SortedIntervalSet copiedIntervals = intervals.copy();
        record QueryRange(long start, long end) {}

        int testNum = 20_000;
        List<QueryRange> outOfOrderRanges = new ArrayList<>(testNum);
        for (int i = 0; i < testNum; i++) {
            // 10, 10 * intervalNUm
            int start = random.nextInt(10, 10 * intervalNum);
            outOfOrderRanges.add(new QueryRange(start, start + 8));
        }

        List<QueryRange> inOrderRanges = new ArrayList<>(testNum);
        for(QueryRange range : outOfOrderRanges){
            inOrderRanges.add(new QueryRange(range.start, range.end));
        }
        inOrderRanges.sort(Comparator.comparingLong(QueryRange::start));

        infoListStarts = new ArrayList<>(testNum);
        infoListEnds = new ArrayList<>(testNum);

        List<Long> comparedStarts = new ArrayList<>(testNum);
        List<Long> comparedEnds = new ArrayList<>(testNum);
        for(int i = 0; i < testNum; i++){
            infoListStarts.add(outOfOrderRanges.get(i).start);
            infoListEnds.add(outOfOrderRanges.get(i).end);
            comparedStarts.add(inOrderRanges.get(i).start);
            comparedEnds.add(inOrderRanges.get(i).end);
        }
        intervals.checkOverlap(infoListStarts, infoListEnds);
        copiedIntervals.checkOverlap(comparedStarts, comparedEnds);

        finalStarts = intervals.getStartList();
        finalEnds = intervals.getEndList();
        checkStarts = copiedIntervals.getStartList();
        checkEnds = copiedIntervals.getEndList();
        int newIntervalNum = intervals.getIntervalNum();
        for(int i = 0; i < newIntervalNum; ++i){
            assertEquals(finalStarts.get(i), checkStarts.get(i));
            assertEquals(finalEnds.get(i), checkEnds.get(i));
        }
    }
}