package join;

import common.Converter;
import common.EventPattern;
import common.EventSchema;
import common.Metadata;
import condition.DependentConstraint;
import common.PartialMatch;

import java.util.*;

/**
 * Greedy Join
 * Select the bucket with the smallest number of events to join at each step
 * only can be used for skip-till-ant-match
 */
public class GreedyJoin extends AbstractJoinEngine {

    /**
     * bucket store byte record
     * @param pattern query pattern
     * @param buckets buckets, each bucket stores byte records
     * @return        number of matched tuples -> count(*)
     */
    @Override
    public int countTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets) {
        // First, sort based on the number of events in the bucket
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // early flag
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            output.append(" ").append(bucketSize);
            pairs.add(new Pair(bucketSize, i));
            if(bucketSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if (earlyStop) { return 0; }
        // sort according to number of events
        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        String schemaName = pattern.getSchemaName();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        // Load the index corresponding to the timestamp of the attribute type array and sequence variable array
        int timeIdx = schema.getTimestampIdx();

        // greedy choose
        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);

            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        // start join
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        HashSet<byte[]> firstEventSet = new HashSet<>();
        int ans = 0;
        for(PartialMatch tuple : partialMatches){
            byte[] firstEvent = tuple.matchList().get(0);
            if(!firstEventSet.contains(firstEvent)){
                ans++;
                firstEventSet.add(firstEvent);
            }
        }

        return ans;
    }

    /**
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    @Override
    public List<Tuple> getTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets) {
        // First, sort based on the number of tuples in the bucket
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // early stop flag
        boolean flag = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            output.append(" ").append(bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        output.append(" ]");
        System.out.println(output);

        if (flag) { return new ArrayList<>(); }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);

            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        List<Tuple> ans = new ArrayList<>();
        // start joining
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        HashSet<byte[]> firstEventSet = new HashSet<>();
        for(PartialMatch tuple : partialMatches){
            byte[] firstEvent = tuple.matchList().get(0);
            if(!firstEventSet.contains(firstEvent)){
                firstEventSet.add(firstEvent);
                Tuple t = new Tuple(buckets.size());
                for(byte[] record : tuple.matchList()){
                    String event = schema.byteEventToString(record);
                    t.addEvent(event);
                }
                ans.add(t);
            }
        }
        return ans;
    }

    @Override
    public int countTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets) {
        // First, sort based on the number of tuples in the bucket
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        // early stop flag
        boolean flag = false;
        System.out.print("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            System.out.print(" " + bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        System.out.println(" ]");

        if (flag) { return 0; }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        String schemaName = pattern.getSchemaName();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);

        int timeIdx = schema.getTimestampIdx();

        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        // start joining
        for(int i = 1; i < patternLen; ++i){
            int curIndex = pairs.get(i).index();
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            if(partialMatches.size() == 0){
                return 0;
            }
            // update marks
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    /**
     * S3 = skip-till-any-match
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    @Override
    public List<Tuple> getTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets) {
        int patternLen = buckets.size();
        record Pair(int bucketSize, int index) { }
        List<Pair> pairs = new ArrayList<>(patternLen);
        boolean flag = false;
        System.out.print("bucket sizes: [");
        for(int i = 0; i < patternLen; ++i){
            int bucketSize = buckets.get(i).size();
            System.out.print(" " + bucketSize);
            pairs.add(new Pair(bucketSize, i));
            flag = bucketSize == 0;
        }
        System.out.println(" ]");

        if (flag) { return new ArrayList<>(); }

        pairs.sort(Comparator.comparingInt(Pair::bucketSize));

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        int index0 = pairs.get(0).index();
        List<byte[]> bucket0 = buckets.get(index0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());
        for (byte[] curRecord : bucket0) {
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(curRecord);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        List<Integer> marks = new ArrayList<>(1);
        marks.add(index0);

        List<Tuple> ans = new ArrayList<>();
        // start joining
        for(int i = 1; i < patternLen; ++i){

            System.out.print("marks:");
            for(int m : marks){
                System.out.print(" " + m);
            }

            int curIndex = pairs.get(i).index();
            System.out.println(" curIndex: " + curIndex);
            partialMatches = joinWithBytesRecord(pattern, partialMatches, marks, buckets.get(curIndex), curIndex);
            if(partialMatches.size() == 0){
                return ans;
            }
            // keep marks ordered
            marks.add(curIndex);
            Collections.sort(marks);
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        for(PartialMatch tuple : partialMatches){
            Tuple t = new Tuple(patternLen);
            for(byte[] record : tuple.matchList()){
                String eventRecord = schema.byteEventToString(record);
                t.addEvent(eventRecord);

            }
            ans.add(t);
        }
        return ans;
    }

    /**
     * @param pattern           query pattern
     * @param partialMatches    partial matches
     * @param marks             variable number for completed join
     * @param bucket            bucket that need to be joined
     * @param k                 join position
     * @return                  partial matched results
     */
    public final List<PartialMatch> joinWithBytesRecord(EventPattern pattern, List<PartialMatch> partialMatches,
                                                                      List<Integer> marks, List<byte[]> bucket, int k){


        // find k position
        int pos = -1;
        int len = marks.size();
        for(int i = 0; i < len; ++i){
            if(k < marks.get(i)){
                pos = i;
                break;
            }
        }

        if(pos == -1){ pos = len;}

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        String[] attrTypes = schema.getAttrTypes();
        final int timeIdx = schema.getTimestampIdx();
        final long tau = pattern.getTau();

        List<PartialMatch> ans = new ArrayList<>(128);
        List<DependentConstraint> dcList = pattern.getDCListToJoin(marks, k);

        // [0, pos) k [pos, len)
        int curPtr = 0;
        for(PartialMatch partialMatch : partialMatches) {
            long firstTime = partialMatch.timeList().get(0);
            long lastTime = partialMatch.timeList().get(len-1);

            for(int i = curPtr; i < bucket.size(); ++i){
                byte[] curRecord = bucket.get(i);
                long curTime = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));

                if(pos == 0){

                    if(curTime > partialMatch.timeList().get(0)){
                        break;
                    }else if(firstTime - curTime > tau){
                        // Exceeding tau, update pointer position to avoid multiple reads
                        curPtr++;
                    }else if(lastTime - curTime <= tau){
                        // If exist identical events, skip
                        byte[] firstRecord = partialMatch.matchList().get(0);
                        boolean isEqual = Arrays.equals(curRecord, firstRecord);

                        if(!isEqual){

                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;

                                    String attrName = dc.getAttrName();

                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);

                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {

                                        curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }
                                    boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                    if (!hold) {
                                        satisfy = false;
                                    }
                                }
                            }

                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);
                                matchList.add(curRecord);
                                matchList.addAll(partialMatch.matchList());
                                List<Long> timeList = new ArrayList<>(len + 1);
                                timeList.add(curTime);
                                timeList.addAll(partialMatch.timeList());
                                ans.add(new PartialMatch(timeList, matchList));
                            }
                        }
                    }
                }else if(pos == len){
                    if(curTime < firstTime){

                        curPtr++;
                    }else if(curTime - firstTime > tau){
                        break;
                    }else if(curTime >= lastTime){
                        byte[] lastRecord = partialMatch.matchList().get(len - 1);
                        boolean isEqual = Arrays.equals(curRecord, lastRecord);

                        if(!isEqual){

                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;

                                    String attrName = dc.getAttrName();

                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);

                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {

                                        curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }
                                    boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                    if (!hold) {
                                        satisfy = false;
                                        break;
                                    }
                                }
                            }

                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);
                                matchList.addAll(partialMatch.matchList());
                                matchList.add(curRecord);
                                List<Long> timeList = new ArrayList<>(len + 1);
                                timeList.addAll(partialMatch.timeList());
                                timeList.add(curTime);
                                ans.add(new PartialMatch(timeList, matchList));
                            }
                        }
                    }
                }else{
                    // [0, pos) k [pos, len)
                    if(curTime < firstTime){
                        // Update pointer position
                        curPtr++;
                    }else if(curTime > partialMatch.timeList().get(pos)){
                        break;
                    }else if(curTime >= partialMatch.timeList().get(pos - 1) && curTime <= partialMatch.timeList().get(pos)){

                        byte[] preRecord = partialMatch.matchList().get(pos - 1);
                        byte[] nextRecord = partialMatch.matchList().get(pos);

                        boolean isEqual1 = Arrays.equals(preRecord, curRecord);
                        boolean isEqual2 = Arrays.equals(nextRecord, curRecord);

                        if(!isEqual1 && !isEqual2){

                            boolean satisfy = true;
                            if(dcList != null && dcList.size() != 0){
                                for(DependentConstraint dc : dcList) {
                                    String varName1 = dc.getVarName1();
                                    String varName2 = dc.getVarName2();
                                    int varIndex1 = pattern.getVarNamePos(varName1);
                                    int varIndex2 = pattern.getVarNamePos(varName2);
                                    int cmpRecordPos = -1;

                                    String attrName = dc.getAttrName();

                                    int idx = schema.getAttrNameIdx(attrName);
                                    boolean isVarName1 = (k == varIndex1);

                                    if (isVarName1) {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex2) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    } else {
                                        for (int j = 0; j < marks.size(); j++) {
                                            if (marks.get(j) == varIndex1) {
                                                cmpRecordPos = j;
                                                break;
                                            }
                                        }
                                    }

                                    byte[] cmpRecord = partialMatch.matchList().get(cmpRecordPos);
                                    long curValue, cmpValue;
                                    if (attrTypes[idx].equals("INT")) {
                                        curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {

                                        curValue = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, idx));
                                        cmpValue = Converter.bytesToLong(schema.getIthAttrBytes(cmpRecord, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }
                                    boolean hold = isVarName1 ? dc.satisfy(curValue, cmpValue) : dc.satisfy(cmpValue, curValue);
                                    if (!hold) {
                                        satisfy = false;
                                    }
                                }
                            }
                            // If the dependency predicate constraint is satisfied,
                            // then add it to results
                            if(satisfy){
                                List<byte[]> matchList = new ArrayList<>(len + 1);

                                for(int j = 0; j < pos; ++j){
                                    matchList.add(partialMatch.matchList().get(j));
                                }
                                matchList.add(curRecord);
                                for(int j = pos; j < len; ++j){
                                    matchList.add(partialMatch.matchList().get(j));
                                }

                                List<Long> timeList = new ArrayList<>(len + 1);
                                for(int j = 0; j < pos; ++j){
                                    timeList.add(partialMatch.timeList().get(j));
                                }
                                timeList.add(curTime);
                                for(int j = pos; j < len; ++j){
                                    timeList.add(partialMatch.timeList().get(j));
                                }

                                ans.add(new PartialMatch(timeList, matchList));
                            }
                        }
                    }
                }
            }
        }

        return ans;
    }

}
