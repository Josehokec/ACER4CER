package join;

import common.Converter;
import common.EventPattern;
import common.EventSchema;
import common.Metadata;
import condition.DependentConstraint;
import common.PartialMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * notice: greedy join cannot prefect process skip-till-next-match
 * Join with the order of defined sequence in event patterns
 * record PartialResultWithTime(List<Long> timeList, List<String> matchList){}
 */
public class OrderJoin extends AbstractJoinEngine {

    @Override
    public int countTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");

        System.out.println(output);
        if(earlyStop){
            return 0;
        }

        int patternLen = buckets.size();

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);

        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        for(int i = 1; i < patternLen; ++i){
            partialMatches = seqJoinUsingFollowBy(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    @Override
    public List<Tuple> getTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return new ArrayList<>(); }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();
        List<byte[]> bucket0 = buckets.get(0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        List<Tuple> ans = new ArrayList<>();

        // use skip till next match
        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = seqJoinUsingFollowBy(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        for(PartialMatch tuple : partialMatches){
            Tuple t = new Tuple(buckets.size());
            for(byte[] record : tuple.matchList()){
                String event = schema.byteEventToString(record);
                t.addEvent(event);
            }
            ans.add(t);
        }

        return ans;
    }

    @Override
    public int countTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return 0; }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }

        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = seqJoinUsingFollowByAny(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return 0;
            }
        }

        return partialMatches.size();
    }

    @Override
    public List<Tuple> getTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets) {
        boolean earlyStop = false;

        StringBuilder output = new StringBuilder(64);
        output.append("bucket sizes: [");
        for (List<byte[]> bucket : buckets) {
            int buckSize = bucket.size();
            output.append(" ").append(buckSize);
            if(buckSize == 0){
                earlyStop = true;
            }
        }
        output.append(" ]");
        System.out.println(output);

        if(earlyStop){ return new ArrayList<>(); }

        pattern.sortDC();

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());

        int timeIdx = schema.getTimestampIdx();

        List<byte[]> bucket0 = buckets.get(0);
        List<PartialMatch> partialMatches = new ArrayList<>(bucket0.size());

        for(byte[] record : bucket0){
            long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(record, timeIdx));
            List<Long> timestamps = new ArrayList<>(1);
            timestamps.add(timestamp);
            List<byte[]> partialMatch = new ArrayList<>(1);
            partialMatch.add(record);
            partialMatches.add(new PartialMatch(timestamps, partialMatch));
        }
        List<Tuple> ans = new ArrayList<>();

        for(int i = 1; i <buckets.size(); ++i){
            partialMatches = seqJoinUsingFollowByAny(pattern, partialMatches, buckets.get(i));
            if(partialMatches.size() == 0){
                return ans;
            }
        }

        for(PartialMatch tuple : partialMatches){
            Tuple t = new Tuple(buckets.size());
            for(byte[] record : tuple.matchList()){
                String event = schema.byteEventToString(record);
                t.addEvent(event);
            }
            ans.add(t);
        }
        return ans;
    }

    /**
     * process skip-till-any-match strategy
     * @param pattern query pattern
     * @param partialMatches previous partial results
     * @param bucket bucket
     * @return new partial results
     */
    public final List<PartialMatch> seqJoinUsingFollowByAny(EventPattern pattern, List<PartialMatch> partialMatches, List<byte[]> bucket){
        // ans
        List<PartialMatch> ans = new ArrayList<>();
        // load schema
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // Load the index corresponding to the timestamp of the attribute type array and sequence variable array
        String[] attrTypes = schema.getAttrTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int timeIdx = schema.getTimestampIdx();
        // tau is the maximum time constraint for query patterns
        long tau = pattern.getTau();
        // Number of partially matched matches that have already been matched
        int len = partialMatches.get(0).timeList().size();

        // dcList
        List<DependentConstraint> dcList = pattern.getContainIthVarDCList(len);

        int curPtr = 0;
        for (PartialMatch partialMatch : partialMatches) {
            // SEQ needs to use the timestamp of the previous record WITH needs to use the timestamp of the first record
            int curBucketSize = bucket.size();
            for (int i = curPtr; i < curBucketSize; ++i) {
                byte[] curRecord = bucket.get(i);
                long curTime = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));

                if (curTime < partialMatch.timeList().get(0)) {
                    curPtr++;
                } else if (curTime - partialMatch.timeList().get(0) > tau) {
                    // If the within condition is not met,
                    // the following ones will definitely not be met either
                    break;
                } else if (curTime >= partialMatch.timeList().get(len - 1)) {

                    byte[] lastRecord = partialMatch.matchList().get(len - 1);
                    boolean isEqual = Arrays.equals(curRecord, lastRecord);

                    if(!isEqual){
                        boolean satisfy = true;
                        if (dcList != null && dcList.size() != 0) {
                            for (DependentConstraint dc : dcList) {
                                String attrName = dc.getAttrName();
                                // Find the index corresponding to the attribute name,
                                // and then determine whether the type is passed into DC for comparison to meet the conditions
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = seqVarNames[len].equals(dc.getVarName1());
                                String cmpVarName = isVarName1 ? dc.getVarName2() : dc.getVarName1();
                                int cmpVarIdx = pattern.getVarNamePos(cmpVarName);

                                byte[] cmpRecord = partialMatch.matchList().get(cmpVarIdx);

                                long curValue;
                                long cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                    cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    // Because the magnification has already been increased during storage,
                                    // there is no need to increase the magnification here
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

                        if (satisfy) {
                            List<Long> timeList = new ArrayList<>(partialMatch.timeList());
                            timeList.add(curTime);

                            List<byte[]> matchList = new ArrayList<>(partialMatch.matchList());
                            matchList.add(curRecord);

                            ans.add(new PartialMatch(timeList, matchList));
                        }
                    }
                }
            }
        }

        return ans;
    }

    /**
     * process skip-till-next-match strategy
     * @param pattern query pattern
     * @param partialMatches previous partial results
     * @param bucket bucket
     * @return new partial results
     */
    public final List<PartialMatch> seqJoinUsingFollowBy(EventPattern pattern, List<PartialMatch> partialMatches, List<byte[]> bucket){
        // ans
        List<PartialMatch> ans = new ArrayList<>();
        // load schema
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(pattern.getSchemaName());
        // Load the index corresponding to the timestamp of the attribute type array and sequence variable array
        String[] attrTypes = schema.getAttrTypes();
        String[] seqVarNames = pattern.getSeqVarNames();
        int timeIdx = schema.getTimestampIdx();
        // tau is the maximum time constraint for query patterns
        long tau = pattern.getTau();
        // Number of partially matched matches that have already been matched
        int len = partialMatches.get(0).timeList().size();

        // dcList
        List<DependentConstraint> dcList = pattern.getContainIthVarDCList(len);

        int curPtr = 0;
        for (PartialMatch partialMatch : partialMatches) {
            // SEQ needs to use the timestamp of the previous record WITH needs to use the timestamp of the first record
            int curBucketSize = bucket.size();
            for (int i = curPtr; i < curBucketSize; ++i) {
                byte[] curRecord = bucket.get(i);
                long curTime = Converter.bytesToLong(schema.getIthAttrBytes(curRecord, timeIdx));

                if (curTime < partialMatch.timeList().get(0)) {
                    curPtr++;
                } else if (curTime - partialMatch.timeList().get(0) > tau) {
                    // If the within condition is not met,
                    // the following ones will definitely not be met either
                    break;
                } else if (curTime >= partialMatch.timeList().get(len - 1)) {
                    // the reason add this segment is to process
                    // SEQ(Type1 v0, Type1 v1)
                    byte[] lastRecord = partialMatch.matchList().get(len - 1);
                    boolean isEqual = Arrays.equals(curRecord, lastRecord);

                    if(!isEqual){
                        boolean satisfy = true;
                        if (dcList != null && dcList.size() != 0) {
                            for (DependentConstraint dc : dcList) {
                                String attrName = dc.getAttrName();
                                // Find the index corresponding to the attribute name,
                                // and then determine whether the type is passed into DC for comparison to meet the conditions
                                int idx = schema.getAttrNameIdx(attrName);
                                boolean isVarName1 = seqVarNames[len].equals(dc.getVarName1());
                                String cmpVarName = isVarName1 ? dc.getVarName2() : dc.getVarName1();
                                int cmpVarIdx = pattern.getVarNamePos(cmpVarName);

                                byte[] cmpRecord = partialMatch.matchList().get(cmpVarIdx);

                                long curValue;
                                long cmpValue;
                                if (attrTypes[idx].equals("INT")) {
                                    curValue = Converter.bytesToInt(schema.getIthAttrBytes(curRecord, idx));
                                    cmpValue = Converter.bytesToInt(schema.getIthAttrBytes(cmpRecord, idx));
                                } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                    // Because the magnification has already been increased during storage,
                                    // there is no need to increase the magnification here
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

                        if (satisfy) {
                            List<Long> timeList = new ArrayList<>(partialMatch.timeList());
                            timeList.add(curTime);

                            List<byte[]> matchList = new ArrayList<>(partialMatch.matchList());
                            matchList.add(curRecord);

                            ans.add(new PartialMatch(timeList, matchList));

                            // skip till next match
                            // once satisfy, then must match
                            break;
                        }
                    }
                }
            }
        }

        return ans;
    }

}
