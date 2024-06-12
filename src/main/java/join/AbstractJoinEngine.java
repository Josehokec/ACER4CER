package join;

import common.EventPattern;

import java.util.List;

public abstract class AbstractJoinEngine {

    /**
     * followBy = skip-till-next-match
     * event is byte array, not string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * followBy = skip-till-next-match
     * event is byte array, not string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingFollowBy(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * followByAny = skip-till-any-match
     * event is byte array, not string
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          number of matched tuples -> count(*)
     */
    public abstract int countTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets);

    /**
     * followByAny = skip-till-any-match
     * record is byte array
     * @param pattern   query pattern
     * @param buckets   buckets, each bucket stores byte records
     * @return          matched tuples
     */
    public abstract List<Tuple> getTupleUsingFollowByAny(EventPattern pattern, List<List<byte[]>> buckets);
}
