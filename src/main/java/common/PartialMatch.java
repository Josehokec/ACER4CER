package common;

import java.util.List;

/**
 * define a partial match
 * a partial match contains a or more event record (byte value)
 * to reduce decode time (byte -> timestamp)
 * here we store the event timestamp
 * @param timeList
 * @param matchList
 */
public record PartialMatch(List<Long> timeList, List<byte[]> matchList){ }
