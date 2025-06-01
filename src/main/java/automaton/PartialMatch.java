package automaton;

import common.EventSchema;

import java.util.Arrays;
import java.util.List;

/**
 * define a partial match
 * a partial match contains a or more event record (byte value)
 */
public class PartialMatch {
    private final long startTime;
    private long endTime;
    // List<Event> events;
    private final List<Integer> recordPointers;

    PartialMatch(long startTime, long endTime, List<Integer> eventPointers){
        this.startTime = startTime;
        this.endTime = endTime;
        this.recordPointers = eventPointers;
    }

    public long getStartTime(){
        return startTime;
    }

    public long getEndTime(){
        return endTime;
    }

    public void setEndTime(long endTime){
        this.endTime = endTime;
    }

    public int getFirstEventPointer(){
        return recordPointers.get(0);
    }

    public int getLastEventPointer(){
        return recordPointers.get(recordPointers.size() - 1);
    }

    public List<Integer> getRecordPointers(){
        return recordPointers;
    }

    public int getPointer(int index){
        return recordPointers.get(index);
    }

    public String getSingleMatchedResult(EventCache eventCache, EventSchema schema){
        // -> Matched tuples:
        StringBuilder result  = new StringBuilder("|");
        for (Integer recordPointer : recordPointers) {
            byte[] record = eventCache.get(recordPointer);
            result.append(schema.byteEventToString(record));
            result.append("|");
        }
        return result.toString();
    }

    @Override
    public String toString(){
        return recordPointers.toString();
    }
}
