package automaton;

import common.EventSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * for skip-till-any-match strategy, it will generate many matches
 * and one event may appear in multiple matches, to reduce the space overhead
 * we need to use pointer to avoid frequently data copy
 * currently, We use reference techniques to reduce space overhead
 */
public class EventCache {
    private final List<byte[]> events;
    private int count;

    EventCache(){
        events = new ArrayList<>(512);
        count = 0;
    }

    public int getCount(){
        return count;
    }

    public int getLastRecordPtr(){
        return count - 1;
    }

    public int insert(byte[] event){
        events.add(event);
        return count++;
    }

//    public int insert(byte[] event, EventSchema schema){
//        System.out.println(schema.byteEventToString(event) + " id: " + count);
//        events.add(event);
//        return count++;
//    }


    public byte[] get(int index){
        return events.get(index);
    }

    public List<byte[]> getRecords(List<Integer> pointers){
        List<byte[]> ans = new ArrayList<>(pointers.size());
        for(int p : pointers){
            ans.add(events.get(p));
        }
        return ans;
    }

    public List<byte[]> getAllEvents(){
        return events;
    }
}
