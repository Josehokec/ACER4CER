package common;

import java.util.ArrayList;
import java.util.List;

/**
 * Matched String tuple
 */
public class Tuple {
    private final List<String> fullMatch;

    public Tuple(int size){
        fullMatch = new ArrayList<>(size);
    }

    public void addEvent(String eventRecord){
        fullMatch.add(eventRecord);
    }

    public void addAllEvents(List<String> records){
        fullMatch.addAll(records);
    }

    public String projectionExclude(int pos){
        StringBuilder ans = new StringBuilder();
        for(int i = 0; i < fullMatch.size(); ++i){
            if(i != pos){
                ans.append(fullMatch.get(i));
            }
        }
        return ans.toString();
    }

    public String getKey(){
        StringBuilder key = new StringBuilder();
        for(String event : fullMatch){
            key.append(event);
        }
        return key.toString();
    }


    @Override
    public String toString(){
        StringBuilder ans = new StringBuilder(512);
        ans.append("[");
        int len = fullMatch.size();
        for(int i = 0; i < len - 1; ++i){
            ans.append(fullMatch.get(i)).append("|");
        }
        ans.append(fullMatch.get(len - 1)).append("]");
        return ans.toString();
    }
}
