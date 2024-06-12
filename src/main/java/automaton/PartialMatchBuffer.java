package automaton;

import common.PartialMatch;

import java.util.ArrayList;
import java.util.List;



public class PartialMatchBuffer {
    private int length;
    List<String> stateNames;
    public List<PartialMatch> partialMatches;

    public PartialMatchBuffer(int length, List<String> stateNames){
        this.length = length;
        this.stateNames = stateNames;

        if(length != stateNames.size()){
            throw new RuntimeException("bug");
        }

        partialMatches = new ArrayList<>();
    }

    public List<PartialMatch> getPartialMatches(){
        return partialMatches;
    }

    public int findStateNamePosition(String stateName){
        for(int i = 0; i < stateNames.size(); ++i){
            if(stateNames.get(i).equals(stateName)){
                return i;
            }
        }
        return -1;
    }

    public void addPartialMatch(PartialMatch match){
        partialMatches.add(match);
    }

    public List<String> getStateNames(){
        return stateNames;
    }

    public int getLength(){
        return length;
    }

    public String toString(){
        StringBuffer str = new StringBuffer();
        for(PartialMatch match : partialMatches){
            for(long ts : match.timeList()){
                str.append(ts).append(" ");
            }
            str.append("\n");
        }
        return str.toString();
    }
}
