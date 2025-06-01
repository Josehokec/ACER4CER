package automaton;

import java.util.ArrayList;
import java.util.List;



public class PartialMatchList {
    private final List<String> stateNames;
    private final List<PartialMatch> partialMatchList;

    public PartialMatchList(List<String> stateNames){
        // this.length = length;
        this.stateNames = stateNames;
        partialMatchList = new ArrayList<>(1024);
    }

    public List<PartialMatch> getPartialMatchList(){
        return partialMatchList;
    }

    public int findStateNamePosition(String stateName){
        // below code is slow
        for(int i = 0; i < stateNames.size(); ++i){
            if(stateNames.get(i).equals(stateName)){
                return i;
            }
        }
        throw new RuntimeException("cannot find stateName: " + stateName);
    }

    public void addPartialMatch(PartialMatch match){
        partialMatchList.add(match);
    }

    public List<String> getStateNames(){
        return stateNames;
    }

    public int getPartialMatchSize(){
        return partialMatchList.size();
    }
}
