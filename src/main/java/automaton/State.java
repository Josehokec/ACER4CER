package automaton;


import common.Converter;
import common.EventSchema;
import condition.DependentConstraint;

import java.util.*;

/**
 * expect start state, each state has a partial match buffer
 */

public class State {
    private final String stateName;                 // state name
    private final int stateId;                      // stateId can unique represent a state
    private final List<Transition> transitions;     // edges / transitions
    private PartialMatchList partialMatchList;    // partial match buffer
    private boolean isFinal;                        // final state
    private boolean isStart;                        // start state

    public State(String stateName, int stateId){
        this.stateName = stateName;
        this.stateId = stateId;
        transitions = new ArrayList<>();
        partialMatchList = null;
        isFinal = false;
        isStart = false;
    }

    public boolean getIsFinal(){
        return isFinal;
    }

    public void setFinal() { isFinal = true; }

    public void setStart(){
        isStart = true;
    }

    public String getStateName(){
        return stateName;
    }

    public int getStateId(){
        return stateId;
    }

    public List<Transition> getTransactions() { return transitions; }

    public void bindTransaction(Transition transition){
        transitions.add(transition);
    }

    public void bindBuffer(PartialMatchList partialMatchList){
        this.partialMatchList = partialMatchList;
    }

    // we need ptr
    public Set<State> transfer(EventCache cache, byte[] record, long window, MatchStrategy matchStrategy, EventSchema schema, boolean hasInserted){
        Set<State> nextStates = new HashSet<>();
        long timestamp = schema.getTimestampFromRecord(record);
        String[] attrTypes = schema.getAttrTypes();

        int recordPointer = -1;

        // for each transaction
        for(Transition transition : transitions){
            // first check independent constraints
            if(transition.checkIC(schema, record)){
                State nextState = transition.getNextState();
                boolean hasTransition = false;
                // here we need to judge whether current state whether is start state
                // if yes, then we directly add this record to next state's match buffer
                // otherwise, we need to check dependent predicates
                if(isStart){
                    if(hasInserted){
                        recordPointer = cache.getLastRecordPtr();
                    }else{
                        recordPointer = cache.insert(record);
                        hasInserted = true;
                    }
                    // this state is start state, we need to generate a partial match
                    List<Integer> eventPointers = new ArrayList<>(4);
                    eventPointers.add(recordPointer);
                    // generate a partial match, start time and end time is record's timestamp
                    PartialMatch match = new PartialMatch(timestamp, timestamp, eventPointers);
                    // add the partial match to match buffer
                    PartialMatchList nextMatchCache =  nextState.getPartialMatchList();
                    if(nextMatchCache == null){
                        // create a buffer and bind to a state
                        List<String> stateNames = new ArrayList<>();
                        stateNames.add(nextState.stateName);
                        nextMatchCache = new PartialMatchList(stateNames);
                        nextState.bindBuffer(nextMatchCache);
                    }
                    nextMatchCache.addPartialMatch(match);
                    hasTransition = true;
                }
                else{
                    List<PartialMatch> curPartialMatches = partialMatchList.getPartialMatchList();
                    Iterator<PartialMatch> it = curPartialMatches.iterator();

                    while(it.hasNext()){
                        PartialMatch curMatch = it.next();
                        long matchStartTime = curMatch.getStartTime();
                        boolean timeout = timestamp - matchStartTime > window;
                        //if timeout we need to remove this partial match
                        if(timeout){
                            it.remove();
                        }
                        // please note that timestamp is non-decreasing
                        else{
                            boolean satisfyAllDC = true;
                            for(DependentConstraint dc : transition.getDCList()){
                                String varName1 = dc.getVarName1();
                                String varName2 = dc.getVarName2();
                                // we only read int/float/double/long values
                                long value1, value2;
                                int idx = schema.getAttrNameIdx(dc.getAttrName());
                                if(varName1.equals(nextState.getStateName())){
                                    // find varName2 position
                                    int pos = partialMatchList.findStateNamePosition(varName2);
                                    //if(pos == -1){throw new RuntimeException("bug");}
                                    byte[] record2 = cache.get(curMatch.getPointer(pos));
                                    if (attrTypes[idx].equals("INT")) {
                                        value2 = Converter.bytesToInt(schema.getIthAttrBytes(record2, idx));
                                        value1 = Converter.bytesToInt(schema.getIthAttrBytes(record, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // Because the magnification has already been increased during storage,
                                        // there is no need to increase the magnification here
                                        value2 = Converter.bytesToLong(schema.getIthAttrBytes(record2, idx));
                                        value1 = Converter.bytesToLong(schema.getIthAttrBytes(record, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }

                                    if(!dc.satisfy(value1, value2)){
                                        satisfyAllDC = false;
                                        break;
                                    }
                                }
                                else if(varName2.equals(nextState.getStateName())){
                                    // find varName1 position
                                    int pos = partialMatchList.findStateNamePosition(varName1);
                                    //if(pos == -1){throw new RuntimeException("bug");}
                                    byte[] record1 = cache.get(curMatch.getPointer(pos));
                                    if (attrTypes[idx].equals("INT")) {
                                        value1 = Converter.bytesToInt(schema.getIthAttrBytes(record1, idx));
                                        value2 = Converter.bytesToInt(schema.getIthAttrBytes(record, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        value1 = Converter.bytesToLong(schema.getIthAttrBytes(record1, idx));
                                        value2 = Converter.bytesToLong(schema.getIthAttrBytes(record, idx));
                                    } else {
                                        throw new RuntimeException("Wrong index position.");
                                    }

                                    if(!dc.satisfy(value1, value2)){
                                        satisfyAllDC = false;
                                        break;
                                    }
                                }
                                else{
                                    throw new RuntimeException("bug");
                                }
                            }

                            if(satisfyAllDC){
                                if(matchStrategy == MatchStrategy.SKIP_TILL_NEXT_MATCH){
                                    it.remove();
                                }else if(matchStrategy == MatchStrategy.STRICT_CONTIGUOUS){
                                    throw new RuntimeException("currently, we do not implement STRICT_CONTIGUOUS interface");
                                }

                                if(hasInserted){
                                    recordPointer = cache.getLastRecordPtr();
                                }else{
                                    recordPointer = cache.insert(record);
                                    hasInserted = true;
                                }

                                // create a match and add it to next buffer
                                List<Integer> newEventPointers = new ArrayList<>(curMatch.getRecordPointers());
                                newEventPointers.add(recordPointer);
                                PartialMatch match = new PartialMatch(curMatch.getStartTime(), timestamp, newEventPointers);
                                PartialMatchList nextCache = nextState.getPartialMatchList();
                                if(nextCache == null){
                                    // create a buffer and bind to a state
                                    List<String> stateNames = new ArrayList<>(partialMatchList.getStateNames());
                                    stateNames.add(nextState.getStateName());
                                    nextCache = new PartialMatchList(stateNames);
                                    nextState.bindBuffer(nextCache);
                                }
                                nextCache.addPartialMatch(match);
                                hasTransition = true;
                            }
                        }
                    }
                }

                if(hasTransition){
                    nextStates.add(nextState);
                }
            }
        }

        return nextStates;
    }

    public PartialMatchList getPartialMatchList(){
        return partialMatchList;
    }

    public String toString(){
        return " [stateId: " + stateId +
                ", stateName: " + stateName +
                ", isStart: " + isStart +
                ", isFinal: " + isFinal +
                ", transitionNum: " + transitions.size() + "]";
    }

    public void recursiveDisplayState(){
        System.out.println("$ state information: " + this);
        if(!isFinal){
            for(Transition t : transitions){
                t.print();
                State nextState = t.getNextState();
                nextState.recursiveDisplayState();
            }
        }
    }
}


/*
// [remove bug...] avoid same events...
if(hasInserted && curMatch.getLastEventPointer() == cache.getLastRecordPtr()){
    continue;
}
 */

