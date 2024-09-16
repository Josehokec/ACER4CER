package automaton;


import common.Converter;
import common.EventSchema;
import common.MatchStrategy;
import condition.DependentConstraint;
import common.PartialMatch;

import java.util.*;

/**
 * expect start state, each state has a partial match buffer
 */

public class State {
    private final String stateName;                 // state name
    private final int stateId;                      // stateId can unique represent a state
    private final List<Transaction> transactions;   // edges / transactions
    private PartialMatchBuffer buffer;              // partial match buffer
    private boolean isFinal;                        // final state
    private boolean isStart;                        // start state

    public State(String stateName, int stateId){
        this.stateName = stateName;
        this.stateId = stateId;
        transactions = new ArrayList<>();
        buffer = null;
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

    public List<Transaction> getTransactions() { return transactions; }

    public void bindTransaction(Transaction transaction){
        transactions.add(transaction);
    }

    public void bindBuffer(PartialMatchBuffer buffer){
        this.buffer = buffer;
    }

    public List<State> transact(EventSchema schema, byte[] eventRecord, long timeWindows, MatchStrategy matchStrategy){
        Set<State> nextStates = new HashSet<>();
        // current state always need to keep
        nextStates.add(this);
        int timeIdx = schema.getTimestampIdx();
        long timestamp = Converter.bytesToLong(schema.getIthAttrBytes(eventRecord, timeIdx));
        String[] attrTypes = schema.getAttrTypes();

        // for each transaction
        for(Transaction transaction :transactions){
            // first check independent constraints
            if(transaction.checkIC(schema, eventRecord)){
                State nextState = transaction.getNextState();
                // 2. then update buffer
                if(isStart){
                    // if this state is start state, then we generate a partial match
                    List<Long> timeList = new ArrayList<>(1);
                    List<byte[]> matchList = new ArrayList<>(1);
                    timeList.add(timestamp);
                    matchList.add(eventRecord);
                    PartialMatch match = new PartialMatch(timeList, matchList);

                    PartialMatchBuffer nextBuffer =  nextState.getBuffer();

                    if(nextBuffer == null){
                        // create a buffer and bind to a state
                        List<String> stateNames = new ArrayList<>();
                        stateNames.add(nextState.stateName);
                        nextBuffer = new PartialMatchBuffer(1, stateNames);
                        nextState.bindBuffer(nextBuffer);
                    }
                    nextBuffer.addPartialMatch(match);
                    nextStates.add(nextState);
                }
                else{
                    List<PartialMatch> curPartialMatches = buffer.getPartialMatches();

                    Iterator<PartialMatch> it = curPartialMatches.iterator();
                    while(it.hasNext()){
                        PartialMatch curMatch = it.next();
                        long matchStartTimestamp = curMatch.timeList().get(0);
                        boolean timeout = timestamp - matchStartTimestamp > timeWindows;

                        // 1. check time window whether timeout
                        // 2. check dependent constraints
                        if(timeout){
                            it.remove();
                        }
                        else{
                            boolean satisfyAllDC = true;
                            for(DependentConstraint dc : transaction.getDCList()){
                                String varName1 = dc.getVarName1();
                                String varName2 = dc.getVarName2();

                                long value1, value2;

                                int idx = schema.getAttrNameIdx(dc.getAttrName());

                                if(varName1.equals(nextState.getStateName())){
                                    // find varName2 position
                                    int pos = buffer.findStateNamePosition(varName2);
                                    if(pos == -1){
                                        throw new RuntimeException("bug");
                                    }
                                    byte[] record2 = curMatch.matchList().get(pos);

                                    if (attrTypes[idx].equals("INT")) {
                                        value2 = Converter.bytesToInt(schema.getIthAttrBytes(record2, idx));
                                        value1 = Converter.bytesToInt(schema.getIthAttrBytes(eventRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // Because the magnification has already been increased during storage,
                                        // there is no need to increase the magnification here
                                        value2 = Converter.bytesToLong(schema.getIthAttrBytes(record2, idx));
                                        value1 = Converter.bytesToLong(schema.getIthAttrBytes(eventRecord, idx));
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
                                    int pos = buffer.findStateNamePosition(varName1);
                                    if(pos == -1){
                                        throw new RuntimeException("bug");
                                    }
                                    byte[] record1 = curMatch.matchList().get(pos);
                                    if (attrTypes[idx].equals("INT")) {
                                        value1 = Converter.bytesToInt(schema.getIthAttrBytes(record1, idx));
                                        value2 = Converter.bytesToInt(schema.getIthAttrBytes(eventRecord, idx));
                                    } else if (attrTypes[idx].contains("FLOAT") || attrTypes[idx].contains("DOUBLE")) {
                                        // Because the magnification has already been increased during storage,
                                        // there is no need to increase the magnification here
                                        value1 = Converter.bytesToLong(schema.getIthAttrBytes(record1, idx));
                                        value2 = Converter.bytesToLong(schema.getIthAttrBytes(eventRecord, idx));
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
                                    System.out.println("We do not support this match strategy");
                                }
                                // create a match and add it to next buffer
                                List<Long> timeList = new ArrayList<>(curMatch.timeList());
                                timeList.add(timestamp);
                                List<byte[]> matchList = new ArrayList<>(curMatch.matchList());
                                matchList.add(eventRecord);
                                PartialMatch match = new PartialMatch(timeList, matchList);

                                PartialMatchBuffer nextBuffer =  nextState.getBuffer();

                                if(nextBuffer == null){
                                    // create a buffer and bind to a state
                                    List<String> stateNames = new ArrayList<>(buffer.getStateNames());
                                    stateNames.add(nextState.getStateName());
                                    int len = stateNames.size();
                                    nextBuffer = new PartialMatchBuffer(len, stateNames);
                                    nextState.bindBuffer(nextBuffer);
                                }
                                nextBuffer.addPartialMatch(match);
                                nextStates.add(nextState);
                            }
                        }
                    }
                }
            }
        }

        return new ArrayList<>(nextStates);
    }

    public PartialMatchBuffer getBuffer(){
        return buffer;
    }

    public String toString(){
        return " | stateId: " + stateId +
                " | stateName: " + stateName +
                " | isStart: " + isStart +
                " | isFinal: " + isFinal +
                " | transactionNum: " + transactions.size() + " |";
    }

    public void recursiveDisplayState(){
        System.out.println("current state information: ");
        System.out.println(this);
        if(!isFinal){
            for(Transaction t : transactions){
                t.print();
                State nextState = t.getNextState();
                nextState.recursiveDisplayState();
            }
        }
    }
}
