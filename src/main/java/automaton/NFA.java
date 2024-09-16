package automaton;

import common.*;
import condition.DependentConstraint;
import condition.IndependentConstraint;
import common.Tuple;
import pattern.DecomposeUtils;
import pattern.QueryPattern;

import java.util.*;

/**
 * Some key claims:
 * Currently, this automata does not support the kleene operator and negation operator.
 * This is a simplified automata, thus it has a fast process speed.
 * If you need to process more kleene operator and negation operator,
 * please choose FlinkCEP or OpenCEP (https://github.com/ilya-kolchinsky/OpenCEP).
 * ACER aims to provide filtered events to speedup complex event query,
 * thus we do not care detailed automata
 */
public class NFA {
    private int stateNum;                           // number of states
    private HashMap<Integer, State> stateMap;       // all states
    private long queryWindow;                       // query window condition
    private  List<State> activeStates;              // active states

    public NFA(){
        stateNum = 0;
        stateMap = new HashMap<>();
        activeStates = new ArrayList<>();
        queryWindow = Long.MAX_VALUE;
        State startState = createState("start", true, false);
        activeStates.add(startState);
    }

    /**
     * control this function to generate state
     * @param stateName     state name
     * @param isStart       mark start state
     * @param isFinal       mark final state
     */
    public State createState(String stateName, boolean isStart, boolean isFinal){
        // when create a new state, we need its state name and the length of a match
        State state = new State(stateName, stateNum);

        if(isStart){
            state.setStart();
        }
        if(isFinal){
            state.setFinal();
        }

        // store state, for start state its stateNum is 0
        stateMap.put(stateNum, state);
        stateNum++;
        return state;
    }

    /**
     * according to stateName find target state list
     * @param stateName state name
     * @return all states whose name is stateName
     */
    public List<State> getState(String stateName){
        // AND(Type1 a, Type2 b) -> a and b has two states
        List<State> states = new ArrayList<>();
        for (State state : stateMap.values()) {
            if(stateName.equals(state.getStateName())){
                states.add(state);
            }
        }

        return states;
    }

    public List<State> getFinalStates(){
        List<State> finalStates = new ArrayList<>();
        for(State state : stateMap.values()){
            if(state.getIsFinal()){
                finalStates.add(state);
            }
        }
        return finalStates;
    }

    public State getStateUsingStateId(int id){
        if(stateMap.containsKey(id)){
            return stateMap.get(id);
        }else{
            System.out.println("do not contain this state id ");
            return null;
        }
    }

    public void printMatch(EventSchema schema){
        System.out.println("Match results:");
        List<State> finalStates = getFinalStates();
        for(State state : finalStates){
            //System.out.println(state);
            PartialMatchBuffer buffer = state.getBuffer();
            if(buffer != null){
                int len = buffer.getLength();
                for(PartialMatch match : buffer.getPartialMatches()){
                    for(int i = 0; i < len; ++i){
                        byte[] eventRecord = match.matchList().get(i);
                        String event = schema.byteEventToString(eventRecord);
                        System.out.print(event + " ");
                    }
                    System.out.println();
                }
            }
        }
    }

    public void generateNFAUsingQueryPattern(QueryPattern pattern){
        this.queryWindow = pattern.getTau();

        if(pattern.onlyContainSEQ){
            // e.g., PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
            String[] seqStatement = pattern.getPatternStr().split("[()]");
            // seqEvent = "IBM a, Oracle b, IBM c, Oracle d"
            String[] seqEvent = seqStatement[1].split(",");
            String[] varNames = new String[seqEvent.length];
            String[] eventTypes = new String[seqEvent.length];

            for(int i = 0; i < seqEvent.length; ++i){
                String[] s = seqEvent[i].trim().split(" ");
                eventTypes[i] = s[0];
                varNames[i] = s[1].trim();
            }

            int varNum = varNames.length;

            // create all states
            for(int i = 0; i < varNum - 1; ++i){
                createState(varNames[i], false, false);
            }
            createState(varNames[varNum - 1], false, true);

            Set<String> preVarName = new HashSet<>();
            // add all transaction
            for(int i = 0; i < varNum; ++i){
                String curVarName = varNames[i];
                List<IndependentConstraint> icList = pattern.getICListUsingVarName(curVarName);
                List<DependentConstraint> dcList = pattern.getDC(preVarName, curVarName);
                addTransaction(stateMap.get(i), stateMap.get(i + 1), eventTypes[i], icList, dcList);
                preVarName.add(curVarName);
            }
        }else{
            String patternStr = pattern.getPatternStr().substring(8);
            List<String> seqQueries = DecomposeUtils.decomposingCEP(patternStr);

            for(String query : seqQueries){
                // e.g., PATTERN SEQ(IBM a, Oracle b, IBM c, Oracle d)
                String[] seqStatement = query.split("[()]");
                // seqEvent = "IBM a, Oracle b, IBM c, Oracle d"
                String[] seqEvent = seqStatement[1].split(",");
                String[] varNames = new String[seqEvent.length];
                String[] eventTypes = new String[seqEvent.length];

                for(int i = 0; i < seqEvent.length; ++i){
                    String[] s = seqEvent[i].trim().split(" ");
                    eventTypes[i] = s[0];
                    varNames[i] = s[1].trim();
                }

                int varNum = varNames.length;

                List<State> createStates = new ArrayList<>(varNum);

                // create all states
                if(varNum == 1){
                    State curState = createState(varNames[0], false, true);
                    createStates.add(curState);
                }else{
                    for(int i = 0; i < varNum - 1; ++i){
                        State curState = createState(varNames[i], false, false);
                        createStates.add(curState);
                    }
                    // final state
                    State curState = createState(varNames[varNum - 1], false, true);
                    createStates.add(curState);
                }

                Set<String> preVarName = new HashSet<>();
                // add all transactions
                State preState = stateMap.get(0);
                for(int i = 0; i < varNum; ++i){
                    String curVarName = varNames[i];
                    List<IndependentConstraint> icList = pattern.getICListUsingVarName(curVarName);
                    List<DependentConstraint> dcList = pattern.getDC(preVarName, curVarName);
                    State curState = createStates.get(i);
                    addTransaction(preState, curState, eventTypes[i], icList, dcList);
                    preState = curState;
                    preVarName.add(curVarName);
                }
            }
        }


    }

    public void addTransaction(State curState, State nextState, String nextEventType,
                               List<IndependentConstraint> icList, List<DependentConstraint> dcList){
        Transaction transaction = new Transaction(nextEventType, icList, dcList, nextState);
        curState.bindTransaction(transaction);
    }

    /**
     * NFA consume an event
     * for each active state, judge whether it can transact next state
     * @param schema        event schema
     * @param eventRecord   event
     * @param matchStrategy skip-till-any-match or skip-till-next-match
     */
    public void consume(EventSchema schema, byte[] eventRecord, MatchStrategy matchStrategy){
        // to speedup match, we use time window to delete can not match tuples
        Set<State> allNextStates = new HashSet<>();
        for(State state : activeStates){
            // final state cannot transact
            if(!state.getIsFinal()){
                // using match strategy
                List<State> nextStates = state.transact(schema, eventRecord, queryWindow, matchStrategy);
                allNextStates.addAll(nextStates);
            }
        }

        // add start state
        State startState = stateMap.get(0);
        allNextStates.add(startState);

        // update active state
        activeStates = new ArrayList<>(allNextStates);
        // debug
        // printActiveStates();
    }

    /**
     * default is skip till any match
     * @param schema        event schema
     * @return              tuple list
     */
    public List<Tuple> getTuple(EventSchema schema){
        List<Tuple> ans = new ArrayList<>();
        // find results from final states
        for(State state : stateMap.values()){
            if(state.getIsFinal()){
                PartialMatchBuffer buff = state.getBuffer();
                if(buff != null){
                    List<PartialMatch> fullMatches = buff.getPartialMatches();
                    if(!fullMatches.isEmpty()){
                        int size = fullMatches.get(0).matchList().size();
                        for(PartialMatch fullMatch : fullMatches){
                            Tuple t = new Tuple(size);
                            for(byte[] record : fullMatch.matchList()){
                                String event = schema.byteEventToString(record);
                                t.addEvent(event);
                            }
                            ans.add(t);
                        }
                    }
                }
            }
        }
        return ans;
    }
    public int countTuple(){
        int cnt = 0;
        // find results from final states
        for(State state : stateMap.values()){
            if(state.getIsFinal()){
                PartialMatchBuffer buff = state.getBuffer();
                if(buff != null){
                    List<PartialMatch> fullMatches = buff.getPartialMatches();
                    if(!fullMatches.isEmpty()){
                        cnt += fullMatches.size();
                    }
                }
            }
        }
        return  cnt;
    }

    public void printActiveStates(){
        for(State state : activeStates){
            System.out.println(state);
        }
    }

    public void displayNFA(){
        State initialState = stateMap.get(0);
        System.out.println("--------All NFA paths-------");
        int cnt = 1;
        for(Transaction t : initialState.getTransactions()){
            System.out.println("path number: " + cnt);
            System.out.println("start state: " + initialState);
            cnt++;
            t.print();
            State nextState = t.getNextState();
            nextState.recursiveDisplayState();
        }

    }
}
