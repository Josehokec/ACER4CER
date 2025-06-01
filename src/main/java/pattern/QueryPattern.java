package pattern;

import automaton.MatchStrategy;
import condition.DependentConstraint;
import condition.IndependentConstraint;

import java.util.*;

public abstract class QueryPattern {
    public boolean onlyContainSEQ;                                      // mark whether only contain SEQ operator
    protected String patternStr;                                        // pattern
    protected int patternLen;                                           // a tuple that contain max event number
    protected int variableNum;                                          // number of variable
    protected String schemaName;                                        // schema name
    protected long tau;                                                 // query time window
    protected MatchStrategy strategy;                                   // strategy
    protected String returnStr;                                         // output format
    protected HashMap<String, List<IndependentConstraint>> icMap;       // independent constraint list
    protected List<DependentConstraint> dcList;                         // dependent constraint list
    protected Map<String, String> varTypeMap;                           // variable name -> event type

    public QueryPattern(String patternStr){
        this.patternStr = patternStr;
        // parse use reference, so we need to initialize
        icMap = new HashMap<>();
        dcList = new ArrayList<>();
        varTypeMap = new HashMap<>();
    }

    // below we define abstract methods

    /**
     * query statement first line example 1:
     * PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
     * query statement first line example 2:
     * PATTERN SEQ(AND(ROBBERY v0, BATTERY v1), MOTOR_VEHICLE_THEFT v2)
     * @param firstLine  query statement firstLine
     */
    public abstract void parse(String firstLine);
    public abstract boolean isOnlyLeftMostNode(String varName);
    public abstract boolean isOnlyRightMostNode(String varName);

    public abstract void print();

    public List<DependentConstraint> getDC(Set<String> preVarNames, String curVarName){
        List<DependentConstraint> ans = new ArrayList<>();
        for(DependentConstraint dc : dcList){
            String varName1= dc.getVarName1();
            String varName2 = dc.getVarName2();
            if(curVarName.equals(varName1) && preVarNames.contains(varName2)){
                ans.add(dc);
            }else if(curVarName.equals(varName2) && preVarNames.contains(varName1)){
                ans.add(dc);
            }
        }
        return ans;
    }

    // below we define setter and getter methods
    public void setSchemaName(String schemaName){ this.schemaName = schemaName;}

    public void setTau(long tau){ this.tau = tau; }

    public void setStrategy(String matchStrategy){
        switch (matchStrategy) {
            case "STRICT_CONTIGUOUS" -> {
                strategy = MatchStrategy.STRICT_CONTIGUOUS;
                System.out.println("Here we do not implement this strategy. So we reset this strategy to SKIP_TILL_ANY_MATCH");
                strategy = MatchStrategy.SKIP_TILL_ANY_MATCH;
            }
            case "SKIP_TILL_NEXT_MATCH" -> strategy = MatchStrategy.SKIP_TILL_NEXT_MATCH;
            case "SKIP_TILL_ANY_MATCH" -> strategy = MatchStrategy.SKIP_TILL_ANY_MATCH;
            default -> {
                System.out.println("Do not support match strategy '" + matchStrategy + "'" +
                        ", so we set default strategy is SKIP_TILL_NEXT_MATCH");
                strategy = MatchStrategy.SKIP_TILL_ANY_MATCH;
            }
        }
    }

    public void setReturnStr(String returnStr){ this.returnStr = returnStr; }

    public List<IndependentConstraint> getICListUsingVarName(String varName){
        return icMap.getOrDefault(varName, new ArrayList<>());
    }

    public List<DependentConstraint> getDcList() { return dcList; }

    @SuppressWarnings("unused")
    public int getVariableNum(){ return variableNum; }

    @SuppressWarnings("unused")
    public String getEventType(String varName){
        return varTypeMap.get(varName);
    }

    public long getTau(){ return tau; }

    public String getSchemaName() {return schemaName;}

    public MatchStrategy getStrategy(){ return strategy; }

    public String getReturnStr() { return returnStr; }

    public boolean existOROperator(){
        return patternStr.contains("OR(");
    }

    public HashMap<String, List<IndependentConstraint>> getIcMap(){ return icMap; }

    public Map<String, String> getVarTypeMap() { return varTypeMap; }

    public void getAllVarType(String[] varNames, String[] eventTypes){
        if(varNames.length != varTypeMap.size()){
            System.out.println("varNames size: " + varNames.length + ", map size: " + varTypeMap.size());
            throw new RuntimeException("length mismatch");
        }
        int cnt = 0;
        for(Map.Entry<String, String> entry : varTypeMap.entrySet()){
            varNames[cnt] = entry.getKey();
            eventTypes[cnt] = entry.getValue();
            cnt++;
        }
    }

    public String getPatternStr() { return patternStr; }
}