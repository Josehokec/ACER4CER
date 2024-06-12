package common;

import condition.DependentConstraint;
import condition.IndependentConstraint;

import java.util.*;

/**
 * here we only support SEQ operator
 * We will support other operators (e.g., CON, DIS, NEQ and Kleene Operator) in the future
 */
public class EventPattern {
    private String[] seqEventTypes;                                     // sequential event types
    private String[] seqVarNames;                                       // sequential event variable name
    private String schemaName;                                          // schema name
    private long tau;                                                   // query time window
    private MatchStrategy strategy;                                     // match strategy
    private String returnStr;                                           // return statement
    private final Map<String, Integer> varMap;                          // <varName, position>
    private final HashMap<String, List<IndependentConstraint>> icMap;   // independent predicate constraints bound by each variable
    private List<DependentConstraint> dcList;                           // dependent predicate constraints list

    public EventPattern() {
        icMap = new HashMap<>();
        varMap = new HashMap<>();
        dcList = new ArrayList<>();
        strategy = MatchStrategy.SKIP_TILL_NEXT_MATCH;
    }

    public String[] getSeqEventTypes() {
        return seqEventTypes;
    }

    public void setSeqEventTypes(String[] seqEventTypes) {
        this.seqEventTypes = seqEventTypes;
    }

    public String[] getSeqVarNames() {
        return seqVarNames;
    }

    public void setSeqVarNames(String[] seqVarNames) {
        this.seqVarNames = seqVarNames;
        for(int i = 0; i < seqVarNames.length; ++i){
            varMap.put(seqVarNames[i], i);
        }
    }

    public boolean onlyLeftMostVar(String varName){
        return seqVarNames[0].equals(varName);
    }

    public boolean onlyRightMostVar(String varName){
        return seqVarNames[seqVarNames.length - 1].equals(varName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public HashMap<String, List<IndependentConstraint>> getIcMap() {
        return icMap;
    }

    /**
     * get $varName$ independent predicate constraints
     * @param varName variable name
     * @return independent predicate constraints list
     */
    public List<IndependentConstraint> getICListUsingVarName(String varName){
        if(icMap.containsKey(varName)){
            return icMap.get(varName);
        }else{
            return new ArrayList<>();
        }
    }

    public List<DependentConstraint> getDcList() {
        return dcList;
    }

    public long getTau() {
        return tau;
    }

    public void setTau(long tau) {
        this.tau = tau;
    }

    public MatchStrategy getStrategy(){
        return strategy;
    }

    public void setStrategy(String s){
        switch (s) {
            case "STRICT_CONTIGUOUS" -> strategy = MatchStrategy.STRICT_CONTIGUOUS;
            case "SKIP_TILL_NEXT_MATCH" -> strategy = MatchStrategy.SKIP_TILL_NEXT_MATCH;
            case "SKIP_TILL_ANY_MATCH" -> strategy = MatchStrategy.SKIP_TILL_ANY_MATCH;
            default -> System.out.println("Do not support match strategy '" + s + "'" +
                    ", we set default strategy is SKIP_TILL_NEXT_MATCH");
        }
    }

    @SuppressWarnings("unused")
    public String getReturnStr() {
        return returnStr;
    }

    public void setReturnStr(String returnStr) {
        this.returnStr = returnStr;
    }

    /**
     * this function used for NFA
     * use varName to get dependent constraints, and another variable name before than this varName
     * @param varName       query variable name
     * @return              dependent constraints
     */
    public List<DependentConstraint> getDC(String varName){
        List<DependentConstraint> ans = new ArrayList<>();
        for(DependentConstraint dc : dcList){
            String varName1 = dc.getVarName1();
            String varName2 = dc.getVarName2();

            if(varName1.equals(varName)){
                int idx2 = varMap.get(varName2);
                if(idx2 < varMap.get(varName)){
                    ans.add(dc);
                }
            }else if(varName2.equals(varName)){
                int idx1 = varMap.get(varName1);
                if(idx1 < varMap.get(varName)){
                    ans.add(dc);
                }
            }
        }
        return ans;
    }

    /**
     * sort the dependent predicate constraint
     */
    public void sortDC(){
        Comparator<DependentConstraint> cmp = (o1, o2) -> {
            int max1 = maxVarIdx(o1.getVarName1(), o1.getVarName2());
            int max2 = maxVarIdx(o2.getVarName1(), o2.getVarName2());
            return max1 - max2;
        };
        dcList.sort(cmp);
    }

    /**
     * get variable idx <br>
     * suppose pattern is (A a, B b, C c)<br>
     * then getVarNamePos(a) = 0; getVarNamePos(b) = 1; getVarNamePos(c) = 2;
     * @param varName variable name
     * @return position
     */
    public int getVarNamePos(String varName){
        Integer pos = varMap.get(varName);
        if(pos == null){
            throw new IllegalStateException("Variable name has error.");
        }
        return pos;
    }

    /**
     * get max variable name idx
     * suppose the pattern is "(A a, B b, C c)"
     * DependentConstraint $dc$ only have:  a.attr < b.attr
     * idx(a) = 0, idx(b) = 1, idx(c) = 2
     * getDCMaxNum(dc) = 1
     * @param dc dependent predicate constraint
     * @return max value
     */
    public int getDCMaxNum(DependentConstraint dc){
        return maxVarIdx(dc.getVarName1(), dc.getVarName2());
    }

    /**
     * get the DependentConstraint that contain i, the list of DependentConstraint is sorted
     * @param ith i-th variable name
     * @return dependent predicate constraint list
     */
    public List<DependentConstraint> getContainIthVarDCList(int ith){
        List<DependentConstraint> ans = new ArrayList<>();
        // complexity is O(N), maybe we can optimize this process
        for(DependentConstraint dc : dcList){
            if(getDCMaxNum(dc) == ith){
                ans.add(dc);
            }else if(getDCMaxNum(dc) > ith){
                // early break
                break;
            }
        }
        return ans;
    }

    public int maxVarIdx(String varName1, String varName2){
        Integer idx1 = varMap.get(varName1);
        Integer idx2 = varMap.get(varName2);
        if(idx2 == null || idx1 == null){
            throw new IllegalStateException("Variable name has error.");
        }
        return idx1 > idx2 ? idx1 : idx2;
    }

    /**
     * get dependent predicate constraints that need to join
     * @param hasJoinPos hasJoinPositionList
     * @param waitJoinPos wait join position
     * @return dependent predicate constraint list
     */
    public List<DependentConstraint> getDCListToJoin(List<Integer> hasJoinPos, int waitJoinPos){
        List<DependentConstraint> ans = new ArrayList<>();
        for(DependentConstraint dc : dcList){
            String varName1 = dc.getVarName1();
            String varName2 = dc.getVarName2();

            int varIndex1 = varMap.get(varName1);
            int varIndex2 = varMap.get(varName2);

            if(waitJoinPos == varIndex1){
                if(hasJoinPos.contains(varIndex2)){
                    ans.add(dc);
                }
            }else if(waitJoinPos == varIndex2){
                if(hasJoinPos.contains(varIndex1)){
                    ans.add(dc);
                }
            }
        }
        return ans;
    }

    public int getPatternLen(){
        return seqEventTypes.length;
    }

    public void print(){
        System.out.println("----------------------Event Pattern Information----------------------");
        System.out.println("query schema: '" + schemaName + "'");
        System.out.println("match strategy: '" + strategy + "'");
        for(int i = 0; i < seqEventTypes.length; i++){
            System.out.println("event type:'" +  seqEventTypes[i] + "'"
                    + " variable_name: '"+ seqVarNames[i] + "'" + " independent_predicate_constraints:");

            List<IndependentConstraint> icList = getICListUsingVarName(seqVarNames[i]);
            if(icList == null){
                System.out.println("null");
            }else{
                for(IndependentConstraint ic : icList){
                    ic.print();
                }
            }

        }

        System.out.println("dependent_predicate_constraints:");
        if(dcList.size() == 0){
            System.out.println("null");
        }else{
            for(DependentConstraint dc : dcList){
                dc.print();
            }
        }

        System.out.println("query time windows: " + tau + "ms");
        System.out.println("return statement: " + returnStr);
        System.out.println("-----------------------------------------------------------------------");
    }
}
