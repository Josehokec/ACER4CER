package pattern;

import condition.DependentConstraint;

import java.util.List;


public class ComplexPattern extends QueryPattern {
    PatternTree patternTree;

    public ComplexPattern(String firstLine) {
        super(firstLine);
        onlyContainSEQ = false;
        parse(firstLine);
    }

    /**
     * @param firstLine  query statement first line
     */
    @Override
    public void parse(String firstLine) {
        // e.g. PATTERN SEQ(AND(ROBBERY v0, BATTERY v1), MOTOR_VEHICLE_THEFT v2)
        String patternStr = firstLine.substring(7).trim();
        this.patternTree = new PatternTree(patternStr);
        // variable name -> event type
        this.varTypeMap = patternTree.getVarNameEventTypeMap();
    }

    @Override
    public boolean isOnlyLeftMostNode(String varName) {
        if(varTypeMap.containsKey(varName)){
            return patternTree.isLeftMostVariable(varName);
        }
        return false;
    }

    @Override
    public boolean isOnlyRightMostNode(String varName) {
        if(varTypeMap.containsKey(varName)){
            return patternTree.isRightMostVariable(varName);
        }
        return false;
    }

    @SuppressWarnings("unused")
    public List<PatternNode> getInorderTravel(){
        return patternTree.getInorderTraversal(patternTree.getRoot());
    }

    public void print(){
        String patternStr = patternTree.print();
        System.out.println("pattern: " + patternStr);
        System.out.println("dc list:");
        for(DependentConstraint dc : dcList){
            dc.print();
        }
    }

}
