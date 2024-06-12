package pattern;

import java.util.HashMap;

public class SequentialPattern extends QueryPattern {
    String[] seqEventTypes;
    String[] seqVarNames;
    private HashMap<String, Integer> varPosMap;	// accelerate find position

    public SequentialPattern(String firstLine){
        super(firstLine);
        onlyContainSEQ = true;
        varPosMap = new HashMap<>();
        parse(firstLine);
    }

    @Override
    public void parse(String firstLine) {
        // e.g., PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
        String[] seqStatement = firstLine.split("[()]");
        // seqEvent = "ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2"
        String[] seqEvent = seqStatement[1].split(",");
        this.patternLen = seqEvent.length;
        this.variableNum = patternLen;
        this.seqEventTypes = new String[patternLen];
        this.seqVarNames = new String[patternLen];

        for(int i = 0; i < patternLen; ++i){
            String[] s = seqEvent[i].trim().split(" ");
            seqEventTypes[i] = s[0];
            seqVarNames[i] = s[1].trim();
            // update two maps
            varTypeMap.put(seqVarNames[i], seqEventTypes[i]);
            varPosMap.put(seqVarNames[i], i);
        }
    }

    @Override
    public boolean isOnlyLeftMostNode(String varName) {
        return varPosMap.get(varName) == 0;
    }

    @Override
    public boolean isOnlyRightMostNode(String varName) {
        return varPosMap.get(varName) == patternLen - 1;
    }

    @Override
    public void print() {
        int len = seqEventTypes.length;
        StringBuffer buff = new StringBuffer(256);
        buff.append("SEQ(");
        for(int i = 0; i < len; ++i){
            buff.append(seqEventTypes[i]).append(" ").append(seqVarNames[i]);
            if(i == len -1){
                buff.append(")");
            }else{
                buff.append(", ");
            }
        }
        System.out.println(buff);
    }
}
