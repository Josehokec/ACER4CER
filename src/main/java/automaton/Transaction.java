package automaton;

import java.util.ArrayList;
import java.util.List;

import common.EventSchema;
import condition.DependentConstraint;
import condition.IndependentConstraint;
import pattern.DecomposeUtils;

public class Transaction {
    private String nextEventType;                   // event type
    private List<IndependentConstraint> icList;     //  independent constraint list
    private List<DependentConstraint> dcList;       // dependent constraint list
    private State nextState;                        // next state

    public Transaction(String nextEventType){
        this.nextEventType = nextEventType;
        this.icList = new ArrayList<>();
        this.dcList = new ArrayList<>();
    }

    public Transaction(String nextEventType, List<IndependentConstraint> icList, List<DependentConstraint> dcList, State nextState){
        this.nextEventType = nextEventType;
        this.icList = icList;
        this.dcList = dcList;
        this.nextState = nextState;
    }

    /**
     * check a event (byte value) whether satisfy independent constraint
     * here we view event type as independent constraint
     * @param schema        event schema
     * @param eventRecord   event
     * @return
     */
    public boolean checkIC(EventSchema schema, byte[] eventRecord){
        // determine the stored column index for event type
        int typeIdx = schema.getTypeIdx();
        String curType = schema.getTypeFromBytesRecord(eventRecord, typeIdx);
        boolean satisfy = true;
        if (curType.equals(nextEventType)) {
            for(IndependentConstraint ic : icList){
                String name = ic.getAttrName();
                long min = ic.getMinValue();
                long max = ic.getMaxValue();
                // obtain the corresponding column for storage based on the attribute name
                int col = schema.getAttrNameIdx(name);
                long value = schema.getValueFromBytesRecord(eventRecord, col);
                if (value < min || value > max) {
                    satisfy = false;
                    break;
                }
            }
        }else{
            satisfy = false;
        }

        return satisfy;
    }

    public List<DependentConstraint> getDCList(){
        return dcList;
    }

    public State getNextState(){
        return nextState;
    }

    public void print(){
        System.out.println("transaction infomartion");
        System.out.println("event type: " + nextEventType);
        System.out.println("ic list: ");
        for(IndependentConstraint ic : icList){
            ic.print();
        }
        System.out.println("dc list: ");
        for(DependentConstraint dc : dcList){
            dc.print();
        }
        System.out.println("next state information: ");
        System.out.println(nextState);
        System.out.println("\n");
    }
}
