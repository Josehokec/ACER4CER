package condition;

import common.ComparedOperator;
import common.EventSchema;

// better design: variableName -> attributeName -> range？
// we can not process OR, only support AND
/**
 * Independent predicate constraints
 * format: minValue <= value(attrName) <= maxValue
 * we can support formats have
 * 100 <= a.open <= 165
 * b.volume > 80
 * assuming all are stored using long types
 * Note that since RangeBitmap does not support storage of floating-point types,
 * transformations need to be made here
 */
public class IndependentConstraint extends Constraint{
    final String attrName;
    final long minValue;
    final long maxValue;

    public IndependentConstraint(String attrName, long minValue, long maxValue){
        this.attrName = attrName;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * varName.attrName cmp value
     * @param attrName      attrName
     * @param cmp           ComparedOperator
     * @param value         value
     * @param schema        schema
     */
    public IndependentConstraint(String attrName, ComparedOperator cmp, String value, EventSchema schema){
        this.attrName = attrName;
        int idx = schema.getAttrNameIdx(attrName);
        String attrType = schema.getIthAttrType(idx);

        long v = 0;
        long min = Long.MIN_VALUE, max = Long.MAX_VALUE;
        if(attrType.equals("INT")){
            v = Integer.parseInt(value);
        }else if(attrType.contains("FLOAT")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v = (int) (Float.parseFloat(value) * magnification);
        }else if(attrType.contains("DOUBLE")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v = (long) (Double.parseDouble(value) * magnification);
        }

        switch (cmp) {
            case LE -> max = v;
            case LT -> max = v - 1;
            case GE -> min = v;
            case GT -> min = v + 1;
        }
        this.minValue = min;
        this.maxValue = max;
    }

    /**
     * value1 <=? varName.attrName <=? value2
     * @param attrName      attribute name
     * @param cmp1          cmp1
     * @param value1        value1
     * @param cmp2          cmp2
     * @param value2        value2
     * @param schema        event schema
     */
    public IndependentConstraint(String attrName, ComparedOperator cmp1, String value1, ComparedOperator cmp2, String value2, EventSchema schema){
        this.attrName = attrName;

        int idx = schema.getAttrNameIdx(attrName);
        String attrType = schema.getIthAttrType(idx);

        long v1 = 0;
        long v2 = 0;
        long min = Long.MIN_VALUE, max = Long.MAX_VALUE;
        if(attrType.equals("INT")){
            v1 = Integer.parseInt(value1);
            v2 = Integer.parseInt(value2);
        }else if(attrType.contains("FLOAT")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v1 = (int) (Float.parseFloat(value1) * magnification);
            v2 = (int) (Float.parseFloat(value2) * magnification);
        }else if(attrType.contains("DOUBLE")){
            int magnification = (int) Math.pow(10, schema.getIthDecimalLens(idx));
            v1 = (long) (Double.parseDouble(value1) * magnification);
            v2 = (long) (Double.parseDouble(value2) * magnification);
        }

        switch (cmp1) {
            case GE -> min = v1;
            case GT -> min = v1 + 1;
            default -> System.out.println("cmp1 is illegal.");
        }
        switch(cmp2){
            case LE -> max = v2;
            case LT -> max = v2 - 1;
            default -> System.out.println("cmp2 is illegal.");
        }
        this.minValue = min;
        this.maxValue = max;
    }

    public final String getAttrName() {
        return attrName;
    }

    public final long getMinValue() {
        return minValue;
    }

    public final long getMaxValue() {
        return maxValue;
    }

    /**
     * Determine if this independent constraint has a maximum and minimum value
     * @return   if there are no maximum and minimum values, return 0;
     *           if there is a minimum value and no maximum value, return 1;
     *           if there is no minimum value, return 2 if there is a maximum value;
     *           if there are minimum and maximum values, return 3;
     */
    public int hasMinMaxValue(){
        //ans = 0011
        int ans = 0x03;
        if(minValue == Long.MIN_VALUE){
            //set last bit value to 0
            ans &= 0xfe;
        }
        if(maxValue == Long.MAX_VALUE){
            // Set the second to last bit value to 0
            ans &= 0xfd;
        }
        return ans;
    }

    @Override
    public void print() {
        int val = hasMinMaxValue();
        if(val == 0){
            System.out.println("attrName: '" + attrName + "' do not have value range constraint.");
        }else if(val == 1){
            System.out.println("attrName: '" + attrName + "' value range is: [" + minValue+ ",INF).");
        }else if(val == 2){
            System.out.println("attrName: '" + attrName + "' value range is: (INF, " + maxValue + "].");
        }else{
            System.out.println("attrName: '" + attrName + "' value range is: [" + minValue + "," + maxValue + "].");
        }
    }

    @Override
    public String toString(){
        int val = hasMinMaxValue();
        String ans;
        if(val == 0){
            ans = "attrName: '" + attrName + "' do not have value range constraint.";
        }else if(val == 1){
            ans = "attrName: '" + attrName + "' value range is: [" + minValue+ ",INF).";
        }else if(val == 2){
            ans = "attrName: '" + attrName + "' value range is: (INF, " + maxValue + "].";
        }else{
            ans = "attrName: '" + attrName + "' value range is: [" + minValue + "," + maxValue + "].";
        }
        return ans;
    }
}
