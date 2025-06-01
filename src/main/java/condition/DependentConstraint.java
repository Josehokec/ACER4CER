package condition;

import common.ArithmeticOperator;
import common.ComparedOperator;
import common.EventSchema;

/**
 * a unified format as follows：
 * varName1.attrName ao1 m1 ao2 a1 LE varName2.attrName ao3 m2 ao4 a2
 * we can support:
 * a.open * 3 + 5 <= b.open * 4 - 5
 * a.open + 3 <= b.open
 * a.open <= b.open
 * a.open + 4 >= b.open * 5.0
 * we cannot support format: a.open <= b.open + c.open | a.open <= b.open <= c.open | 2 * a.open + 4 <= 4 * b.open + 9
 */
public class DependentConstraint extends Constraint{
    private String attrName;            // attribute name
    private ComparedOperator cmp;       // comparison operator
    private String varName1;            // variable name 1
    private String varName2;            // variable name 2
    private double m1;                  // multiple 1
    private long a1;                    // addend number1
    private double m2;                  // multiple 2
    private long a2;                    // addend number2
    ArithmeticOperator ao1, ao2, ao3, ao4;

    public DependentConstraint(){
        attrName = null;
        varName1 = null;
        varName2 = null;
        cmp = null;
        m1 = m2 = 1;
        a1 = a2 = 0;
        ao1 = ao3 = ArithmeticOperator.MUL;
        ao2 = ao4 = ArithmeticOperator.ADD;
    }

    /**
     * String format: a.open < b.open<br>
     * @param attrName attribute name
     * @param varName1 variable name 1
     * @param varName2 variable name 2
     * @param cmp comparison operator
     */
    public DependentConstraint(String attrName, String varName1, String varName2, ComparedOperator cmp){
        this.attrName = attrName;
        this.varName1 = varName1;
        this.varName2 = varName2;
        this.cmp = cmp;
        m1 = m2 = 1;
        a1 = a2 = 0;
        ao1 = ao3 = ArithmeticOperator.MUL;
        ao2 = ao4 = ArithmeticOperator.ADD;
    }

    public void setCMP(ComparedOperator cmp){
        this.cmp = cmp;
    }

    public ComparedOperator getCMP() {return cmp;}

    public String getAttrName(){
        return attrName;
    }

    public String getVarName1(){
        return varName1;
    }

    public String getVarName2(){
        return varName2;
    }

    /**
     * Construct the expression on the left <br>
     * format: attrName.varName1 ao1 m1 ao2 a1<br>
     * supported format：a.open * 3 + 3 ｜ a.open + 4 ｜ a.open * 3 <br>
     * we cannot support 3 * a.open + 4 and 4 + a.open
     * @param left left string
     * @param s event schema
     */
    public void constructLeft(String left, EventSchema s){
        int dotPos = -1;
        boolean readAttrName = false;
        int hasMulOrDiv = -1;
        int hasAddOrSub = -1;
        for(int i = 0; i < left.length(); ++i){
            if(left.charAt(i) == '.' && dotPos == -1){
                dotPos = i;
                varName1 = left.substring(0, dotPos).trim();
            }else if(left.charAt(i) == '*' || left.charAt(i) == '/'){
                hasMulOrDiv = i;
                if(!readAttrName){
                    attrName = left.substring(dotPos + 1, hasMulOrDiv).trim();
                    readAttrName = true;
                }
                ao1 = left.charAt(i) == '*' ? ArithmeticOperator.MUL : ArithmeticOperator.DIV;
            }else if(left.charAt(i) == '+' || left.charAt(i) == '-'){
                hasAddOrSub = i;
                if(!readAttrName){
                    attrName = left.substring(dotPos + 1, hasAddOrSub).trim();
                }
                ao2 = left.charAt(i) == '+' ? ArithmeticOperator.ADD : ArithmeticOperator.SUB;
                String a1Str = left.substring(i + 1).trim();

                int idx = s.getAttrNameIdx(attrName);
                if(s.getIthAttrType(idx).equals("INT")){
                    a1 = Long.parseLong(a1Str);
                }else if(s.getIthAttrType(idx).contains("FLOAT")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a1 = (long) (Float.parseFloat(a1Str) * magnification);
                }else if(s.getIthAttrType(idx).contains("DOUBLE")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a1 = (long) (Double.parseDouble(a1Str) * magnification);
                }

                //a1 = (long) (Double.parseDouble(a1Str));
                break;
            }
        }

        if(attrName == null){
            attrName = left.substring(dotPos + 1).trim();
        }

        // If m1 is not assigned a value, it defaults to 1
        if(hasMulOrDiv != -1){
            String m1Str;
            if(hasAddOrSub == -1){
                m1Str = left.substring(hasMulOrDiv + 1).trim();
            }else{
                m1Str = left.substring(hasMulOrDiv + 1, hasAddOrSub);
            }
            m1 = Double.parseDouble(m1Str);
        }

        // debug
        // System.out.println("varName1: " + varName1 + " attrName: " + attrName + " m1: " + m1 + " a1: " + a1);
    }

    /**
     * Construct the expression on the right <br>
     * format: attrName.varName2 ao3 m2 ao4 a2<br>
     * supported format example: b.open * 3 + 3 ｜ b.open + 4 ｜ b.open * 3 <br>
     * we cannot support 3 * b.open + 4 and 4 + b.open<br>
     * @param right right string
     * @param s event schema
     */
    public void constructRight(String right, EventSchema s){
        //点的位置
        int dotPos = -1;
        boolean readAttrName = false;
        int hasMulOrDiv = -1;
        int hasAddOrSub = -1;
        String rightAttrName;
        for(int i = 0; i < right.length(); ++i){
            if(right.charAt(i) == '.' && dotPos == -1){
                dotPos = i;
                varName2 = right.substring(0, dotPos).trim();
            }else if(right.charAt(i) == '*' || right.charAt(i) == '/'){
                hasMulOrDiv = i;
                if(!readAttrName){
                    readAttrName = true;
                    rightAttrName = right.substring(dotPos + 1, hasMulOrDiv).trim();


                    if(!attrName.equals(rightAttrName)){
                        throw new RuntimeException("Illegal dependent constraint");
                    }
                }
                ao3 = right.charAt(i) == '*' ? ArithmeticOperator.MUL : ArithmeticOperator.DIV;
            }else if(right.charAt(i) == '+' || right.charAt(i) == '-'){
                hasAddOrSub = i;
                if(!readAttrName){
                    rightAttrName = right.substring(dotPos + 1, hasAddOrSub).trim();
                    if(!attrName.equals(rightAttrName)){
                        throw new RuntimeException("Illegal dependent constraint");
                    }
                }
                ao4 = right.charAt(i) == '+' ? ArithmeticOperator.ADD : ArithmeticOperator.SUB;
                String a2Str = right.substring(i + 1).trim();

                int idx = s.getAttrNameIdx(attrName);
                if(s.getIthAttrType(idx).equals("INT")){
                    a2 = Long.parseLong(a2Str);
                }else if(s.getIthAttrType(idx).contains("FLOAT")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a2 = (long) (Float.parseFloat(a2Str) * magnification);
                }else if(s.getIthAttrType(idx).contains("DOUBLE")){
                    int magnification = (int) Math.pow(10, s.getIthDecimalLens(idx));
                    a2 = (long) (Double.parseDouble(a2Str) * magnification);
                }

                //a2 = (long) (Double.parseDouble(a2Str));
                break;
            }
        }
        // If m1 was not previously assigned a value, it defaults to 1
        if(hasMulOrDiv != -1){
            String m2Str;
            if(hasAddOrSub == -1){
                m2Str = right.substring(hasMulOrDiv + 1).trim();
            }else{
                m2Str = right.substring(hasMulOrDiv + 1, hasAddOrSub);
            }
            m2 = Double.parseDouble(m2Str);
        }

        // debug
        // System.out.println("varName2: " + varName2 + " attrName: " + attrName + " m2: " + m1 + " a2: " + a1);
    }

    /**
     * Be sure to pass it in order, otherwise accuracy cannot be guaranteed
     * @param value1 value of varName1
     * @param value2 value of varName2
     * @return return true if satisfied, otherwise return false
     */
    public boolean satisfy(long value1, long value2){
        double leftValue = getLeftValue(value1);
        double rightValue = getRightValue(value2);
        switch (cmp) {
            case LT -> {
                return (leftValue < rightValue);
            }
            case GE -> {
                return (leftValue >= rightValue);
            }
            case GT -> {
                return (leftValue > rightValue);
            }
            case LE -> {
                return (leftValue <= rightValue);
            }
            case EQ -> {
                return (leftValue == rightValue);
            }
            default -> System.out.println("undefine compared operator");
        }
        return false;
    }

    // maybe has bug
    public long[] getRightVarRange(long leftAttrMin, long leftAttrMax){
        double leftMin, leftMax;
        if(m1 < 0){
            leftMin = getLeftValue(leftAttrMax);
            leftMax = getLeftValue(leftAttrMin);
        }else{
            leftMin = getLeftValue(leftAttrMin);
            leftMax = getLeftValue(leftAttrMax);
        }
        long min, max;
        if(m2 > 0){
            double tempMin, tempMax;
            switch(ao4){
                case ADD -> {
                    tempMin = leftMin - a2;
                    tempMax = leftMax - a2;
                }
                case SUB -> {
                    tempMin = leftMin + a2;
                    tempMax = leftMax + a2;
                }
                default -> throw new RuntimeException("algorithm operator must be add or sub");
            }

            switch (ao3){
                case DIV -> {
                    min = (long) (tempMin * m2);
                    max = (long) (tempMax * m2);
                }
                case MUL -> {
                    min = (long) (tempMin / m2);
                    max = (long) (tempMax / m2);
                }
                default -> throw new RuntimeException("algorithm operator must be mul or div");
            }
        }else{
            double tempMin, tempMax;
            switch(ao4){
                case ADD -> {
                    tempMin = leftMin - a2;
                    tempMax = leftMax - a2;
                }
                case SUB -> {
                    tempMin = leftMin + a2;
                    tempMax = leftMax + a2;
                }
                default -> throw new RuntimeException("algorithm operator must be add or sub");
            }

            switch (ao3){
                case DIV -> {
                    max = (long) (tempMin * m2);
                    min = (long) (tempMax * m2);
                }
                case MUL -> {
                    max = (long) (tempMin / m2);
                    min = (long) (tempMax / m2);
                }
                default -> throw new RuntimeException("algorithm operator must be mul or div");
            }
        }

        return new long[]{min, max};
    }

    public long[] getLeftVarRange(long rightAttrMin, long rightAttrMax){
        double rightMin, rightMax;
        if(m2 < 0){
            rightMin = getLeftValue(rightAttrMax);
            rightMax = getLeftValue(rightAttrMin);
        }else{
            rightMin = getLeftValue(rightAttrMin);
            rightMax = getLeftValue(rightAttrMax);
        }
        long min, max;
        if(m1 > 0){
            double tempMin, tempMax;
            switch(ao2){
                case ADD -> {
                    tempMin = rightMin - a1;
                    tempMax = rightMax - a1;
                }
                case SUB -> {
                    tempMin = rightMin + a1;
                    tempMax = rightMax + a1;
                }
                default -> throw new RuntimeException("algorithm operator must be add or sub");
            }

            switch (ao1){
                case DIV -> {
                    min = (long) (tempMin * m1);
                    max = (long) (tempMax * m1);
                }
                case MUL -> {
                    min = (long) (tempMin / m1);
                    max = (long) (tempMax / m1);
                }
                default -> throw new RuntimeException("algorithm operator must be mul or div");
            }
        }else{
            double tempMin, tempMax;
            switch(ao2){
                case ADD -> {
                    tempMin = rightMin - a1;
                    tempMax = rightMax - a1;
                }
                case SUB -> {
                    tempMin = rightMin + a1;
                    tempMax = rightMax + a1;
                }
                default -> throw new RuntimeException("algorithm operator must be add or sub");
            }

            switch (ao3){
                case DIV -> {
                    max = (long) (tempMin * m1);
                    min = (long) (tempMax * m1);
                }
                case MUL -> {
                    max = (long) (tempMin / m1);
                    min = (long) (tempMax / m1);
                }
                default -> throw new RuntimeException("algorithm operator must be mul or div");
            }
        }

        return new long[]{min, max};
    }

    /**
     * obtain the value on the left side of the expression
     * @param value1 incoming parameters
     * @return left value
     */
    public double getLeftValue(long value1){
        double ans = switch (ao1) {
            case MUL -> value1 * m1;
            case DIV -> value1 / m1;
            default -> 0;
        };

        switch (ao2) {
            case ADD -> ans += a1;
            case SUB -> ans -= a1;
        }
        return ans;
    }

    /**
     * obtain the value on the right side of the expression
     * @param value2 incoming parameters
     * @return right value
     */
    public double getRightValue(long value2){
        double ans = switch (ao3) {
            case MUL -> value2 * m2;
            case DIV -> value2 / m2;
            default -> 0;
        };

        switch (ao4) {
            case ADD -> ans += a2;
            case SUB -> ans -= a2;
        }
        return ans;
    }

    @Override
    public void print() {
        switch (cmp) {
            case GT -> System.out.print(leftPart() + " > " + rightPart() + ";");
            case GE -> System.out.print(leftPart() + " >= " + rightPart() + ";");
            case LE -> System.out.print(leftPart() + " <= " + rightPart() + ";");
            case LT -> System.out.print(leftPart() + " < " + rightPart() + ";");
            case EQ -> System.out.print(leftPart() + " = " + rightPart() + ";");
        }
    }

    public String leftPart(){
        StringBuilder buff = new StringBuilder();
        buff.append(varName1).append(".").append(attrName);
        if(ao1 != ArithmeticOperator.MUL || m1 != 1){
            if(ao1 == ArithmeticOperator.MUL){
                buff.append(" * ");
            }else{
                buff.append(" / ");
            }
            buff.append(m1);
        }

        if(ao2 != ArithmeticOperator.ADD || a1 != 0){
            if(ao2 == ArithmeticOperator.ADD){
                buff.append(" + ");
            }else{
                buff.append(" - ");
            }

            buff.append(a1);
        }
        return buff.toString();
    }

    public String rightPart(){
        StringBuilder buff = new StringBuilder();
        buff.append(varName2).append(".").append(attrName);
        if(ao3 != ArithmeticOperator.MUL || m2 != 1){
            if(ao3 == ArithmeticOperator.MUL){
                buff.append(" * ");
            }else{
                buff.append(" / ");
            }
            buff.append(m2);
        }

        if(ao4 != ArithmeticOperator.ADD || a2 != 0){
            if(ao4 == ArithmeticOperator.ADD){
                buff.append(" + ");
            }else{
                buff.append(" - ");
            }

            buff.append(a2);
        }
        return buff.toString();
    }


    public static void main(String[] args){
        DependentConstraint dc0= new DependentConstraint("open", "a", "b", ComparedOperator.LE);
        dc0.print();

        String str1 = "a.open + 4 >= b.open * 5";
        DependentConstraint dc1 = new DependentConstraint();
        for(int i = 0; i < str1.length(); ++i){
            if(str1.charAt(i) == '<' && str1.charAt(i + 1) == '='){
                dc1.setCMP(ComparedOperator.LE);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 2), null);
                break;
            }else if(str1.charAt(i) == '<'){
                dc1.setCMP(ComparedOperator.LT);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 1), null);
                break;
            }else if(str1.charAt(i) == '>' && str1.charAt(i + 1) == '='){
                dc1.setCMP(ComparedOperator.GE);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 2), null);
                break;
            }else if(str1.charAt(i) == '>'){
                dc1.setCMP(ComparedOperator.GT);
                dc1.constructLeft(str1.substring(0, i), null);
                dc1.constructRight(str1.substring(i + 1), null);
                break;
            }
        }
        dc1.print();

        String str2 = "a.open / 2 - 40 >= b.open * 5 - 1";
        DependentConstraint dc2 = new DependentConstraint();

        for(int i = 0; i < str2.length(); ++i){
            if(str2.charAt(i) == '<' && str2.charAt(i + 1) == '='){
                dc2.setCMP(ComparedOperator.LE);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 2), null);
                break;
            }else if(str2.charAt(i) == '<'){
                dc2.setCMP(ComparedOperator.LT);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 1), null);
                break;
            }else if(str2.charAt(i) == '>' && str2.charAt(i + 1) == '='){
                dc2.setCMP(ComparedOperator.GE);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 2), null);
                break;
            }else if(str2.charAt(i) == '>'){
                dc2.setCMP(ComparedOperator.GT);
                dc2.constructLeft(str2.substring(0, i), null);
                dc2.constructRight(str2.substring(i + 1), null);
                break;
            }
        }
        dc2.print();

        String str3 = "a.open + 3 <= b.open";
        DependentConstraint dc3 = new DependentConstraint();
        for(int i = 0; i < str3.length(); ++i){
            if(str3.charAt(i) == '<' && str3.charAt(i + 1) == '='){
                dc3.setCMP(ComparedOperator.LE);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 2), null);
                break;
            }else if(str3.charAt(i) == '<'){
                dc3.setCMP(ComparedOperator.LT);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 1), null);
                break;
            }else if(str3.charAt(i) == '>' && str3.charAt(i + 1) == '='){
                dc3.setCMP(ComparedOperator.GE);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 2), null);
                break;
            }else if(str3.charAt(i) == '>'){
                dc3.setCMP(ComparedOperator.GT);
                dc3.constructLeft(str3.substring(0, i), null);
                dc3.constructRight(str3.substring(i + 1), null);
                break;
            }
        }
        dc3.print();
    }
}
