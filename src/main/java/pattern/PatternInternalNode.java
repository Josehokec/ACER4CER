package pattern;

class PatternInternalNode extends PatternNode{
    private PatternNode leftNode;
    private PatternNode rightNode;
    private PatternOperator patternOperator;

    // root node construction
    public PatternInternalNode(){
        super(null);
        this.nodeType = PatternNodeType.ROOT;
        level = 0;
    }

    public PatternInternalNode(PatternNode father, String str, int level){
        super(father);
        this.level = level;
        this.nodeType = PatternNodeType.INTERNAL_NODE;
        parse(str);
    }

    public void parse(String str){
        //remove all leading and trailing space
        str = str.trim();
        // marks size is 3, marks[0] -> leftmost bracket， marks[1] -> splitPos, marks[2] -> rightmost bracket
        int[] marks = findMarks(str);
        // read PatternOperator
        String operatorStr = str.substring(0, marks[0]);
        // switch case, set patternOperator
        if(operatorStr.equalsIgnoreCase("AND")){
            patternOperator = PatternOperator.AND;
        }else if(operatorStr.equalsIgnoreCase("OR")){
            patternOperator = PatternOperator.OR;
        }else if(operatorStr.equalsIgnoreCase("SEQ")){
            patternOperator = PatternOperator.SEQ;
        }else{
            System.out.println("(Leaf) Do not support '" + operatorStr + "' pattern operator.");
        }

        // split str based on marks[2]
        String leftStr = str.substring(marks[0] + 1, marks[1]);
        String rightStr = str.substring(marks[1] + 1, marks[2]);
        //  System.out.println("leftStr: " + leftStr + " rightStr: " + rightStr);
        if(leftStr.indexOf(',') != -1){
            this.leftNode = new PatternInternalNode(this, leftStr, level + 1);
        }else{
            this.leftNode = new PatternLeafNode(this, leftStr, level + 1);
        }

        if(rightStr.indexOf(',') != -1){
            this.rightNode = new PatternInternalNode(this, rightStr, level + 1);
        }else{
            this.rightNode = new PatternLeafNode(this, rightStr, level + 1);
        }


    }

    public int[] findMarks(String str){
        // e.g. SEQ(A a, B b)
        int[] marks = new int[3];
        // according to symbol ',' to split left string and right string
        // if number of '(' is equal to number of ')', then it means this split is right
        int leftBracketNum = 0;
        // here we do not use indexOf and lastIndexOf function
        int len = str.length();
        // first bracket is leftmost bracket
        boolean leftmostBracket = true;
        // start loop to set marks
        for(int i = 0; i < len; ++i){
            if(str.charAt(i) == '('){
                leftBracketNum++;
                if(leftmostBracket){
                    marks[0] = i;
                    leftmostBracket = false;
                }
            }else if(str.charAt(i) == ',' && leftBracketNum == 1){
                marks[1] = i;
            }else if(str.charAt(i) == ')'){
                leftBracketNum--;
                marks[2] = i;				// always update marks[2]
            }
        }

        return marks;
    }

    @Override
    public int getPatternLen() {
        if(patternOperator == PatternOperator.AND){
            return rightNode.getPatternLen() + leftNode.getPatternLen();
        }else if(patternOperator == PatternOperator.SEQ){
            return rightNode.getPatternLen() + leftNode.getPatternLen();
        }else if(patternOperator == PatternOperator.OR){
            // SEQ(OR(SEQ(A a, B b), C c), D d)
            return Math.max(rightNode.getPatternLen(), leftNode.getPatternLen());
        }else{
            return -1;
        }
    }

    @Override
    public String print() {
        String ans = "";
        switch (patternOperator) {
            case AND -> ans = ans + "AND(" + leftNode.print() + ", " + rightNode.print() + ")";
            case SEQ -> ans = ans + "SEQ(" + leftNode.print() + ", " + rightNode.print() + ")";
            case OR -> ans = ans + "OR(" + leftNode.print() + ", " + rightNode.print() + ")";
            default -> System.out.println("wrong state.");
        }
        return ans;
    }

    public PatternNode getRightNode(){
        return rightNode;
    }

    public PatternNode getLeftNode(){
        return leftNode;
    }

    public PatternOperator getPatternOperator() { return patternOperator; }

    public String toString(){
        String op;
        switch (patternOperator) {
            case AND -> op = "AND";
            case SEQ -> op = "SEQ";
            case OR -> op = "OR";
            default -> op = "Error";
        }

        return "internal info: [node type: " + nodeType + ", operator: " + op + "]";
    }
}

