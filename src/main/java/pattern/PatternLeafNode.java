package pattern;

class PatternLeafNode extends PatternNode{
    private String varName;
    String eventType;
    public PatternLeafNode(PatternNode father, String str, int level) {
        super(father);
        this.level = level;
        this.nodeType = PatternNodeType.LEAF_NODE;
        parse(str);
    }

    public String getVarName(){
        return varName;
    }

    public String getEventType(){
        return eventType;
    }

    /**
     * leaf node str format: eventType variableName
     * so we split left and right part
     * @param str   part of complex event pattern statement
     */
    @Override
    public void parse(String str) {
        // e.g. A a
        str = str.trim();
        String[] twoParts = str.split(" ");
        this.eventType = twoParts[0];
        this.varName = twoParts[1];
    }

    @Override
    public int getPatternLen() {
        return 1;
    }

    @Override
    public String print() {
        return eventType + " " + varName;
    }

    @Override
    public String toString(){
        return "leaf info: [eventType: " + eventType + ", variableName: " + varName + "]";
    }
}
