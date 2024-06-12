package pattern;

public abstract class PatternNode{
    protected PatternNode father;
    protected int level;
    protected PatternNodeType nodeType;

    public PatternNode(PatternNode father){
        this.father = father;
    }

    public abstract void parse(String str);
    public abstract int getPatternLen();
    public abstract String print();

    public PatternNode getFather(){
        return father;
    }

    public int getLevel(){
        return level;
    }
    public PatternNodeType getNodeType(){
        return nodeType;
    }
}
