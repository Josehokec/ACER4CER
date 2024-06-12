package pattern;

import java.util.*;

class PatternTree{
    private final PatternNode root;

    public PatternTree(String queryPattern){
        root = new PatternInternalNode();
        root.parse(queryPattern);
    }

    public PatternNode getRoot() { return root; }

    /**
     * get pattern length
     * len(SEQ(A a, OR(B b, C c))) = 2, len(SEQ(A a, OR(AND(B b, C c), D d))) = 2 or 3?
     * @return      pattern length
     */
    public int getPatternLen(){
        return root.getPatternLen();
    }

    public String print(){
        return root.print();
    }

    public List<PatternLeafNode> getAllLeafNode(){
        List<PatternLeafNode> ans = new ArrayList<>();
        // level traversal
        Queue<PatternInternalNode> queue = new LinkedList<>();
        PatternInternalNode rootNode = (PatternInternalNode) root;
        queue.add(rootNode);
        while(!queue.isEmpty()){
            PatternInternalNode curNode = queue.remove();
            PatternNode leftNode = curNode.getLeftNode();
            PatternNode rightNode = curNode.getRightNode();
            if(leftNode.getNodeType() == PatternNodeType.INTERNAL_NODE){
                queue.add((PatternInternalNode) leftNode);
            }else{
                ans.add((PatternLeafNode) leftNode);
            }

            if(rightNode.getNodeType() == PatternNodeType.INTERNAL_NODE){
                queue.add((PatternInternalNode) rightNode);
            }else{
                ans.add((PatternLeafNode) rightNode);
            }
        }
        return ans;
    }

    public HashMap<String, String> getVarNameEventTypeMap(){
        List<PatternLeafNode> leafNodes = getAllLeafNode();

        HashMap<String, String> ans = new HashMap<>((int) (leafNodes.size() / 0.75  + 1.0));
        for(PatternLeafNode leafNode : leafNodes){
            String varName = leafNode.getVarName();
            String eventType = leafNode.getEventType();
            ans.put(varName, eventType);
        }

        return ans;
    }

    /**
     * we suggest firstly use varMap to check this variable whether exists in pattern
     * check this varName whether is left most node
     * here we suppose there do not exist OR operator
     * @param varName        variable name
     * @return               left most node?
     */
    public boolean isLeftMostVariable(String varName){
        // Traverse all the way to the left (here we ignore OR operator)
        // if encountering an AND operator, it must not be the only leftmost node
        PatternNode curNode = root;

        while(curNode.getNodeType() != PatternNodeType.LEAF_NODE){
            PatternInternalNode internal = (PatternInternalNode) curNode;
            if(internal.getPatternOperator() == PatternOperator.AND){
                return false;
            }
            curNode = internal.getLeftNode();
        }

        // return whether same variable name
        PatternLeafNode leaf = (PatternLeafNode) curNode;
        return leaf.getVarName().equalsIgnoreCase(varName);
    }

    /**
     * we suggest firstly use varMap to check this variable whether exists in pattern
     * check this varName whether is right most node
     * here we suppose there do not exist OR operator
     * @param varName        variable name
     * @return               left most node?
     */
    public boolean isRightMostVariable(String varName){
        // Traverse all the way to the left (here we ignore OR operator)
        // if encountering an AND operator, it must not be the only leftmost node
        PatternNode curNode = root;

        while(curNode.getNodeType() != PatternNodeType.LEAF_NODE){
            PatternInternalNode internal = (PatternInternalNode) curNode;
            if(internal.getPatternOperator() == PatternOperator.AND){
                return false;
            }
            curNode = internal.getRightNode();
        }

        // return whether same variable name
        PatternLeafNode leaf = (PatternLeafNode) curNode;
        return leaf.getVarName().equalsIgnoreCase(varName);
    }


    /**
     * inorder traversal: left, middle, right
     * @param curNode   current node
     * @return          inorder result
     */
    public List<PatternNode> getInorderTraversal(PatternNode curNode){

        if(curNode.getNodeType() == PatternNodeType.LEAF_NODE){
            List<PatternNode> nodeList = new ArrayList<>();
            nodeList.add(curNode);
            return nodeList;
        }

        PatternInternalNode internalNode = (PatternInternalNode) curNode;
        // left
        List<PatternNode> ans = new ArrayList<>(getInorderTraversal(internalNode.getLeftNode()));
        // middle
        ans.add(curNode);
        // right
        ans.addAll(getInorderTraversal(internalNode.getRightNode()));
        return ans;
    }


}
