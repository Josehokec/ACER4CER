package pattern;

import java.util.ArrayList;
import java.util.List;

public class DecomposeUtils {

    /**
     * this function aims to decomposing complex event pattern to a series sequential pattern
     * @param complexEventPattern       cep query statement
     * @return                          a series of sequential patterns
     */
    public static List<String> decomposingCEP(String complexEventPattern){
        complexEventPattern = complexEventPattern.trim();

        List<String> onlySEQStr = new ArrayList<>();

        if(!complexEventPattern.contains("OR") && !complexEventPattern.contains("AND")){
            onlySEQStr.add(complexEventPattern);

        }else{
            int[] marks = findMarks(complexEventPattern);
            String operator = complexEventPattern.substring(0, marks[0]);

            String leftSubPattern = complexEventPattern.substring(marks[0] + 1, marks[1]).trim();
            String rightPattern = complexEventPattern.substring(marks[1] + 1, marks[2]).trim();

            if(operator.equalsIgnoreCase("AND")){
                String newPattern1 = "SEQ(" + leftSubPattern + ", " + rightPattern + ")";
                String newPattern2 = "SEQ(" + rightPattern + ", " + leftSubPattern + ")";
                onlySEQStr.addAll(decomposingCEP(newPattern1));
                onlySEQStr.addAll(decomposingCEP(newPattern2));
            }else if(operator.equalsIgnoreCase("OR")){
                onlySEQStr.addAll(decomposingCEP(leftSubPattern));
                onlySEQStr.addAll(decomposingCEP(rightPattern));
            }else if(operator.equalsIgnoreCase("SEQ")){
                List<String> leftSubPatternDecomposingList = decomposingCEP(leftSubPattern);
                List<String> rightSubPatternDecomposingList = decomposingCEP(rightPattern);
                for(String leftStr : leftSubPatternDecomposingList){
                    for(String rightStr : rightSubPatternDecomposingList){
                        String newPattern = "SEQ(" + leftStr + ", " + rightStr + ")";
                        onlySEQStr.add(newPattern);
                    }
                }

            }else{
                System.out.println("no define operator: " + operator);
            }
        }

        List<String> ans = new ArrayList<>(onlySEQStr.size());
        for(String str : onlySEQStr){
            String newStr = str.replaceAll("SEQ\\(", "");
            newStr = newStr.replaceAll("\\)", "");
            newStr ="SEQ(" + newStr + ")";
            ans.add(newStr);
        }
        return ans;
    }

    public static int[] findMarks(String str){
        // e.g. SEQ(A a, B b)
        // marks[0] = 3, marks[1] = 7, marks[2] = 12
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

    // one operator
    public static void test1(){
        String[] testCEPList = {
                "SEQ(A a, B b)",
                "AND(A a, B b)",
                "OR(A a, B b)"
        };

        for(String cep : testCEPList){
            System.out.println("test CEP: " + cep);
            List<String> allSEP = DecomposeUtils.decomposingCEP(cep);


            System.out.println("All sequential event patterns: ");
            for(String sep : allSEP){
                System.out.println(sep);
            }
            System.out.println();
        }
    }

    public static void test2(){
        String[] testCEPList = {
                "OR(SEQ(A a, B b), C c)",
                "SEQ(AND(A a, B b), OR(C c, D d))",
                "AND(SEQ(AND(A a, B b), C c), D d)",
                "OR(SEQ(A a, B b), AND(C c, D d))"
        };

        for(String cep : testCEPList){
            System.out.println("test CEP: " + cep);
            List<String> allSEP = DecomposeUtils.decomposingCEP(cep);

            System.out.println("All sequential event patterns: ");
            for(String sep : allSEP){
                System.out.println(sep);
            }
            System.out.println();
        }
    }

    public static void main(String[] args){
        DecomposeUtils.test1();
        DecomposeUtils.test2();
    }
}
