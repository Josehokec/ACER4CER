package generator;

import net.sf.json.JSONArray;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * here we test complex event pattern in synthetic dataset
 * | P1 | `SEQ(A a, B b, C c)`               |
 * | P2 | `SEQ(A a, B b, C c, D d, E e)`     |
 * | P3 | `SEQ(A a, AND(B b, C c), D d)`     |
 * | P4 | `AND(SEQ(A a, B b), SEQ(C c, D d))` |
 * | P5 | `SEQ(AND(A a, B b),AND(C c, D d))` |
 */
public class SyntheticQueryGenerator {
    private final int eventTypeNum = 20;
    private final Random random = new Random(11);
    public SyntheticQueryGenerator() {}

    public String[] selectNItems(int n){
        String[] selectedStrings = new String[n];
        HashSet<Integer> selectIds = new HashSet<>();
        for (int i = 0; i < n; i++) {
            int randomIndex;
            do {
                randomIndex = random.nextInt(eventTypeNum);
            } while (selectIds.contains(randomIndex));
            selectedStrings[i] = "TYPE_" + randomIndex;
            selectIds.add(randomIndex);
        }
        return selectedStrings;
    }

    //       selId -> 1,   2,   3,   4,   5
    // selectivity -> 5%, 10%, 15%, 20%, 25%
    public String getQueryRearPart(boolean enableNextMatch, int varNum, int selId){
        long window = 1000;
        StringBuilder queryRearPart = new StringBuilder(512);
        // second line
        queryRearPart.append("FROM synthetic\n");

        if(enableNextMatch){
            int flag = random.nextInt(0, 2);
            if(flag == 1){
                queryRearPart.append("USING SKIP_TILL_ANY_MATCH\n");
            }else{
                queryRearPart.append("USING SKIP_TILL_NEXT_MATCH\n");
            }
        }
        else{
            queryRearPart.append("USING SKIP_TILL_ANY_MATCH\n");
        }

        // three variables
        queryRearPart.append("WHERE ");

        int maxA1OrA2;
        double maxA3OrA4;
        switch (selId){
            case 1 -> {
                maxA1OrA2 = 51;
                maxA3OrA4 = 3355.2;
            }
            case 2 -> {
                maxA1OrA2 = 101;
                maxA3OrA4 = 3718.5;
            }
            case 3 -> {
                maxA1OrA2 = 151;
                maxA3OrA4 = 3963.6;
            }
            case 4 -> {
                maxA1OrA2 = 201;
                maxA3OrA4 = 4158.4;
            }
            case 5 -> {
                maxA1OrA2 = 251;
                maxA3OrA4 = 4325.5;
            }
            default -> throw new RuntimeException("we cannot support this selId: " + selId);
        }

        // selectivity-> 5%, 10%, 15%, 20%, 25%
        // a1, a2 ~ U[1, 1000] ->
        // 51, 101, 151, 201, 251
        // 3355.15, 3718.45, 3963.57, 4158.38, 4325.51
        for(int i = 0; i < varNum; i++){
            int attrId1 = random.nextInt(1, 5);
            int attrId2;
            do{
                attrId2 = random.nextInt(1, 5);
            }while (attrId2 == attrId1);

            queryRearPart.append(0).append(" <= v").append(i).append(".a").append(attrId1).append(" <= ");
            if(attrId1 == 1 || attrId1 == 2){
                queryRearPart.append(maxA1OrA2).append(" AND ");
            }else{
                queryRearPart.append(maxA3OrA4).append(" AND ");
            }

            queryRearPart.append(0).append(" <= v").append(i).append(".a").append(attrId2).append(" <= ");
            if(attrId2 == 1 || attrId2 == 2){
                queryRearPart.append(maxA1OrA2).append(" AND ");
            }else{
                queryRearPart.append(maxA3OrA4).append(" AND ");
            }
        }

        // then we add dc list (randomly choose 1 ~ 3 DC)
        int dcNum = random.nextInt(1, 3);
        for(int j = 0; j < dcNum; ++j){
            int var1 = random.nextInt(varNum);
            int var2 = random.nextInt(varNum);
            if(var2 == var1){
                var2 = (var2 + 1) % varNum;
            }
            int attrId = random.nextInt(1, 5);
            queryRearPart.append("v").append(var1).append(".a").append(attrId).append(" <= ").append("v").append(var2).append(".a").append(attrId);
            if(j != dcNum - 1){
                queryRearPart.append(" AND ");
            }else{
                queryRearPart.append("\n");
            }
        }

        queryRearPart.append("WITHIN ").append(window).append(" units\n");
        queryRearPart.append("RETURN COUNT(*)");

        return queryRearPart.toString();
    }

    public List<String> generateQueries(){
        List<String> queries = new ArrayList<>(500);
        int number = 100;
        int selId = 5;

        for(int patternId = 1; patternId <= 5; patternId++){
            switch (patternId){
                case 1 -> {
                    // SEQ(A a, B b, C c)
                    for(int i = 0; i < number; i++){
                        String[] eventTypes = selectNItems(3);
                        String buffer = "PATTERN SEQ("
                                + eventTypes[0] + " v0, "
                                + eventTypes[1] + " v1, "
                                + eventTypes[2] + " v2" + ")\n";
                        String rearPart = getQueryRearPart(false, 3, selId);
                        String query = buffer + rearPart;
                        queries.add(query);
                        System.out.println("--> query: \n" + query);
                    }
                }
                case 2 -> {
                    // SEQ(A a, B b, C c, D d, E e)
                    for(int i = 0; i < number; i++){
                        String[] eventTypes = selectNItems(5);
                        String buffer = "PATTERN SEQ("
                                + eventTypes[0] + " v0, "
                                + eventTypes[1] + " v1, "
                                + eventTypes[2] + " v2, "
                                + eventTypes[3] + " v3, "
                                + eventTypes[4] + " v4" +")\n";
                        String rearPart = getQueryRearPart(false, 5, selId);
                        String query = buffer + rearPart;
                        queries.add(query);
                        System.out.println("--> query: \n" + query);
                    }
                }
                case 3 -> {
                    // SEQ(A a, AND(B b, C c), D d)) ==> SEQ(SEQ(A a, AND(B b, C c)), D d)
                    for(int i = 0; i < number; i++){
                        String[] eventTypes = selectNItems(4);
                        String buffer = "PATTERN SEQ(SEQ("
                                + eventTypes[0] + " v0, AND("
                                + eventTypes[1] + " v1, "
                                + eventTypes[2] + " v2)), "
                                + eventTypes[3] + " v3" +")\n";
                        String rearPart = getQueryRearPart(false, 4, selId);
                        String query = buffer + rearPart;
                        queries.add(query);
                        System.out.println("--> query: \n" + query);
                    }
                }
                case 4 -> {
                    // AND(SEQ(A a, B b), SEQ(C c, D d))
                    for(int i = 0; i < number; i++){
                        String[] eventTypes = selectNItems(4);
                        String buffer = "PATTERN AND(SEQ("
                                + eventTypes[0] + " v0, "
                                + eventTypes[1] + " v1), SEQ("
                                + eventTypes[2] + " v2, "
                                + eventTypes[3] + " v3)" +")\n";
                        String rearPart = getQueryRearPart(false, 4, selId);
                        String query = buffer + rearPart;
                        queries.add(query);
                        System.out.println("--> query: \n" + query);
                    }
                }
                case 5 -> {
                    // SEQ(AND(A a, B b),AND(C c, D d))
                    for(int i = 0; i < number; i++){
                        String[] eventTypes = selectNItems(4);
                        String buffer = "PATTERN SEQ(AND("
                                + eventTypes[0] + " v0, "
                                + eventTypes[1] + " v1), AND("
                                + eventTypes[2] + " v2, "
                                + eventTypes[3] + " v3)" +")\n";
                        String rearPart = getQueryRearPart(false, 4, selId);
                        String query = buffer + rearPart;
                        queries.add(query);
                        System.out.println("--> query: \n" + query);
                    }
                }
                default -> throw new RuntimeException("exceeds 5");
            }
        }

        return queries;
    }

    public static void main(String[] args){
        String sep = File.separator;
        SyntheticQueryGenerator qg = new SyntheticQueryGenerator();
        String filePath = System.getProperty("user.dir") + sep + "src" + sep + "main" +
                sep + "java" + sep + "query" + sep + "synthetic_query_sel25.json";

        List<String> queries = qg.generateQueries();
        JSONArray jsonArray = JSONArray.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
