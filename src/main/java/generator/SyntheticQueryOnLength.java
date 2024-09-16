package generator;

import net.sf.json.JSONArray;

import java.io.File;
import java.io.FileWriter;
import java.util.*;


/**
 * here we test complex event pattern in synthetic dataset
 * for a random query, it maybe generates a large result
 * so we run experiment delete these queries
 * | `SEQ(A a, B b, C c)`               |
 * | `SEQ(A a, B b, C c, D d, E e)`     |
 * | `SEQ(A a, AND(B b, C c), D d)`     |
 * | `AND(SEQ(A a, B b), SEQ(C c, D d)` |
 * | `SEQ(AND(A a, B b),AND(C c, D d))` |
 */
public class SyntheticQueryOnLength {
    private final int eventTypeNum;
    private final Random random;
    public SyntheticQueryOnLength(int eventTypeNum) {
        this.eventTypeNum = eventTypeNum;
        random = new Random();
    }

    public double[] zipfProbability(double alpha){
        double[] ans = new double[eventTypeNum];
        double C = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            C += Math.pow((1.0 / i), alpha);
        }

        for(int i = 1; i <= eventTypeNum; ++i){
            double pro = 1.0 / (Math.pow(i, alpha) * C);
            ans[i - 1] = pro;
        }

        return ans;
    }

    // binary search
    public int getTypeId(double pro, double[] cdf) {
        int left = 0;
        int right = eventTypeNum - 1;
        int mid = (left + right) >> 1;
        while (left <= right) {
            if (pro <= cdf[mid]) {
                if (mid == 0 || pro > cdf[mid - 1]) {
                    break;
                } else {
                    right = mid - 1;
                    mid = (left + right) >> 1;
                }
            } else {
                if (mid == eventTypeNum - 1) {
                    break;
                } else {
                    left = mid + 1;
                    mid = (left + right) >> 1;
                }
            }
        }
        return mid;
    }

    // get variable event type
    public String[] getVarType(int varNum){
        double[] probability = zipfProbability(1.3);
        // cdf: Cumulative Distribution Function
        double[] cdf = new double[eventTypeNum];
        cdf[eventTypeNum - 1] = 1;
        cdf[0] = probability[0];
        for(int i = 1; i < eventTypeNum - 1; ++i){
            cdf[i] = cdf[i - 1] + probability[i];
        }

        Set<Integer> typeIdSet = new HashSet<>();

        String[] ans = new String[varNum];
        for(int i = 0; i < varNum; ++i){
            double pro = random.nextDouble();
            int typeId = getTypeId(pro, cdf);
            // debug
            // System.out.println("pro: " + pro + " typeId: " + typeId);

            if(typeIdSet.contains(typeId) && typeId < eventTypeNum / 5){
                typeId += eventTypeNum / 5;
            }
            typeIdSet.add(typeId);
            ans[i] = "TYPE_" + typeId;
        }

        return ans;
    }

    // PATTERN SEQ(A a, B b)
    public void generateLength2(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        long window = 1000;

        String[] varNames = {"v0", "v1"};
        int len = varNames.length;
        for(int i = 0; i < queryNUm; ++i){
            String[] eventTypes = getVarType(len);
            // first line
            // SEQ(A a, B b)
            StringBuffer query = new StringBuffer(512);
            query.append("PATTERN SEQ(").append(eventTypes[0]).append(" v0");
            query.append(", ").append(eventTypes[1]).append(" v1").append(")\n");
            // second line
            query.append("FROM synthetic\n");

            query.append("USING SKIP_TILL_NEXT_MATCH\n");

            // three variables
            query.append("WHERE ");
            // first we add ic list
            for(int varId = 0; varId < len; ++ varId){
                int icNum = random.nextInt(1, 4);
                int range = random.nextInt(10, 201);

                for(int j = 0; j < icNum; ++j){
                    int min = random.nextInt(0, 800);
                    int max = min + range;
                    query.append(min).append(" <= v");
                    query.append(varId).append(".a").append(j + 1);
                    query.append(" <= ").append(max).append(" AND ");
                }
            }

            // without dependent constraints
            query.append("\n");

            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");

            // debug
            // System.out.println("query:\n" + query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // PATTERN SEQ(A a, B b, C c)
    public void generateLength3(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        long window = 1000;

        String[] varNames = {"v0", "v1", "v2"};
        int len = varNames.length;

        for(int i = 0; i < queryNUm; ++i){
            String[] eventTypes = getVarType(len);
            // first line
            // SEQ(A a, B b, C c)
            StringBuffer query = new StringBuffer(512);
            query.append("PATTERN SEQ(").append(eventTypes[0]).append(" v0");
            query.append(", ").append(eventTypes[1]).append(" v1");
            query.append(", ").append(eventTypes[2]).append(" v2").append(")\n");
            // second line
            query.append("FROM synthetic\n");

            query.append("USING SKIP_TILL_NEXT_MATCH\n");

            // three variables
            query.append("WHERE ");
            // first we add ic list, selectivity 0.01~0.2
            for(int varId = 0; varId < 3; ++ varId){
                int icNum = random.nextInt(1, 4);
                int range = random.nextInt(10, 201);

                for(int j = 0; j < icNum; ++j){
                    int min = random.nextInt(0, 800);
                    int max = min + range;
                    query.append(min).append(" <= v");
                    query.append(varId).append(".a").append(j + 1);
                    query.append(" <= ").append(max).append(" AND ");
                }
            }

            // without dependent constraints
            query.append("\n");
            
            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");
            // debug
            // System.out.println("query:\n" + query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // PATTERN SEQ(A a, B b, C c, D d)
    public void generateLength4(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        long window = 1000;

        String[] varNames = {"v0", "v1", "v2", "v3"};
        int len = varNames.length;
        for(int i = 0; i < queryNUm; ++i){
            String[] eventTypes = getVarType(len);
            // first line
            // SEQ(A a, B b, C c, D d)
            StringBuffer query = new StringBuffer(512);
            query.append("PATTERN SEQ(").append(eventTypes[0]).append(" v0");
            query.append(", ").append(eventTypes[1]).append(" v1");
            query.append(", ").append(eventTypes[2]).append(" v2");
            query.append(", ").append(eventTypes[3]).append(" v3").append(")\n");
            // second line
            query.append("FROM synthetic\n");

            query.append("USING SKIP_TILL_NEXT_MATCH\n");

            // three variables
            query.append("WHERE ");
            // first we add ic list
            for(int varId = 0; varId < len; ++ varId){
                int icNum = random.nextInt(1, 4);
                int range = random.nextInt(10, 201);

                for(int j = 0; j < icNum; ++j){
                    int min = random.nextInt(0, 800);
                    int max = min + range;
                    query.append(min).append(" <= v");
                    query.append(varId).append(".a").append(j + 1);
                    query.append(" <= ").append(max).append(" AND ");
                }
            }

            query.append("\n");

            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");

            // debug
            // System.out.println("query:\n" + query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // PATTERN SEQ(A a, B b, C c, D d, E e)
    public void generateLength5(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        long window = 1000;

        String[] varNames = {"v0", "v1", "v2", "v3", "v4"};
        int len = varNames.length;
        for(int i = 0; i < queryNUm; ++i){
            String[] eventTypes = getVarType(len);
            // first line
            // SEQ(A a, B b, C c, D d, E e)
            StringBuffer query = new StringBuffer(512);
            query.append("PATTERN SEQ(").append(eventTypes[0]).append(" v0");
            query.append(", ").append(eventTypes[1]).append(" v1");
            query.append(", ").append(eventTypes[2]).append(" v2");
            query.append(", ").append(eventTypes[3]).append(" v3");
            query.append(", ").append(eventTypes[4]).append(" v4").append(")\n");
            // second line
            query.append("FROM synthetic\n");

            query.append("USING SKIP_TILL_NEXT_MATCH\n");


            // three variables
            query.append("WHERE ");
            // first we add ic list
            for(int varId = 0; varId < len; ++ varId){
                int icNum = random.nextInt(1, 4);
                int range = random.nextInt(10, 201);

                for(int j = 0; j < icNum; ++j){
                    int min = random.nextInt(0, 800);
                    int max = min + range;
                    query.append(min).append(" <= v");
                    query.append(varId).append(".a").append(j + 1);
                    query.append(" <= ").append(max).append(" AND ");
                }
            }

            query.append("\n");

            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");

            // debug
            // System.out.println("query:\n" + query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // PATTERN SEQ(A a, B b, C c, D d, E e, F f)
    public void generateLength6(String filePath, int queryNUm){
        List<String> queries = new ArrayList<>(queryNUm);
        long window = 1000;

        String[] varNames = {"v0", "v1", "v2", "v3", "v4", "v5"};
        int len = varNames.length;
        for(int i = 0; i < queryNUm; ++i){
            String[] eventTypes = getVarType(len);
            // first line
            // SEQ(A a, B b, C c, D d, E e)
            StringBuffer query = new StringBuffer(512);
            query.append("PATTERN SEQ(").append(eventTypes[0]).append(" v0");
            query.append(", ").append(eventTypes[1]).append(" v1");
            query.append(", ").append(eventTypes[2]).append(" v2");
            query.append(", ").append(eventTypes[3]).append(" v3");
            query.append(", ").append(eventTypes[4]).append(" v4");
            query.append(", ").append(eventTypes[4]).append(" v5").append(")\n");
            // second line
            query.append("FROM synthetic\n");

            query.append("USING SKIP_TILL_NEXT_MATCH\n");

            // three variables
            query.append("WHERE ");
            // first we add ic list
            for(int varId = 0; varId < len; ++ varId){
                int icNum = random.nextInt(1, 4);
                int range = random.nextInt(10, 201);

                for(int j = 0; j < icNum; ++j){
                    int min = random.nextInt(0, 800);
                    int max = min + range;
                    query.append(min).append(" <= v");
                    query.append(varId).append(".a").append(j + 1);
                    query.append(" <= ").append(max).append(" AND ");
                }
            }
            query.append("\n");

            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");

            // debug
            // System.out.println("query:\n" + query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args){
        SyntheticQueryOnLength qg = new SyntheticQueryOnLength(50);
        // random generate 10000 queries

        String dir = System.getProperty("user.dir");
        String prefix = dir + File.separator + "src" + File.separator + "main" +
                File.separator + "java" + File.separator + "Query" + File.separator + "synthetic_query_length";

        qg.generateLength6(prefix + "6.json", 100);

    }
}
