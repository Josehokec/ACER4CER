package query;

import common.JsonReader;
import net.sf.json.JSONArray;

import java.io.File;
import java.io.FileWriter;
import java.util.*;


/**
 * this class aims to generate 500 queries for NASDAQ dataset
 */

public class DeleteSameQuery {

    public static void main(String[] args){
        Random random = new Random(2);
        String prefixPath = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main" + File.separator;
        String jsonFilePath = prefixPath + "java" + File.separator + "Query" + File.separator + "nasdaq_query_py.json";

        String jsonStr = JsonReader.getJson(jsonFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonStr);

        int queryNum = jsonArray.size();

        Set<String> querySet= new HashSet<>();
        // "PATTERN SEQ(MSFT v0, GOOG v1, MSFT v2, GOOG v3)\n
        // FROM nasdaq\nUSING SKIP_TILL_NEXT_MATCH\n
        // WHERE 275 <= v0.open <= 285 AND 105 <= v1.open <= 115 AND v2.open >= v0.open * 1.007 AND v3.open <= v1.open * 0.993\n
        // WITHIN 900 units\nRETURN tuples"
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            // if querySet has same query, then replace
            if(querySet.contains(queryStatement)){
                double[] scales = new double[]{0.005, 0.006, 0.008, 0.009, 0.01};
                int idx = random.nextInt(0, scales.length);
                queryStatement = queryStatement.replaceAll("1.007", String.valueOf(scales[idx]));
                queryStatement = queryStatement.replaceAll("0.993", String.valueOf(1 - scales[idx]));
            }
            querySet.add(queryStatement);
        }



        List<String> queries = new ArrayList<>(querySet.size());
        int cnt = 0;
        for(String query : querySet){
            queries.add(query);
            cnt++;
            if(cnt == 500){
                break;
            }
        }

        JSONArray newJsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter;
        try{
            fileWriter = new FileWriter("nasdaq_query.json");
            fileWriter.write(newJsonArray.toString());
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("new query number: " + queries.size());
    }
}
