package arrival;

import common.JsonReader;
import net.sf.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;

public class JsonMap {

    /**
     * map structure -> json file
     * @param map    map structure
     */
    public static void arrivalMapToJson(HashMap<String, Double> map, String filePath){
        File file = new File(filePath);
        if(file.exists()){
            return;
        }
        JSONObject jsonMap = JSONObject.fromObject(map);
        FileWriter fileWriter = null;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonMap.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                fileWriter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static HashMap<String, Double> jsonToArrivalMap(String filePath){
        HashMap<String, Double> ans = new HashMap<>();
        String jsonStr = JsonReader.getJson(filePath);
        JSONObject jsonObject = JSONObject.fromObject(jsonStr);

        Iterator it = jsonObject.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            double value = (double) jsonObject.get(key);
            ans.put(key, value);
        }

        return ans;
    }

    public static void typeMapToJsonFile(HashMap<String, Integer> map, String filePath){
        File file = new File(filePath);
        if(file.exists()){
            return;
        }
        JSONObject jsonMap = JSONObject.fromObject(map);
        FileWriter fileWriter = null;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonMap.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                fileWriter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static HashMap<String, Integer> jsonToTypeMap(String filePath){
        HashMap<String, Integer> ans = new HashMap<>();
        String jsonStr = JsonReader.getJson(filePath);
        JSONObject jsonObject = JSONObject.fromObject(jsonStr);

        Iterator it = jsonObject.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            int value = (int) jsonObject.get(key);
            ans.put(key, value);
        }

        return ans;
    }
}
