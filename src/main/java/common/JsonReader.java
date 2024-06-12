package common;

import net.sf.json.JSONArray;

import java.io.*;

public class JsonReader {
    public static String getJson(String filePath) {
        String jsonStr;
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            Reader reader = new InputStreamReader(new FileInputStream(file));
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) {
        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "Query" + File.separator + "stock.json";

        JSONArray jsonarray;

        String array= getJson(filePath);
        jsonarray = JSONArray.fromObject(array);
        System.out.println(jsonarray.getString(1));

    }
}
