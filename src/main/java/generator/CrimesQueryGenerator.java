package generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CrimesQueryGenerator {
    public void generateQuery(String filePath, int queryNum){
        Random random = new Random();
        List<String> queries = new ArrayList<>(queryNum);

        // query window is 30 minutes -> 1800 seconds
        long windows = 1800;
        // set min.max value for beat, district,  latitude and longitude attributes
        double[] minValues = {200, 2, 41.80, -87.63};
        double[] maxValues = {2500, 30, 41.84, -87.59};

        for(int i = 0; i < queryNum; ++i){
            StringBuilder query = new StringBuilder(256);
            int patternLen = 3;
            query.append("PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)\n");
            query.append("FROM crimes\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");

            int queryType = random.nextInt(3);

            switch (queryType){
                case 0 ->{
                    // beat attribute
                    // give v0 beat range
                    // v1 beat, v2 beat is closed to v0 beat -> [v0.beat - 20, v0.beat + 20]
                    double startBeat = random.nextDouble(minValues[0], maxValues[0]);
                    int minBeat = (int) startBeat;
                    int maxBeat = minBeat + 30;
                    query.append(minBeat).append(" <= v0.Beat <= ").append(maxBeat + 30);

                    query.append(" AND v1.Beat >= ").append("v0.Beat - 20");
                    query.append(" AND v1.Beat <= ").append("v0.Beat + 20");
                    query.append(" AND v2.Beat >= ").append("v0.Beat - 20");
                    query.append(" AND v2.Beat <= ").append("v0.Beat + 20");

                    // here we define some redundant independent predicates
                    // it can be view that query rewrite
                    query.append(" AND ").append(minBeat - 20).append(" <= v1.Beat <= ").append(maxBeat + 20);
                    query.append(" AND ").append(minBeat - 20).append(" <= v2.Beat <= ").append(maxBeat + 20);
                    query.append("\n");
                }
                case 1 -> {
                    // district attribute
                    // give v0 district range
                    // v1 district, v2 district is closed to v0 district -> [v0.district - 1, v0.district + 1]
                    double startDistrict = random.nextDouble(minValues[1], maxValues[1]);
                    int minDistrict = (int) startDistrict;
                    int maxDistrict = minDistrict + 1;
                    query.append(minDistrict).append(" <= v0.District <= ").append(maxDistrict);

                    query.append(" AND v1.District >= ").append("v0.District - 1");
                    query.append(" AND v1.District <= ").append("v0.District + 1");
                    query.append(" AND v2.District >= ").append("v0.District - 1");
                    query.append(" AND v2.District <= ").append("v0.District + 1");

                    // here we define some redundant independent predicates
                    // it can be view that query rewrite
                    query.append(" AND ").append(minDistrict - 1).append(" <= v1.District <= ").append(maxDistrict + 1);
                    query.append(" AND ").append(minDistrict - 1).append(" <= v2.District <= ").append(maxDistrict + 1);
                    query.append("\n");
                }
                case 2 -> {
                    // longitude and latitude attributes
                    // give v0 longitude and latitude ranges
                    // v1 longitude, v2 longitude is closed to v0 longitude -> [v0.longitude - 0.05, v0.longitude + 0.05]
                    // v2 Latitude, v2 Latitude is closed to v0 Latitude -> [v0.Latitude - 0.02, v0.Latitude + 0.02]
                    double startLongitude = random.nextDouble(-87.7, -87.6);
                    String minLongitude = String.format("%.9f", startLongitude);
                    String maxLongitude = String.format("%.9f", startLongitude + 0.02);
                    query.append(minLongitude).append(" <= v0.Longitude <= ").append(maxLongitude);

                    double startLatitude = random.nextDouble(41.80, 41.84);
                    String minLatitude = String.format("%.9f", startLatitude);
                    String maxLatitude = String.format("%.9f", startLatitude + 0.01);
                    query.append(" AND ").append(minLatitude).append(" <= v0.Latitude <= ").append(maxLatitude);


                    query.append(" AND v1.Longitude >= ").append("v0.Longitude - 0.05");
                    query.append(" AND v1.Longitude <= ").append("v0.Longitude + 0.05");
                    query.append(" AND v1.Latitude >= ").append("v0.Latitude - 0.02");
                    query.append(" AND v1.Latitude <= ").append("v0.Latitude + 0.02");
                    query.append(" AND v2.Longitude >= ").append("v0.Longitude - 0.05");
                    query.append(" AND v2.Longitude <= ").append("v0.Longitude + 0.05");
                    query.append(" AND v2.Latitude >= ").append("v0.Latitude - 0.02");
                    query.append(" AND v2.Latitude <= ").append("v0.Latitude + 0.02");

                    // here we define some redundant independent predicates
                    // it can be view that query rewrite
                    String minV1V2Longitude = String.format("%.9f", startLongitude - 0.05);
                    String maxV1V2Longitude = String.format("%.9f", startLongitude + 0.02 + 0.05);

                    String minV1V2Latitude = String.format("%.9f", startLatitude - 0.02);
                    String maxV1V2Latitude = String.format("%.9f", startLatitude + 0.01 + 0.02);

                    query.append(" AND ").append(minV1V2Longitude).append(" <= v1.Longitude <= ").append(maxV1V2Longitude);
                    query.append(" AND ").append(minV1V2Latitude).append(" <= v1.Latitude <= ").append(maxV1V2Latitude);
                    query.append(" AND ").append(minV1V2Longitude).append(" <= v2.Longitude <= ").append(maxV1V2Longitude);
                    query.append(" AND ").append(minV1V2Latitude).append(" <= v2.Latitude <= ").append(maxV1V2Latitude);
                    query.append("\n");
                }
            }
            query.append("WITHIN ").append(windows).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());
        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter = null;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
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

    public static void main(String[] args){
        CrimesQueryGenerator cqg = new CrimesQueryGenerator();
        // random generate 300 queries
        cqg.generateQuery("crimes_query.json", 500);
    }
}
