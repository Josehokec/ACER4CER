package acer;

import automaton.NFA;

import common.JsonReader;
import common.MatchEngine;
import common.StatementParser;
import common.Tuple;
import method.Index;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_ACER_Crimes {

    public static void createSchema(){
//        String[] initialStatements = {
//                "CREATE TABLE crimes (PrimaryType TYPE, ID INT,  Beat INT,  District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)",
//                "ALTER TABLE crimes ADD CONSTRAINT PrimaryType IN RANGE [0,50]",
//                "ALTER TABLE crimes ADD CONSTRAINT Beat IN RANGE [0,3000]",
//                "ALTER TABLE crimes ADD CONSTRAINT District IN RANGE [0,50]",
//                "ALTER TABLE crimes ADD CONSTRAINT Latitude IN RANGE [36.6,42.1]",
//                "ALTER TABLE crimes ADD CONSTRAINT Longitude IN RANGE [-91.7,-87.5]"
//        };

        String statement = "CREATE TABLE crimes (PrimaryType TYPE, ID INT,  Beat INT,  District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different methods using different create index statement
    public static Index createIndex(){
        String createIndexStr = "CREATE INDEX fast_crimes USING ACER ON crimes(Beat, District, Latitude, Longitude)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        return index;
    }

    public static long storeEvents(String filePath, Index index){
        long startBuildTime = System.currentTimeMillis();
        // insert record to index
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            // String previousLine = null;
            while ((line = b.readLine()) != null) {
                index.insertOrDeleteRecord(line, false);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        long endBuildTime = System.currentTimeMillis();
        return endBuildTime - startBuildTime;
    }

    // this function only support a query
    @SuppressWarnings("unused")
    public static void indexQuery(Index index, String queryStatement, MatchEngine engine){
        // System.out.println(queryStatement);
        // start query
        long startRunTs = System.currentTimeMillis();
        if(queryStatement.contains("COUNT")){
            int cnt;
            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            cnt = index.processCountQueryUsingNFA(pattern, new NFA());
            System.out.println("number of tuples: " + cnt);
        }else {
            List<Tuple> tuples;
            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
            for (Tuple t : tuples) {
                System.out.println(t);
            }
            System.out.println("number of tuples: " + tuples.size());
        }
        long endRunTs = System.currentTimeMillis();
        System.out.println("query cost: " + (endRunTs - startRunTs) + "ms.");
    }

    // this function support multiple query
    public static void indexBatchQuery(Index index, JSONArray jsonArray, MatchEngine engine){
        int queryNum = jsonArray.size();
        assert(engine.equals(MatchEngine.NFA));
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();
            if(queryStatement.contains("COUNT")){
                int cnt;
                cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples;
                tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
                for (Tuple t : tuples) {
                    System.out.println(t);
                }
                System.out.println("number of tuples: " + tuples.size());
            }
            long endRunTs = System.currentTimeMillis();
            System.out.println(i + "-th query cost: " + (endRunTs - startRunTs) + "ms.");
        }
    }

    // rebuttal experiments
    public static void indexQuery4Negation(Index index){
        String query1 = """
                PATTERN SEQ(ROBBERY v0, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 13 <= v0.district <= 14 AND 12 <= v2.District <= 15
                WITHIN 1800 units
                RETURN COUNT(*)""";

        String query2 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 13 <= v0.district <= 14 AND v1.district > 15 AND 12 <= v2.District <= 15
                WITHIN 1800 units
                RETURN COUNT(*)""";

        long startRunTs = System.currentTimeMillis();
        QueryPattern pattern1 = StatementParser.getQueryPattern(query1);
        List<Tuple> tuples1  = index.processTupleQueryUsingNFA(pattern1, new NFA());
        System.out.println("tuples1 size: " + tuples1.size());

        QueryPattern pattern2 = StatementParser.getQueryPattern(query2);
        List<Tuple> tuples2  = index.processTupleQueryUsingNFA(pattern2, new NFA());
        System.out.println("tuples2 size: " + tuples2.size());

        NegationProcessor processor = new NegationProcessor(tuples1, tuples2);
        List<Tuple> tuples = processor.getResult(1);

        for (Tuple t : tuples) {
            System.out.println(t);
        }
        System.out.println("number of tuples: " + tuples.size());

        long endRunTs = System.currentTimeMillis();
        System.out.println("query cost: " + (endRunTs - startRunTs) + "ms.");
    }

    public static void testExample(Index index, MatchEngine engine){
        String query1 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 13 <= v0.District <= 14 AND 12 <= v1.District <= 15 AND 12 <= v2.District <= 15
                WITHIN 1800 units
                RETURN COUNT(*)""";
        indexQuery(index, query1, engine);

        String query2 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 2394 <= v0.Beat <= 2648 AND 2394 <= v1.Beat <= 2648  AND 2394 <= v2.Beat <= 2648
                WITHIN 1800 units
                RETURN COUNT(*)""";
        indexQuery(index, query2, engine);


        String query3 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE -87.7 <= v0.Longitude <= -87.6 AND 41.8 <= v0.Latitude <= 41.9 AND -87.7 <= v1.Longitude <= -87.6 AND 41.8 <= v1.Latitude <= 41.9 AND -87.7 <= v2.Longitude <= -87.6 AND 41.8 <= v2.Latitude <= 41.9
                WITHIN 1800 units
                RETURN COUNT(*)""";
        indexQuery(index, query3, engine);
    }

    public static void main(String[] args) throws FileNotFoundException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        Test_ACER_Crimes.createSchema();
        // 2. create index
        Index index = Test_ACER_Crimes.createIndex();
        // 3. store events
        String filename = "crimes.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_ACER_Crimes.storeEvents(dataFilePath, index);
        System.out.println("buildCost: " + buildCost + "ms");
        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "crimes_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // Test_FAST_Crimes.testExample(index, MatchEngine.NFA);
        // Test_ACER_Crimes.indexQuery4Negation(index);

        // 5. choose NFA engine to match
        System.out.println("use NFA to match");
        MatchEngine engine1 = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "acer_crimes_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_ACER_Crimes.indexBatchQuery(index, jsonArray, engine1);
    }
}