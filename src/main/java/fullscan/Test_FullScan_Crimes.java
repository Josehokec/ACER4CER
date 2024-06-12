package fullscan;

import automaton.NFA;
import common.*;
import join.GreedyJoin;
import join.OrderJoin;
import join.Tuple;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_FullScan_Crimes {

    public static void createSchema(){
        String[] initialStatements = {
                "CREATE TABLE crimes (PrimaryType TYPE, ID INT,  Beat INT,  District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)",
                "ALTER TABLE crimes ADD CONSTRAINT PrimaryType IN RANGE [0,50]",
                "ALTER TABLE crimes ADD CONSTRAINT Beat IN RANGE [0,3000]",
                "ALTER TABLE crimes ADD CONSTRAINT District IN RANGE [0,50]",
                "ALTER TABLE crimes ADD CONSTRAINT Latitude IN RANGE [36.6,42.1]",
                "ALTER TABLE crimes ADD CONSTRAINT Longitude IN RANGE [-91.7,-87.5]"
        };

        // create schema and define attribute range
        for(String statement : initialStatements){
            String str = StatementParser.convert(statement);
            String[] words = str.split(" ");
            if(words[0].equals("ALTER")){
                StatementParser.setAttrValueRange(str);
            } else if (words[0].equals("CREATE") && words[1].equals("TABLE")){
                StatementParser.createTable(str);
            }
        }
    }

    // different schema creates different object
    public static FullScan createFullScan(){
        String schemaName = "CRIMES";
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        return new FullScan(schema);
    }

    public static long storeEvents(String filePath, FullScan fullScan){
        long startBuildTime = System.currentTimeMillis();
        // insert record to index
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            while ((line = b.readLine()) != null) {
                fullScan.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        long endBuildTime = System.currentTimeMillis();
        return endBuildTime - startBuildTime;
    }

    public static void batchQuery(FullScan fullScan, JSONArray jsonArray, MatchEngine engine){
        int queryNum = jsonArray.size();
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();
            if(queryStatement.contains("COUNT")){
                int cnt;
                switch (engine) {
                    case NFA -> {
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                    }
                    case OrderJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        cnt = fullScan.processCountQueryUsingJoin(pattern, new OrderJoin());
                    }
                    case GreedyJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        cnt = fullScan.processCountQueryUsingJoin(pattern, new GreedyJoin());
                    }
                    default -> {
                        System.out.println("we use nfa as default match engine");
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                    }
                }
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples;
                switch (engine) {
                    case NFA -> {
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
                    }
                    case OrderJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        tuples = fullScan.processTupleQueryUsingJoin(pattern, new OrderJoin());
                    }
                    case GreedyJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        tuples = fullScan.processTupleQueryUsingJoin(pattern, new GreedyJoin());
                    }
                    default -> {
                        System.out.println("we use nfa as default match engine");
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
                    }
                }
                for (Tuple t : tuples) {
                    System.out.println(t);
                }
                System.out.println("number of tuples: " + tuples.size());
            }
            long endRunTs = System.currentTimeMillis();
            System.out.println(i + "-th query cost: " + (endRunTs - startRunTs) + "ms.");
        }
    }

    // this function only support a query
    @SuppressWarnings("unused")
    public static void singleQuery(FullScan fullScan, String queryStatement, MatchEngine engine){
        // start querying
        long startRunTs = System.currentTimeMillis();
        if(queryStatement.contains("COUNT")){
            int cnt;
            switch (engine) {
                case NFA -> {
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                }
                case OrderJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    cnt = fullScan.processCountQueryUsingJoin(pattern, new OrderJoin());
                }
                case GreedyJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    cnt = fullScan.processCountQueryUsingJoin(pattern, new GreedyJoin());
                }
                default -> {
                    System.out.println("we use nfa as default match engine");
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                }
            }
            System.out.println("number of tuples: " + cnt);
        }else {
            List<Tuple> tuples;
            switch (engine) {
                case NFA -> {
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
                }
                case OrderJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    tuples = fullScan.processTupleQueryUsingJoin(pattern, new OrderJoin());
                }
                case GreedyJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    tuples = fullScan.processTupleQueryUsingJoin(pattern, new GreedyJoin());
                }
                default -> {
                    System.out.println("we use nfa as default match engine");
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
                }
            }
            for (Tuple t : tuples) {
                System.out.println(t);
            }
            System.out.println("number of tuples: " + tuples.size());
        }
        long endRunTs = System.currentTimeMillis();
        System.out.println("query cost: " + (endRunTs - startRunTs) + "ms.");
    }

    public static void testExample(FullScan fullScan, MatchEngine engine){
        String query1 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 13 <= v0.District <= 14 AND 12 <= v1.District <= 15 AND 12 <= v2.District <= 15
                WITHIN 1800 units
                RETURN matched_tuples""";
        //singleQuery(fullScan, query1, engine);

        String query2 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 2394 <= v0.Beat <= 2648 AND 2394 <= v1.Beat <= 2648  AND 2394 <= v2.Beat <= 2648
                WITHIN 1800 units
                RETURN matched_tuples""";
        //singleQuery(fullScan, query2, engine);


        String query3 = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE -87.7 <= v0.Longitude <= -87.6 AND 41.8 <= v0.Latitude <= 41.9 AND -87.7 <= v1.Longitude <= -87.6 AND 41.8 <= v1.Latitude <= 41.9 AND -87.7 <= v2.Longitude <= -87.6 AND 41.8 <= v2.Latitude <= 41.9
                WITHIN 1800 units
                RETURN matched_tuples""";
        singleQuery(fullScan, query3, engine);
    }


    public static void main(String[] args) throws FileNotFoundException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        // 1. create table, alter attribute range
        Test_FullScan_Crimes.createSchema();
        // 2. create fullScan Object
        FullScan fullScan = Test_FullScan_Crimes.createFullScan();
        // 3. store events
        String filename = "crimes.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_FullScan_Crimes.storeEvents(dataFilePath, fullScan);
        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "crimes_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // Test_FullScan_Crimes.testExample(fullScan, MatchEngine.NFA);
        // System.exit(0);

        // 5.1 choose NFA engine to match
        final PrintStream consoleOut = System.out;
        System.out.println("use NFA to match");
        MatchEngine engine1 = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "fullscan_crimes_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_FullScan_Crimes.batchQuery(fullScan, jsonArray, engine1);

        // 5.2 choose OrderJoin engine to match
        System.setOut(consoleOut);
        System.out.println("use OrderJoin to match");
        MatchEngine engine2 = MatchEngine.OrderJoin;
        PrintStream printStream2 = new PrintStream(prefixPath + "output" + sep + "fullscan_crimes_join.txt");
        System.setOut(printStream2);
        System.out.println("choose OrderJoin engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_FullScan_Crimes.batchQuery(fullScan, jsonArray, engine2);
    }

}
