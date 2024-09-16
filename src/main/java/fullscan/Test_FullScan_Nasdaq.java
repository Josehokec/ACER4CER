package fullscan;

import automaton.NFA;
import common.*;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_FullScan_Nasdaq {

    public static void createSchema(){
        String statement = "CREATE TABLE nasdaq (ticker TYPE, open DOUBLE.2,  high DOUBLE.2,  low DOUBLE.2, close DOUBLE.2, vol INT, Date TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different schema creates different object
    public static FullScan createFullScan(){
        String schemaName = "NASDAQ";
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

    // this function support multiple query
    public static void batchQuery(FullScan fullScan, JSONArray jsonArray, MatchEngine engine){
        assert(engine.equals(MatchEngine.NFA));
        int queryNum = jsonArray.size();
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();
            if(queryStatement.contains("COUNT")){
                int cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples= fullScan.processTupleQueryUsingNFA(pattern, new NFA());
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
        assert(engine.equals(MatchEngine.NFA));
        // start querying
        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        long startRunTs = System.currentTimeMillis();
        if(queryStatement.contains("COUNT")){
            int cnt= fullScan.processCountQueryUsingNFA(pattern, new NFA());
            System.out.println("number of tuples: " + cnt);
        }else {
            List<Tuple> tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
            for (Tuple t : tuples) {
                System.out.println(t);
            }
            System.out.println("number of tuples: " + tuples.size());
        }
        long endRunTs = System.currentTimeMillis();
        System.out.println("query cost: " + (endRunTs - startRunTs) + "ms.");
    }

    public static void testExample(FullScan fullScan, MatchEngine engine){
//        String query = """
//                PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
//                FROM NASDAQ
//                USE SKIP_TILL_NEXT_MATCH
//                WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
//                WITHIN 720 units
//                RETURN tuples""";
        String query = """
                PATTERN SEQ(BABA v0, AMZN v1, BABA v2, AMZN v3)
                FROM nasdaq
                USING SKIP_TILL_NEXT_MATCH
                WHERE 90 <= v0.open <= 110 AND 125 <= v1.open <= 145 AND v2.open >= v0.open * 0.01 AND v3.open <= v1.open * 0.99
                WITHIN 900 units
                RETURN tuples""";
        singleQuery(fullScan, query, engine);
    }

    public static void main(String[] args) throws FileNotFoundException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        Test_FullScan_Nasdaq.createSchema();
        // 2. create fullScan Object
        FullScan fullScan = Test_FullScan_Nasdaq.createFullScan();
        // 3. store events
        String filename = "nasdaq.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_FullScan_Nasdaq.storeEvents(dataFilePath, fullScan);

        Test_FullScan_Nasdaq.testExample(fullScan, MatchEngine.NFA);
        System.exit(0);

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "nasdaq_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5. choose NFA engine to match
        System.out.println("use NFA to match");
        MatchEngine engine1 = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "fullscan_nasdaq_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_FullScan_Nasdaq.batchQuery(fullScan, jsonArray, engine1);

    }
}

