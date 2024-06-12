package acer;

import automaton.NFA;
import common.EventPattern;
import common.JsonReader;
import common.MatchEngine;
import common.StatementParser;
import join.GreedyJoin;
import join.OrderJoin;
import join.Tuple;
import method.Index;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_ACER_Nasdaq {

    public static void createSchema(){
        String[] initialStatements = {
                "CREATE TABLE nasdaq (ticker TYPE, open DOUBLE.2,  high DOUBLE.2,  low DOUBLE.2, close DOUBLE.2, vol INT, Date TIMESTAMP)",
                "ALTER TABLE nasdaq ADD CONSTRAINT ticker IN RANGE [0,50]",
                "ALTER TABLE nasdaq ADD CONSTRAINT open IN RANGE [0,4000.00]",
                "ALTER TABLE nasdaq ADD CONSTRAINT vol IN RANGE [0,100000000]"
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

    // different methods using different create index statement
    public static Index createIndex(){
        String createIndexStr = "CREATE INDEX fast_nasdaq USING ACER ON nasdaq(open, vol)";
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        return index;
    }

    // out-of-insertion experiments
    // update and deletion experiments
    public static long storeEvents(String filePath, Index index){
        long startBuildTime = System.currentTimeMillis();
        //Random random = new Random(5);
        // insert record to index
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            String previousLine = null;
            while ((line = b.readLine()) != null) {
                index.insertRecord(line);
                // update operation
                //int rd = random.nextInt(100);
                //if(rd >= 90){
                    //update
                    //if(previousLine != null){
                        //index.insertRecord(line);
                    //}
                //}else if(rd >= 80){
                    //index.delete(line);
                //}else if(rd < 5){
                    //previousLine = line;
                //}
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        //System.exit(0);
        long endBuildTime = System.currentTimeMillis();
        return endBuildTime - startBuildTime;
    }

    // this function only support a query
    @SuppressWarnings("unused")
    public static void indexQuery(Index index, String queryStatement, MatchEngine engine){
        System.out.println(queryStatement);
        // start query
        long startRunTs = System.currentTimeMillis();
        if(queryStatement.contains("COUNT")){
            int cnt;
            switch (engine) {
                case NFA -> {
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                }
                case OrderJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    cnt = index.processCountQueryUsingJoin(pattern, new OrderJoin());
                }
                case GreedyJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    cnt = index.processCountQueryUsingJoin(pattern, new GreedyJoin());
                }
                default -> {
                    System.out.println("we use nfa as default match engine");
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                }
            }
            System.out.println("number of tuples: " + cnt);
        }else {
            List<Tuple> tuples;
            switch (engine) {
                case NFA -> {
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
                }
                case OrderJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    tuples = index.processTupleQueryUsingJoin(pattern, new OrderJoin());
                }
                case GreedyJoin -> {
                    EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                    tuples = index.processTupleQueryUsingJoin(pattern, new GreedyJoin());
                }
                default -> {
                    System.out.println("we use nfa as default match engine");
                    QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                    tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
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

    // this function support multiple query
    public static void indexBatchQuery(Index index, JSONArray jsonArray, MatchEngine engine){
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
                        cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                    }
                    case OrderJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        cnt = index.processCountQueryUsingJoin(pattern, new OrderJoin());
                    }
                    case GreedyJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        cnt = index.processCountQueryUsingJoin(pattern, new GreedyJoin());
                    }
                    default -> {
                        System.out.println("we use nfa as default match engine");
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                    }
                }
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples;
                switch (engine) {
                    case NFA -> {
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
                    }
                    case OrderJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        tuples = index.processTupleQueryUsingJoin(pattern, new OrderJoin());
                    }
                    case GreedyJoin -> {
                        EventPattern pattern = StatementParser.getEventPattern(queryStatement);
                        tuples = index.processTupleQueryUsingJoin(pattern, new GreedyJoin());
                    }
                    default -> {
                        System.out.println("we use nfa as default match engine");
                        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                        tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
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

    public static void testExample(Index index, MatchEngine engine){
        String query = """
                PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
                WITHIN 720 units
                RETURN tuples""";
        indexQuery(index, query, engine);
    }

    public static void main(String[] args) throws FileNotFoundException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        Test_ACER_Nasdaq.createSchema();
        // 2. create index
        Index index = Test_ACER_Nasdaq.createIndex();
        // 3. store events
        String filename = "nasdaq.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_ACER_Nasdaq.storeEvents(dataFilePath, index);

        // example test...
        // Test_ACER_Nasdaq.testExample(index, MatchEngine.NFA);
        // System.exit(0);

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "nasdaq_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5.1 choose NFA engine to match
        final PrintStream consoleOut = System.out;
        System.out.println("use NFA to match");
        MatchEngine engine1 = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "acer_nasdaq_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost + "ms");
        Test_ACER_Nasdaq.indexBatchQuery(index, jsonArray, engine1);

        // 5.2 choose OrderJoin engine to match
        System.setOut(consoleOut);
        System.out.println("use OrderJoin to match");
        MatchEngine engine2 = MatchEngine.OrderJoin;
        PrintStream printStream2 = new PrintStream(prefixPath + "output" + sep + "acer_nasdaq_join.txt");
        System.setOut(printStream2);
        System.out.println("choose OrderJoin engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_ACER_Nasdaq.indexBatchQuery(index, jsonArray, engine2);
    }
}