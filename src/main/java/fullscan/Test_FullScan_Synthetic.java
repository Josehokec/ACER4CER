package fullscan;

import automaton.NFA;
import common.*;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

public class Test_FullScan_Synthetic {

    public static void createSchema(){
        String statement = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different schema creates different object
    public static FullScan createFullScan(){
        String schemaName = "SYNTHETIC";
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
        // int queryNum = jsonArray.size();
        for(int i = 0; i < 100; ++i) {
            String queryStatement = jsonArray.getString(i);
            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();
            if(queryStatement.contains("COUNT")){
                int cnt;
                cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples;
                tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
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
        long startRunTs = System.currentTimeMillis();
        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);

        if(queryStatement.contains("COUNT")){
            int cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
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


    public static void main(String[] args) throws FileNotFoundException {
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        Test_FullScan_Synthetic.createSchema();
        // 2. create fullScan Object
        FullScan fullScan = Test_FullScan_Synthetic.createFullScan();
        // 3. store events
        // 3. store events, datasets: synthetic10M, synthetic100M, synthetic1G
        String[] datasets = {"synthetic10M", "synthetic100M", "synthetic1G"};
        String filename = datasets[0];
        String dataFilePath = prefixPath + "dataset" + sep + filename + ".csv";
        long buildCost = Test_FullScan_Synthetic.storeEvents(dataFilePath, fullScan);

        // we have defined five types of event patterns to query
        // here we use loop to query all
        String[] queryFileNames = {"synthetic_query_pattern1", "synthetic_query_pattern2",
                "synthetic_query_pattern3", "synthetic_query_pattern4", "synthetic_query_pattern5"};

        for(int i = 0; i < 1; ++i){
            // 4. read query file
            String queryFilePath = prefixPath + "java" + sep + "Query" + sep + queryFileNames[i] + ".json";
            String jsonFileContent = JsonReader.getJson(queryFilePath);
            JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

            // currently, only NFA support complex event pattern query,
            // so we choose NFA engine to match
            System.out.println("use NFA to match");
            MatchEngine engine = MatchEngine.NFA;
            String outputFilePath = prefixPath + "output" + sep + "fullscan_" + filename + "_nfa_query" + (i+1) + ".txt";
            PrintStream printStream = new PrintStream(outputFilePath);
            System.setOut(printStream);
            System.out.println("choose NFA engine to match");
            System.out.println("build cost: " + buildCost +"ms");
            Test_FullScan_Synthetic.batchQuery(fullScan, jsonArray, engine);
        }
    }
}
