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

class Test_ACER_Cluster {

    public static void createSchema(){
//        String[] initialStatements = {
//                "CREATE TABLE job (EventType TYPE, jobID FLOAT.0,  schedulingClass INT, timestamp TIMESTAMP)",
//                "ALTER TABLE job ADD CONSTRAINT EventType IN RANGE [0,30]",
//                "ALTER TABLE job ADD CONSTRAINT schedulingClass IN RANGE [0,7]"
//        };
//
//        // create schema and define attribute range
//        for(String statement : initialStatements){
//            String str = StatementParser.convert(statement);
//            String[] words = str.split(" ");
//            if(words[0].equals("ALTER")){
//                StatementParser.setAttrValueRange(str);
//            } else if (words[0].equals("CREATE") && words[1].equals("TABLE")){
//                StatementParser.createTable(str);
//            }
//        }
        String statement = "CREATE TABLE job (EventType TYPE, jobID FLOAT.0,  schedulingClass INT, timestamp TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different methods using different create index statement
    public static Index createIndex(){
        String createIndexStr = "CREATE INDEX fast_cluster USING ACER ON job(schedulingClass)";
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
            while ((line = b.readLine()) != null) {
                index.insertOrDeleteRecord(line, false);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        long endBuildTime = System.currentTimeMillis();
        // System.out.println("build cots: " + (endBuildTime - startBuildTime) +"ms");
        return endBuildTime - startBuildTime;
    }

    // this function only support a query
    @SuppressWarnings("unused")
    public static void indexQuery(Index index, String queryStatement, MatchEngine engine){
        System.out.println(queryStatement);
        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        // start query
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

    public static void main(String[] args) throws FileNotFoundException{
        // note that we implement out-of-order insertion in rebuttal stage (see ACER.java)
        // then it only returns an event if multiple events have same type and timestamp
        // in order to obtain same output results compared with FullScan
        // you need to remove out-of-order insertion in ACER.java
        // to the best of our knowledge, our code is bug-free

        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table
        Test_ACER_Cluster.createSchema();
        // 2. create index
        Index index = Test_ACER_Cluster.createIndex();
        // 3. store events
        String filename = "job.csv";   // 'job' = 'cluster'
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_ACER_Cluster.storeEvents(dataFilePath, index);
        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "job_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5 choose NFA engine to match
        System.out.println("use NFA to match");
        MatchEngine nfaEngine = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "acer_job_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_ACER_Cluster.indexBatchQuery(index, jsonArray, nfaEngine);

    }
}
