package fullscan;

import automaton.NFA;
import common.*;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_FullScan_Cluster {

    public static void createSchema(){
        String statement = "CREATE TABLE job (EventType TYPE, jobID FLOAT.0,  schedulingClass INT, timestamp TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different schema creates different object
    public static FullScan createFullScan(){
        String schemaName = "JOB";
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
        int queryNum = jsonArray.size();
        assert(engine.equals(MatchEngine.NFA));
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();
            if(queryStatement.contains("COUNT")){
                int cnt;
                QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
                cnt = fullScan.processCountQueryUsingNFA(pattern, new NFA());
                System.out.println("number of tuples: " + cnt);
            }else {
                List<Tuple> tuples;
                QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
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

    public static void main(String[] args) throws FileNotFoundException{
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        Test_FullScan_Cluster.createSchema();
        // 2. create fullScan Object
        FullScan fullScan = Test_FullScan_Cluster.createFullScan();
        // 3. store events
        String filename = "job.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_FullScan_Cluster.storeEvents(dataFilePath, fullScan);
        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "job_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5. choose NFA engine to match
        System.out.println("use NFA to match");
        MatchEngine nfaEngine = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "fullscan_job_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_FullScan_Cluster.batchQuery(fullScan, jsonArray, nfaEngine);
    }
}
