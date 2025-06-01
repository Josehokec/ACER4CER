package baselines;

import automaton.NFA;

import common.JsonReader;
import common.StatementParser;
import automaton.Tuple;
import generator.DataGenerator;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

class Test_IntervalScan {

    /**
     * [updated]
     * @param datasetName       create index for different dataset (filename)
     */
    private static void createSchema(String datasetName){
        String statement;
        switch (datasetName) {
            case "crimes" -> statement =
                    "CREATE TABLE crimes (PrimaryType TYPE, ID INT,  Beat INT,  District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)";
            case "nasdaq" -> statement =
                    "CREATE TABLE nasdaq (ticker TYPE, open DOUBLE.2,  high DOUBLE.2,  low DOUBLE.2, close DOUBLE.2, vol INT, Date TIMESTAMP)";
            case "job" -> statement =
                    "CREATE TABLE job (timestamp TIMESTAMP, jobID FLOAT.0, eventType TYPE, username CHAR[44], schedulingClass INT, jobName CHAR[44])";
            case "synthetic" -> statement =
                    "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.1, a4 DOUBLE.1, time TIMESTAMP)";
            default -> throw new RuntimeException("create schema fail, cannot support this filename...");
        }
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    // different methods using different create index statement
    private static baselines.Index createIndex(String datasetName){
        String createIndexStr;
        switch (datasetName) {
            case "crimes" -> createIndexStr =
                    "CREATE INDEX crimes_interval_index USING INTERVAL_SCAN ON crimes(Beat, District, Latitude, Longitude)";
            case "nasdaq" -> createIndexStr =
                    "CREATE INDEX nasdaq_interval_index USING INTERVAL_SCAN ON nasdaq(open, vol)";
            case "job" -> createIndexStr =
                    "CREATE INDEX job_interval_scan USING INTERVAL_SCAN ON job(schedulingClass)";
            case "synthetic" -> createIndexStr =
                    "CREATE INDEX synthetic_interval_scan USING INTERVAL_SCAN ON synthetic(a1, a2, a3, a4)";
            default -> throw new RuntimeException("create index fail, cannot support this filename...");
        }
        String str = StatementParser.convert(createIndexStr);
        baselines.Index index = StatementParser.createIndex(str);
        index.initial();
        return index;
    }

    private static long storeEvents(String filePath, baselines.Index index){
        long startBuildTime = System.nanoTime();
        int eventCount = 0;
        // insert record to index
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            // delete first line
            String line;
            b.readLine();
            while ((line = b.readLine()) != null) {
                index.insertRecord(line, false);
                eventCount++;
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
        long endBuildTime = System.nanoTime();
        System.out.println("average insertion latency: " + (endBuildTime - startBuildTime) / eventCount + "ns");
        // ns -> ms
        return (endBuildTime - startBuildTime) / 1_000_000;
    }

    /**
     * this function is provided by synthetic datasets
     * skew = 1.2, typeNum = 20
     * zipf probability:
     *          [0.3498,0.1523,0.0936,0.0663,0.0507,0.0407,0.0339,0.0288,0.0250,0.0221,
     *          0.0197,0.0177,0.0161,0.0147,0.0136,0.0126,0.0117,0.0109,0.0102,0.0096]
     * @param eventNum          number of events
     * @param batchSize         batch size to insert index [256 or 512]
     * @param enableUpdate      with/without update & deletion operation
     * @param ratio             ratio of update and deletion
     * @param index             index
     * @return                  overehad build overhead
     */
    private static long storeEvents(int eventNum, int batchSize, boolean inOrder, boolean enableUpdate, double ratio, baselines.Index index){
        long startBuildTime = System.nanoTime();
        int eventCount = 0;
        // 6 attributes: type, a1-a4, timestamp; a1, a2 ~ U[1, 1000]; a3, a4 ~ U[0, 10000]
        String[] attrDataTypes = {"INT_UNIFORM", "INT_UNIFORM", "DOUBLE_UNIFORM", "DOUBLE_UNIFORM"};
        DataGenerator generator = new DataGenerator(attrDataTypes);

        int batchNum = eventNum / batchSize;
        for (int i = 0; i < batchNum; i++) {
            List<String[]> batchRecords = generator.generateDataInBatch(batchSize, inOrder);
            if(enableUpdate){
                int updateNum = (int) (ratio * batchSize);
                for(int e = 0; e < updateNum; e++){
                    String[] updateRecord = new String[attrDataTypes.length + 2];
                    updateRecord[0] = batchRecords.get(e)[0];
                    updateRecord[1] = "500";
                    updateRecord[2] = "500";
                    updateRecord[3] = "5000";
                    updateRecord[4] = "5000";
                    updateRecord[5] = batchRecords.get(e)[5];
                    batchRecords.add(updateRecord);
                }
            }
            index.insertBatchRecord(batchRecords, enableUpdate);
            eventCount += batchRecords.size();
        }
        long endBuildTime = System.nanoTime();
        System.out.println("average insertion latency: " + (endBuildTime - startBuildTime) / eventCount + "ns");
        // ns -> ms
        return (endBuildTime - startBuildTime) / 1_000_000;
    }

    // this function only support a query
    @SuppressWarnings("unused")
    private static void indexQuery(baselines.Index index, String queryStatement){
        System.out.println(queryStatement);

        // start query...
        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        long startRunTs = System.currentTimeMillis();
        if(queryStatement.contains("COUNT")){
            int cnt = index.processCountQueryUsingNFA(pattern, new NFA());
            System.out.println("number of tuples: " + cnt);
        }else {
            List<Tuple> tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
            for (Tuple t : tuples) {
                System.out.println(t);
            }
            System.out.println("number of tuples: " + tuples.size());
        }
        long endRunTs = System.currentTimeMillis();
        System.out.println("query cost: " + (endRunTs - startRunTs) + "ms.");
    }

    // this function support multiple query, use nfa as evaluation engine
    private static void indexBatchQuery(baselines.Index index, JSONArray jsonArray){
        int queryNum = jsonArray.size();
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();

            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
            if(queryStatement.contains("COUNT")){
                int cnt = index.processCountQueryUsingNFA(pattern, new NFA());
                System.out.println("number of tuples: " + cnt);
            }
            else {
                List<Tuple> tuples = index.processTupleQueryUsingNFA(pattern, new NFA());
                for (Tuple t : tuples) {
                    System.out.println(t);
                }
                System.out.println("number of tuples: " + tuples.size());
            }
            long endRunTs = System.currentTimeMillis();
            System.out.println(i + "-th query cost: " + (endRunTs - startRunTs) + "ms.");
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        long startRunTs = System.currentTimeMillis();
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        //String[] datasets = {"crimes", "nasdaq", "job", "synthetic"};
        String datasetName = "crimes";
        String filename = datasetName + ".csv";
        String queryFilename = datasetName + "_query.json";
        String outputFileName = datasetName + "_interval_scan";

        // 1. create table, alter attribute range
        Test_IntervalScan.createSchema(datasetName);
        // 2. create index
        baselines.Index index = Test_IntervalScan.createIndex(datasetName);
        // 3. store events
        long buildCost;
        if(datasetName.equals("synthetic")){
            int eventNum = 10 * 1024 * 1024; // about 10M
            int batchSize = 512;
            boolean inOrder = true;
            boolean enableUpdate = false;
            double ratio = 0;
            buildCost = Test_IntervalScan.storeEvents(eventNum, batchSize, inOrder, enableUpdate, ratio, index);
        }
        else{
            String dataFilePath = prefixPath + "dataset" + sep + filename;
            buildCost = Test_IntervalScan.storeEvents(dataFilePath, index);
        }

        // synthetic dataset
        // window: {"synthetic_query_w500.json", "synthetic_query.json", "synthetic_query_w1500.json", "synthetic_query_w2000.json", "synthetic_query_w5000.json"}
        // selectivity: {"synthetic_query_sel5.json", "synthetic_query.json", "synthetic_query_sel15.json", "synthetic_query_sel20.json", "synthetic_query_sel25.json"}

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "query" + sep + queryFilename;
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5 choose NFA engine to match
        PrintStream printStream = new PrintStream(prefixPath + "output" + sep + outputFileName + ".txt");
        System.setOut(printStream);
        System.out.println("build cost: " + buildCost +"ms");
        Test_IntervalScan.indexBatchQuery(index, jsonArray);

        long endRunTs = System.currentTimeMillis();
        System.out.println("\ntotal run time: " + (endRunTs - startRunTs) / 1000 + "s.");
    }
}
