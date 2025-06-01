package acer;

import automaton.NFA;
import baselines.Index;
import common.JsonReader;
import common.StatementParser;
import automaton.Tuple;
import generator.DataGenerator;
import net.sf.json.JSONArray;
import pattern.QueryPattern;

import java.io.*;
import java.util.List;

public class Test_ACER {

    /**
     * @param dataset       create index for a given dataset (filename)
     */
    private static void createSchema(String dataset){
        String statement;
        switch (dataset) {
            case "crimes" -> statement =
                    "CREATE TABLE crimes (PrimaryType TYPE, ID INT, Beat INT, District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)";
            case "nasdaq" -> statement =
                    "CREATE TABLE nasdaq (ticker TYPE, open DOUBLE.2, high DOUBLE.2, low DOUBLE.2, close DOUBLE.2, vol INT, Date TIMESTAMP)";
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
    private static Index createIndex(String dataset){
        String createIndexStr;
        switch (dataset) {
            case "crimes" -> createIndexStr =
                    "CREATE INDEX crimes_acer USING ACER ON crimes(Beat, District, Latitude, Longitude)";
            case "nasdaq" -> createIndexStr =
                    "CREATE INDEX nasdaq_acer USING ACER ON nasdaq(open, vol)";
            case "job" -> createIndexStr =
                    "CREATE INDEX job_acer USING ACER ON job(schedulingClass)";
            case "synthetic" -> createIndexStr =
                    "CREATE INDEX synthetic_acer USING ACER ON synthetic(a1, a2, a3, a4)";
            default -> throw new RuntimeException("create index fail, cannot support this filename...");
        }
        String str = StatementParser.convert(createIndexStr);
        Index index = StatementParser.createIndex(str);
        index.initial();
        return index;
    }

    private static long storeEvents(String filePath, Index index){
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
    private static long storeEvents(int eventNum, int batchSize, boolean inOrder, boolean enableUpdate, double ratio, Index index){
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
    private static void indexQuery(Index index, String queryStatement){
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
    private static void indexBatchQuery(Index index, JSONArray jsonArray){
        int queryNum = jsonArray.size();//i = i + 10
        for(int i = 0; i < queryNum; ++i) {
            String queryStatement = jsonArray.getString(i);
            // start query
            System.out.println("\n" + i + "-th query starting...");
            long startRunTs = System.currentTimeMillis();

            QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
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
            System.out.println(i + "-th query cost: " + (endRunTs - startRunTs) + "ms.");
        }
    }

    public static void testExample(Index index) throws FileNotFoundException {
        String query0 = """
                PATTERN SEQ(CSCO v1, AMD v2)
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 0 <= v1.open <= 1000 AND 0 <= v2.vol <= 1000\s
                WITHIN 60 units
                RETURN COUNT(*)
                """;
        int WARMUP = 20;
        for(int i = 0; i < WARMUP; i++){
            indexQuery(index, query0);
        }

        int LOOP = 10;

        String query1 = """
                PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
                WITHIN 12 minutes
                RETURN COUNT(*)
                """;
        System.out.println("Q1: \n" + query1);
        for(int i = 0; i < LOOP; i++){
            indexQuery(index, query1);
            System.out.println();
        }

        String query2 = """
                PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
                WITHIN 6 minutes
                RETURN COUNT(*)
                """;
        System.out.println("Q2: \n" + query2);
        for(int i = 0; i < LOOP; i++){
            indexQuery(index, query2);
            System.out.println();
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        long startRunTs = System.currentTimeMillis();
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;
        //String[] datasets = {"crimes", "nasdaq", "job"};
        String datasetName = "crimes";
        String filename = datasetName + ".csv";
        String queryFilename = datasetName + "_query.json";
        String outputFileName = datasetName + "_acer";

        // 1. create table, alter attribute range
        Test_ACER.createSchema(datasetName);
        // 2. create index
        Index index = Test_ACER.createIndex(datasetName);
        // 3. store events
        long buildCost;
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        buildCost = Test_ACER.storeEvents(dataFilePath, index);

        //testExample(index);System.exit(0);

        // synthetic dataset
        // window: {"synthetic_query_w500.json", "synthetic_query.json", "synthetic_query_w1500.json", "synthetic_query_w2000.json", "synthetic_query_w2500.json"}
        // selectivity: {"synthetic_query_sel5.json", "synthetic_query.json", "synthetic_query_sel15.json", "synthetic_query_sel20.json", "synthetic_query_sel25.json"}

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "query" + sep + queryFilename;  // ==> "synthetic_query_sel25.json"
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);
        // outputFileName = "synthetic1G";
        // 5. choose NFA engine to match
        // |-----------------------------------------------------------------------|
        // | output filename format: method_dataset_[Y/N]1_[Y/N]2_A[1-3].txt       |
        // | Y1: enable optimized index layout, N1: disable optimized index layout |
        // | Y2: enable truncation, N2: disable truncation                         |
        // | A1: DELTA2, A2: VARINT, A3: SIMPLE8B, A4: DELTA                       |
        // |-----------------------------------------------------------------------|
        // To avoid human error, the parameter information is printed out here
        PrintStream printStream = new PrintStream(prefixPath + "output" + sep + outputFileName + "_Y1_Y2_A3.txt");
        System.setOut(printStream);
        System.out.print("|OPTIMIZED_LAYOUT: " + Parameters.OPTIMIZED_LAYOUT);
        System.out.print("|CAPACITY: " + Parameters.CAPACITY);
        System.out.print("|COMPRESSOR: " + Parameters.COMPRESSOR);
        System.out.println("|ENABLE_TRUNCATE: " + Parameters.ENABLE_TRUNCATE + "|");
        System.out.println("build cost: " + buildCost +"ms");
        Test_ACER.indexBatchQuery(index, jsonArray);

        long endRunTs = System.currentTimeMillis();
        System.out.println("\ntotal run time: " + (endRunTs - startRunTs) / 1000 + "s.");
    }
}





