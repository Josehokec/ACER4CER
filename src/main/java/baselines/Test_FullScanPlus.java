package baselines;

import acer.Test_ACER;
import automaton.NFA;
import automaton.Tuple;
import common.*;
import generator.DataGenerator;
import pattern.QueryPattern;
import net.sf.json.JSONArray;

import java.io.*;
import java.util.List;

/**
 * filter irrelevant events based window and predicate conditions
 * bucketed filtering
 */
public class Test_FullScanPlus {

    /**
     * [updated]
     * @param datasetName       create index for a given dataset (filename)
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

    // different schema creates different object
    private static baselines.FullScanPlus createFullScan(String datasetName){
        String schemaName = datasetName.toUpperCase();
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        return new baselines.FullScanPlus(schema);
    }

    private static long storeEvents(String filePath, baselines.FullScanPlus fullScan){
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
                fullScan.insertOrDeleteRecord(line, false);
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

    private static long storeEvents(int eventNum, int batchSize, boolean inOrder, boolean enableUpdate, double ratio, baselines.FullScanPlus fullScan){
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
            fullScan.insertBatchRecord(batchRecords, enableUpdate, inOrder);
            eventCount += batchRecords.size();
        }
        long endBuildTime = System.nanoTime();
        System.out.println("average insertion latency: " + (endBuildTime - startBuildTime) / eventCount + "ns");
        // ns -> ms
        return (endBuildTime - startBuildTime) / 1_000_000;
    }

    // this function support multiple query
    private static void batchQuery(baselines.FullScanPlus fullScan, JSONArray jsonArray){
        int queryNum = jsonArray.size();//i = i + 10
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
                List<Tuple> tuples = fullScan.processTupleQueryUsingNFA(pattern, new NFA());
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
    private static void singleQuery(baselines.FullScanPlus fullScan, String queryStatement){
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

    private static void testExample(baselines.FullScanPlus fullScan){
//        String query = """
//                PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
//                FROM NASDAQ
//                USE SKIP_TILL_NEXT_MATCH
//                WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
//                WITHIN 720 units
//                RETURN tuples""";
        String query = """
                PATTERN SEQ(AND(AMZN v0, BABA v1), AND(AMZN v2, BABA v3))
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 122 <= v0.open <= 132 AND 82 <= v1.open <= 92 AND v2.open >= v0.open * 0.01 AND v3.open <= v1.open * 0.99
                WITHIN 900 units
                RETURN tuples
                """;
        singleQuery(fullScan, query);
    }

    public static void main(String[] args) throws FileNotFoundException {
        long startRunTs = System.currentTimeMillis();

        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        //String[] datasets = {"crimes", "nasdaq", "job", "synthetic"};
        String datasetName = "crimes";
        String filename = datasetName + ".csv";
        String queryFilename = datasetName + "_query.json";
        String outputFileName = datasetName + "_full_scan";

        // 1. create table, alter attribute range
        Test_FullScanPlus.createSchema(datasetName);

        // 2. create fullScan Object
        baselines.FullScanPlus fullScan = Test_FullScanPlus.createFullScan(datasetName);

        // 3. store events
        long buildCost;
        if(datasetName.equals("synthetic")){
            int eventNum = 10 * 1024 * 1024; // about 10M
            int batchSize = 512;
            boolean inOrder = true;
            boolean enableUpdate = false;
            double ratio = 0;
            buildCost = Test_FullScanPlus.storeEvents(eventNum, batchSize, inOrder, enableUpdate, ratio, fullScan);
        }
        else{
            String dataFilePath = prefixPath + "dataset" + sep + filename;          // ==> "nasdaq_mini.csv"
            buildCost = Test_FullScanPlus.storeEvents(dataFilePath, fullScan);
            fullScan.updateArrivalJson();               // write json file
        }

        // testExample(fullScan);System.exit(0);

        // synthetic dataset
        // window: {"synthetic_query_w500.json", "synthetic_query.json", "synthetic_query_w1500.json", "synthetic_query_w2000.json", "synthetic_query_w2500.json"}
        // selectivity: {"synthetic_query_sel5.json", "synthetic_query.json", "synthetic_query_sel15.json", "synthetic_query_sel20.json", "synthetic_query_sel25.json"}

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "query" + sep + queryFilename;  // ==> queryFilename
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5. choose NFA engine to match
        PrintStream printStream = new PrintStream(prefixPath + "output" + sep + outputFileName + ".txt");
        System.setOut(printStream);
        System.out.println("build cost: " + buildCost +"ms");
        Test_FullScanPlus.batchQuery(fullScan, jsonArray);

        long endRunTs = System.currentTimeMillis();
        System.out.println("\ntotal run time: " + (endRunTs - startRunTs) / 1000 + "s.");
    }
}
