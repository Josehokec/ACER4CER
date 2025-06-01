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


class Test_NaiveIndex {

    /**
     * [updated]
     * @param datasetName create index for different datasets
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

    /**
     * [updated]
     * @param datasetName   create index for different datasets
     * @return index
     */
    private static baselines.Index createIndex(String datasetName){
        String createIndexStr;
        switch (datasetName) {
            case "crimes" -> createIndexStr =
                    "CREATE INDEX crimes_naive_index USING NAIVE_INDEX ON crimes(Beat, District, Latitude, Longitude)";
            case "nasdaq" -> createIndexStr =
                    "CREATE INDEX nasdaq_naive_index USING NAIVE_INDEX ON nasdaq(open, vol)";
            case "job" -> createIndexStr =
                    "CREATE INDEX job_naive_index USING NAIVE_INDEX ON job(schedulingClass)";
            case "synthetic" -> createIndexStr =
                    "CREATE INDEX synthetic_naive_index USING NAIVE_INDEX ON synthetic(a1, a2, a3, a4)";
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
        // store all records
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
        /*
        We constructed queries with varying lengths and semantics as well as query workloads of different sizes.
        To simulate query predicates, we drew uniformly selectivities from the range 1 − 10%. If not stated otherwise, the time window used was 1 minute.
         */
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

    // this function only support a query, use nfa as evaluation engine
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

    // this function support multiple query
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

    public static void main(String[] args) throws FileNotFoundException {
        long startRunTs = System.currentTimeMillis();
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // dataset -> {"crimes", "nasdaq", "job", "synthetic"}
        String datasetName = "crimes";
        String filename = datasetName + ".csv";
        String queryFilename = datasetName + "_query.json";
        String outputFileName = datasetName + "_naive_index";

        // 1. create table, alter attribute range
        Test_NaiveIndex.createSchema(datasetName);
        // 2. create index
        baselines.Index index = Test_NaiveIndex.createIndex(datasetName);
        // 3. store events
        long buildCost;
        if(datasetName.equals("synthetic")) {
            int eventNum = 10 * 1024 * 1024; // about 10M
            int batchSize = 512;
            boolean inOrder = true;
            boolean enableUpdate = false;
            double ratio = 0;
            buildCost = Test_NaiveIndex.storeEvents(eventNum, batchSize, inOrder, enableUpdate, ratio, index);
        }
        else{
            String dataFilePath = prefixPath + "dataset" + sep + filename;
            buildCost = Test_NaiveIndex.storeEvents(dataFilePath, index);
        }

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "query" + sep + queryFilename;
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5. choose NFA engine to match
        PrintStream printStream = new PrintStream(prefixPath + "output" + sep + outputFileName + ".txt");
        System.setOut(printStream);
        System.out.println("build cost: " + buildCost +"ms");
        Test_NaiveIndex.indexBatchQuery(index, jsonArray);

        long endRunTs = System.currentTimeMillis();
        System.out.println("\ntotal run time: " + (endRunTs - startRunTs) / 1000 + "s.");
    }
}

/*
String query = """
              PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
              FROM NASDAQ
              USE SKIP_TILL_NEXT_MATCH
              WHERE 325 <= v1.open <= 335 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
              WITHIN 720 units
              RETURN tuples""";

--------------------------------------------------------------------
"ALTER TABLE crimes ADD CONSTRAINT PrimaryType IN RANGE [0,50]",
"ALTER TABLE crimes ADD CONSTRAINT Beat IN RANGE [0,3000]",
"ALTER TABLE crimes ADD CONSTRAINT District IN RANGE [0,50]",
"ALTER TABLE crimes ADD CONSTRAINT Latitude IN RANGE [36.6,42.1]",
"ALTER TABLE crimes ADD CONSTRAINT Longitude IN RANGE [-91.7,-87.5]"
--------------------------------------------------------------------
"ALTER TABLE nasdaq ADD CONSTRAINT ticker IN RANGE [0,50]",
"ALTER TABLE nasdaq ADD CONSTRAINT open IN RANGE [0,4000.00]",
"ALTER TABLE nasdaq ADD CONSTRAINT vol IN RANGE [0,100000000]"
--------------------------------------------------------------------
"ALTER TABLE job ADD CONSTRAINT EventType IN RANGE [0,30]",
"ALTER TABLE job ADD CONSTRAINT schedulingClass IN RANGE [0,7]"
--------------------------------------------------------------------

String[] queryFileNames = {"synthetic_query_pattern1", "synthetic_query_pattern2",
                "synthetic_query_pattern3", "synthetic_query_pattern4", "synthetic_query_pattern5"};
 */