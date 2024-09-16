package automaton;

import common.EventSchema;
import common.MatchStrategy;
import common.Metadata;
import common.StatementParser;
import fullscan.FullScan;
import pattern.QueryPattern;
import store.EventStore;
import store.RID;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * a simple test for crimes dataset
 */
class Test_NFA_Crimes {
    public static void storeCSVToByteFile(FullScan fullScan, String filePath){
        try {
            FileReader f = new FileReader(filePath);
            BufferedReader b = new BufferedReader(f);
            String line;
            while ((line = b.readLine()) != null) {
                fullScan.insertRecord(line);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void initial(){
        StatementParser.createTable("CREATE TABLE crimes (PrimaryType TYPE, ID INT,  Beat INT,  District INT, Latitude DOUBLE.9, Longitude DOUBLE.9, Date TIMESTAMP)");
    }

    public static void cepNFATest(){
        // create all states
        Test_NFA_Crimes.initial();
        // define test file
        String filename = "crimes.csv";
        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "dataset" + File.separator + filename;

        String queryStatement = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE v1.Longitude >= v0.Longitude - 0.05 AND v1.Longitude <= v0.Longitude + 0.05 AND v1.Latitude >= v0.Latitude - 0.02 AND v1.Latitude <= v0.Latitude + 0.02 AND v2.Longitude >= v0.Longitude - 0.05 AND v2.Longitude <= v0.Longitude + 0.05 AND v2.Latitude >= v0.Latitude - 0.02 AND v2.Latitude <= v0.Latitude + 0.02
                WITHIN 10800 units
                RETURN tuples""";

        String testQuery = """
                PATTERN SEQ(ROBBERY v0, AND(BATTERY v1, MOTOR_VEHICLE_THEFT v2))
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE v1.Longitude >= v0.Longitude - 0.05 AND v1.Longitude <= v0.Longitude + 0.05 AND v1.Latitude >= v0.Latitude - 0.02 AND v1.Latitude <= v0.Latitude + 0.02 AND v2.Longitude >= v0.Longitude - 0.05 AND v2.Longitude <= v0.Longitude + 0.05 AND v2.Latitude >= v0.Latitude - 0.02 AND v2.Latitude <= v0.Latitude + 0.02
                WITHIN 10800 units
                RETURN tuples""";

        String cep1 = """
                PATTERN AND(SEQ(Type0 v0, Type1 v1), AND(Type2 v2, Type3 v3))
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE 3 <=v0.Beat <= 5 AND v1.Beat <= v0.Beat + 1 AND v1.Beat >= v0.Beat - 1 AND v2.Beat = v3.Beat AND 3<= v3.Beat <= 4
                WITHIN 10800 units
                RETURN tuples""";

        QueryPattern pattern = StatementParser.getQueryPattern(cep1);
        pattern.print();
        NFA testNFA = new NFA();
        testNFA.generateNFAUsingQueryPattern(pattern);
        testNFA.displayNFA();
    }

    public static void test2(){
        // create all states
        Test_NFA_Crimes.initial();
        // define test file
        String filename = "crimes.csv";
        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "dataset" + File.separator + filename;

        String queryStatement = """
                PATTERN SEQ(ROBBERY v0, BATTERY v1, MOTOR_VEHICLE_THEFT v2)
                FROM crimes
                USING SKIP_TILL_NEXT_MATCH
                WHERE v1.Longitude >= v0.Longitude - 0.05 AND v1.Longitude <= v0.Longitude + 0.05 AND v1.Latitude >= v0.Latitude - 0.02 AND v1.Latitude <= v0.Latitude + 0.02 AND v2.Longitude >= v0.Longitude - 0.05 AND v2.Longitude <= v0.Longitude + 0.05 AND v2.Latitude >= v0.Latitude - 0.02 AND v2.Latitude <= v0.Latitude + 0.02
                WITHIN 10800 units
                RETURN tuples""";

        NFA testNFA = new NFA();
        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        testNFA.generateNFAUsingQueryPattern(pattern);

        testNFA.displayNFA();

        String schemaName = "CRIMES";
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        FullScan fullScan = new FullScan(schema);

        // insert event to store
        storeCSVToByteFile(fullScan, filePath);

        long startRunTs = System.currentTimeMillis();
        // query
        int recordSize = schema.getStoreRecordSize();
        EventStore store = schema.getStore();

        int curPage = 0;
        short curOffset = 0;
        int pageSize = store.getPageSize();

        // access all records
        for(int i = 0; i < fullScan.getRecordIndices(); ++i) {
            if (curOffset + recordSize > pageSize) {
                curPage++;
                curOffset = 0;
            }
            RID rid = new RID(curPage, curOffset);
            curOffset += (short) recordSize;
            // read record from store
            byte[] record = store.readByteRecord(rid);

            testNFA.consume(schema, record, MatchStrategy.SKIP_TILL_NEXT_MATCH);
        }

        testNFA.printMatch(schema);
        long endRunTs = System.currentTimeMillis();

        System.out.println("cost: " + (endRunTs - startRunTs) + "ms.");
    }
    public static void main(String[] args){
        //Test_NFA_Crimes.cepNFATest();
    }
}
