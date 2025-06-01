package acer;

import automaton.NFA;
import baselines.Index;
import common.StatementParser;
import automaton.Tuple;
import pattern.QueryPattern;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

class ACERTest {
    public static void createSchema(){
        String statement = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);
    }

    public static Index createIndex(){
        String createIndexStr = "CREATE INDEX fast_synthetic USING ACER ON synthetic(a1, a2, a3)";
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
            //b.readLine();
            while ((line = b.readLine()) != null) {
                index.insertRecord(line, false);
            }
            b.close();
            f.close();
        }catch (IOException e) {
            e.printStackTrace();
        }

        // update operations
        index.insertRecord("TYPE_2,878,66,62.33,140.92,1696150489533", true);
        index.insertRecord("TYPE_2,79,455,111.56,672.17,1696150489660", true);

        long endBuildTime = System.currentTimeMillis();
        return endBuildTime - startBuildTime;
    }

    public static void indexQuery(Index index, String queryStatement){
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

    @org.junit.jupiter.api.Test
    public void demoTest(){
        String sep = File.separator;
        String prefixPath = System.getProperty("user.dir") + sep + "src" + sep + "main" + sep;

        // 1. create table, alter attribute range
        createSchema();
        // 2. create index
        Index index = createIndex();
        // 3. "synthetic_test"; synthetic_out_of_order
        String filename = "synthetic_test";
        String dataFilePath = prefixPath + "dataset" + sep + filename + ".csv";
        long buildCost = storeEvents(dataFilePath, index);

        String queryStatement = """
                        PATTERN SEQ(TYPE_0 v0, TYPE_1 v1, Type_2 v2)
                        FROM synthetic
                        USING SKIP_TILL_NEXT_MATCH
                        WHERE 483 <= v0.a1 <= 662 AND 200 <= v0.a2 <= 300 AND 400 <= v1.a3 <= 700
                        WITHIN 40 units
                        RETURN matched_tuples""";

        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        indexQuery(index, queryStatement);
    }
}