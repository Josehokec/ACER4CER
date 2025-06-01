package automaton;

import common.EventSchema;
import common.Metadata;
import common.StatementParser;
import org.junit.Test;
import pattern.QueryPattern;

public class NFATest {

    @Test
    public void simpleTest(){
        String creatTable = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.1, a4 DOUBLE.1, time TIMESTAMP)";
        String str = StatementParser.convert(creatTable);
        StatementParser.createTable(str);
        EventSchema schema = Metadata.getInstance().getEventSchema("SYNTHETIC");

        String queryStatement = """
                PATTERN SEQ(TYPE_1 v0, TYPE_3 v1, TYPE_0 v2)
                FROM synthetic
                USING SKIP_TILL_ANY_MATCH
                WHERE 0 <= v0.a1 <= 101 AND 0 <= v0.a3 <= 3718.5 AND 0 <= v1.a4 <= 3718.5 AND 0 <= v2.a2 <= 101 AND v0.a1 <= v2.a1 AND v1.a4 <= v2.a4
                WITHIN 10 units
                RETURN tuples""";

        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        NFA nfa = new NFA();
        nfa.generateNFAUsingQueryPattern(pattern);
        //nfa.display();

        // type TYPE, a1 INT, a2 INT, a3 DOUBLE.1, a4 DOUBLE.1, time TIMESTAMP
        String[] records = {
                "TYPE_1,10,100,45.4,100.5,1",
                "TYPE_9,10,100,45.4,100.5,3",
                "TYPE_1,10,100,45.4,100.5,5",
                "TYPE_3,10,100,45.4,100.5,7",
                "TYPE_0,10,100,45.4,100,10",
                "TYPE_0,200,10,45.4,118,13",
                "TYPE_0,10,200,45.4,118,14",
                "TYPE_0,10,100,45.4,103.7,15",
        };

        for(String record : records){
            byte[] byteRecord = schema.convertToBytes(record.split(","));
            nfa.consume(schema, byteRecord, MatchStrategy.SKIP_TILL_ANY_MATCH);
        }

        nfa.printMatch(schema);
    }

    @Test
    public void testNasdaq(){
        String creatTable = "CREATE TABLE nasdaq (ticker TYPE, open DOUBLE.2,  high DOUBLE.2,  low DOUBLE.2, close DOUBLE.2, vol INT, Date TIMESTAMP)";
        String str = StatementParser.convert(creatTable);
        StatementParser.createTable(str);
        EventSchema schema = Metadata.getInstance().getEventSchema("NASDAQ");

        String queryStatement = """
                PATTERN SEQ(AND(AMZN v0, BABA v1), AND(AMZN v2, BABA v3))
                FROM NASDAQ
                USE SKIP_TILL_NEXT_MATCH
                WHERE 122 <= v0.open <= 132 AND 82 <= v1.open <= 92 AND v2.open >= v0.open * 0.01 AND v3.open <= v1.open * 0.99
                WITHIN 900 units
                RETURN tuples
                """;

        QueryPattern pattern = StatementParser.getQueryPattern(queryStatement);
        NFA nfa = new NFA();
        nfa.generateNFAUsingQueryPattern(pattern);



        // nfa.display();

        String[] records = {
                "AMZN,122.33,122.69,122.3,122.5,544548,1682582820",
                "BABA,85.16,85.23,85.16,85.22,11615,1682582820",
                "AMZN,122.55,123.0,121.8,122.0,519298,1682582880",
                "BABA,85.2,85.2,85.15,85.15,369,1682582880",
                "AMZN,121.98,122.23,121.28,122.03,538060,1682582940",
                "BABA,85.15,85.2,85.12,85.13,3849,1682582940",
                "AMZN,122.0,122.22,121.1,122.05,293708,1682583000",
                "BABA,85.13,85.2,85.13,85.2,3636,1682583000",
                "BABA,85.2,85.22,85.2,85.22,1461,1682583060",
                "AMZN,122.02,122.08,109.82,121.5,403896,1682583060",
                "AMZN,121.54,121.74,109.82,121.0,427070,1682583120",
                "BABA,85.21,85.21,85.05,85.05,5467,1682583120",
                "BABA,85.09,85.09,84.99,85.0,2711,1682583180",
                "AMZN,121.0,121.25,109.82,120.7,357946,1682583180",
                "BABA,84.95,85.11,84.9,84.9,1918,1682583240",
                "AMZN,120.7,120.7,119.99,120.03,430779,1682583240",
                "BABA,84.82,84.9,84.82,84.85,970,1682583300",
                "AMZN,120.07,120.25,119.9,120.0,320797,1682583300",
                "AMZN,120.01,120.2,119.53,119.83,267839,1682583360",
                "BABA,84.85,84.9,84.7,84.79,3763,1682583360",
                "AMZN,119.94,120.0,119.1,119.83,150727,1682583420",
                "BABA,84.68,85.0,84.68,85.0,14934,1682583420",
                "AMZN,119.81,119.91,119.3,119.4,213373,1682583480",
                "BABA,84.98,85.06,84.98,85.06,605,1682583480",
                "BABA,84.99,85.07,84.84,84.99,1136,1682583540",
                "AMZN,119.4,119.56,118.1,119.07,404246,1682583540",
                "AMZN,119.07,119.15,118.1,118.2,212113,1682583600",
                "BABA,85.07,85.07,85.07,85.07,100,1682583600",
                "BABA,84.99,84.99,84.99,84.99,1500,1682583660",
                "AMZN,118.26,118.3,116.1,117.0,424942,1682583660",
                "BABA,84.1,85.0,84.1,84.87,300,1682583720",
                "AMZN,117.2,118.16,117.0,118.16,330091,1682583720"
        };

        for(String record : records){
            byte[] byteRecord = schema.convertToBytes(record.split(","));
            nfa.consume(schema, byteRecord, pattern.getStrategy());
        }

        nfa.printMatch(schema);
        System.out.println("tuple size: " + nfa.getTuple(schema).size());
    }

}