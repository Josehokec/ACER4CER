package pattern;

import common.StatementParser;
import org.junit.jupiter.api.Test;



class ComplexPatternTest {
    @Test
    void demoTest(){

        String statement = "CREATE TABLE synthetic (type TYPE, a1 INT, a2 INT, a3 DOUBLE.2, a4 DOUBLE.2, time TIMESTAMP)";
        String str = StatementParser.convert(statement);
        StatementParser.createTable(str);

        String query = """
                PATTERN SEQ(SEQ(TYPE_3 v0, AND(TYPE_17 v1, TYPE_8 v2)), TYPE_16 v3)
                FROM synthetic
                USING SKIP_TILL_ANY_MATCH
                WHERE 0 <= v0.a2 <= 101 AND 0 <= v0.a1 <= 101 AND 0 <= v1.a3 <= 3718.5 AND 0 <= v1.a4 <= 3718.5 AND 0 <= v2.a1 <= 101 AND 0 <= v2.a3 <= 3718.5 AND 0 <= v3.a2 <= 101 AND 0 <= v3.a4 <= 3718.5 AND v2.a3 <= v1.a3
                WITHIN 1000 units
                RETURN COUNT(*)
                """;
        QueryPattern pattern = StatementParser.getQueryPattern(query);
    }
}