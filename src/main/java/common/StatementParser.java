package common;

import condition.DependentConstraint;
import condition.IndependentConstraint;
import acer.ACER;
import store.EventStore;
import pattern.ComplexPattern;
import pattern.QueryPattern;
import pattern.SequentialPattern;
import method.Index;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A Simple Version of Query Language Interpreter
 * We support four statements as following：
 * 1. CREATE TABLE
 * 2. CREATE INDEX
 * 3. ALTER
 * 4. PATTERN
 */
public class StatementParser {

    /**
     * Capitalize the statement and remove the leading and trailing spaces,
     * turning two consecutive spaces into one space
     * @param statement input statement
     * @return          returns a string converted to uppercase
     */
    public static String convert(String statement){
        return statement.toUpperCase().trim().replaceAll("\\s+", " ");
    }

    /**
     * create a schema and a store
     * CREATE TABLE stock (ticker TYPE, open FLOAT.2, volume INT, time TIMESTAMP)
     * Currently, only float, int, and double type data is supported
     * When the types are float and double, it is necessary to specify the number of decimal places to be retained
     * @param statement create table statement
     */
    public static void createTable(String statement){
        EventSchema schema = new EventSchema();

        // Divide a string into two parts based on parentheses
        String[] parts = statement.split("[()]");
        String schemaName = parts[0].split(" ")[2].trim();
        schema.setSchemaName(schemaName);

        // attributes: ['ticker TYPE', ' open FLOAT.2', ' volume INT', ' time TIMESTAMP']
        String[] attributes = parts[1].split(",");

        String[] attrNames = new String[attributes.length];
        String[] attrTypes = new String[attributes.length];
        int[] decimalLens = new int[attributes.length];

        // Note that RangeBitmap currently only supports inserting integers greater than or equal to 0
        // Before creating an index, a value range must be specified
        // Index can only be created on INT, FLOAT, and DOUBLE
        //String[] supportValueType = {"TYPE", "INT", "FLOAT", "DOUBLE", "TIMESTAMP"};

        for(int i = 0; i < attributes.length; ++i){
            String[] splits = attributes[i].trim().split(" ");
            attrNames[i] = splits[0].trim();
            attrTypes[i] = splits[1].trim();

            if(attrTypes[i].contains("FLOAT") || attrTypes[i].contains("DOUBLE")){
                int dotPos = -1;
                for(int j = 0; j < attrTypes[i].length(); ++j){
                    if(attrTypes[i].charAt(j) == '.'){
                        dotPos = j;
                    }
                }
                decimalLens[i] = Integer.parseInt(attrTypes[i].substring(dotPos + 1));
            }else{
                boolean flag = attrTypes[i].equals("TYPE") ||  attrTypes[i].equals("INT") || attrTypes[i].equals("TIMESTAMP");
                assert flag : "Class StatementParser - Do not support '" + attrTypes[i] + "' value type";
            }

            schema.insertAttrName(attrNames[i], i);
        }

        schema.setAttrNames(attrNames);
        // Calculate the recordSize here
        schema.setAttrTypes(attrTypes);
        schema.setDecimalLens(decimalLens);

        int recordSize = schema.getStoreRecordSize();
        EventStore store = new EventStore(schemaName, recordSize);
        schema.setStore(store);

        Metadata metadata = Metadata.getInstance();
        if(metadata.storeSchema(schema)){
            System.out.println("Create schema successfully.");
        }else{
            System.out.println("Create schema fail, this schema name '"
                    + schema.getSchemaName() + "' has existing.");
        }
    }

    /**
     * CREATE INDEX index_name1 USING FAST ON stock(open, volume)
     * @param statement create an index statement
     * @return return the index to be created
     */
    public static Index createIndex(String statement){
        String[] parts = statement.split("[()]");

        String[] splits = parts[0].split(" ");
        String indexName = splits[2];
        String indexType = splits[4];
        String schemaName = splits[6].trim();

        String[] indexAttrNames = parts[1].split(",");

        Index index;
        switch (indexType) {
            case "ACER" -> index = new ACER(indexName);
            default -> {
                System.out.println("Can not support " + indexType + " index.");
                index = new ACER(indexName);
            }
        }


        for(String name : indexAttrNames){
            index.addIndexAttrNameMap(name.trim());
        }
        // bind the previously created schema
        Metadata metadata = Metadata.getInstance();
        index.setSchema(metadata.getEventSchema(schemaName));
        if(metadata.bindIndex(schemaName, index)){
            System.out.println("Create '" + indexType + "' index successfully.");
        }else{
            System.out.println("Class StatementParser - Index has been created before.");
        }

        return index;
    }

    /**
     * query example
     * --------------------------------
     * PATTERN SEQ(A a, B b)
     * FROM tableName
     * USING SKIP_TILL_NEXT_MATCH
     * WHERE 15 <= a.attribute <= 16
     * WITHIN 1800 units
     * RETURN COUNT(*)
     * --------------------------------
     * @param statement - query statement
     * @return - query pattern
     */
    public static QueryPattern getQueryPattern(String statement){
        QueryPattern pattern;
        // There are a total of 6 rows, each row is processed
        String[] sentences = statement.split("\n");
        // first line is seq pattern
        String firstLine = sentences[0].toUpperCase();

        int strLen = firstLine.length();
        int cnt = 0;
        for(int i = 0 ; i < strLen; ++i){
            if(firstLine.charAt(i) == '('){
                cnt++;
                if(cnt > 1){
                    break;
                }
            }
        }

        if(cnt < 1){
            throw new RuntimeException("Illegal pattern");
        }else if(cnt == 1){
            pattern = new SequentialPattern(firstLine);
        }else{
            pattern = new ComplexPattern(firstLine);
        }
        // second line is table/schema name
        String schemaName = sentences[1].toUpperCase().split(" ")[1];
        pattern.setSchemaName(schemaName);
        // third line is match strategy
        String matchStrategy = sentences[2].toUpperCase().split(" ")[1];
        pattern.setStrategy(matchStrategy);
        // fourth line is predicate constraints
        readFourthLine(pattern, sentences[3].toUpperCase(), schemaName);
        // fifth line is query time window
        long tau;
        String[] withinStatement = sentences[4].toUpperCase().split(" ");
        if(withinStatement[2].contains("HOUR")){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 60 * 1000L;
        }else if(withinStatement[2].contains("MIN")){
            tau = Integer.parseInt(withinStatement[1]) * 60 * 1000L;
        }else if(withinStatement[2].contains("SEC")){
            tau = Integer.parseInt(withinStatement[1]) * 1000L;
        }else{
            tau = Integer.parseInt(withinStatement[1]);
        }
        pattern.setTau(tau);
        //sixth lien is return statement
        String returnStr = sentences[5].toUpperCase().substring(6);
        pattern.setReturnStr(returnStr);

        return pattern;
    }

    // this function used for QueryPattern rather than EventPattern
    private static void readFourthLine(QueryPattern pattern, String fourthLine, String schemaName){
        // 90 <= a.open <= 110 AND 90.1 <= b.open <= 110.1 AND c.open >= 125 AND d.open <= 75
        String[] predicates = fourthLine.substring(6).split("AND");
        String regex1 = "^([-+])?\\d+(\\.\\d+)? *<=? *[A-Za-z0-9]+.[A-Za-z0-9]+ *<=? *([-+])?\\d+(\\.\\d+)?";
        String regex2 = "^[A-Za-z0-9]+.[A-Za-z0-9]+ *[><]=? *([-+])?\\d+(\\.\\d+)?";
        String regex3 = "^[A-Za-z0-9]+.[A-Za-z0-9]+ *([*/] *([-+])?\\d+(\\.\\d+)?)? *([+-] *([-+])?\\d+(\\.\\d+)?)? *[><=]=? *[A-Za-z0-9]+.[A-Za-z0-9]+ *([*/] *([-+])?\\d+(\\.\\d+)?)? *([+-] *([-+])?\\d+(\\.\\d+)?)?";

        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);

        for (String predicate : predicates) {
            // delete blank
            String curPredicate = predicate.trim();
            if(Pattern.matches(regex1, curPredicate)){
                // number <=? varName.attrName <=? number
                parseIC1(pattern, curPredicate, schema);
            }else if(Pattern.matches(regex2, curPredicate)){
                // varName.attrName [><]=? number
                parseIC2(pattern, curPredicate, schema);
            }else if(Pattern.matches(regex3, curPredicate)){
                // varName.attrName * number + number  [><=]=? varName.attrName * number + number
                parseDC(pattern, curPredicate, schema);
            }else{
                System.out.println("current predicate: '" + curPredicate + "'" + " is illegal.");
                throw new RuntimeException("query pattern exists error.");
            }
        }
    }


    private static void parseIC1(QueryPattern pattern, String curPredicate, EventSchema schema){
        int aoPos1 = -1;
        int aoPos2 = -1;
        ComparedOperator cmp1, cmp2;
        for(int k = 0; k < curPredicate.length(); ++k) {
            char ch = curPredicate.charAt(k);
            if(ch == '<'){
                if(aoPos1 == -1){
                    aoPos1 = k;
                }else{
                    aoPos2 = k;
                    break;
                }
            }
        }
        int midPos;
        String value1 = curPredicate.substring(0, aoPos1).trim();
        if(curPredicate.charAt(aoPos1 + 1) == '='){
            cmp1 = ComparedOperator.GE;
            midPos = aoPos1 + 2;
        }else{
            cmp1 = ComparedOperator.GT;
            midPos = aoPos1 + 1;
        }

        String varNameAndAttrName  = curPredicate.substring(midPos, aoPos2).trim();
        String[] split = varNameAndAttrName.split("\\.");
        String varName = split[0];
        String attrName = split[1];


        String value2;
        if(curPredicate.charAt(aoPos2 + 1) == '='){
            cmp2 = ComparedOperator.LE;
            value2 = curPredicate.substring(aoPos2 + 2).trim();
        }else{
            cmp2 = ComparedOperator.LT;
            value2 = curPredicate.substring(aoPos2 + 1).trim();
        }

        IndependentConstraint ic = new IndependentConstraint(attrName, cmp1, value1, cmp2, value2, schema);

        if(pattern.getIcMap().containsKey(varName)){
            List<IndependentConstraint> icList = pattern.getIcMap().get(varName);
            icList.add(ic);
        }else{
            List<IndependentConstraint> icList = new ArrayList<>(4);
            icList.add(ic);
            pattern.getIcMap().put(varName,icList);
        }
    }

    private static void parseIC2(QueryPattern pattern, String curPredicate, EventSchema schema){
        // Find the position of the less than or greater than sign
        int aoPos = -1;
        ComparedOperator cmp;
        for(int k = 0; k < curPredicate.length(); ++k) {
            char ch = curPredicate.charAt(k);
            if(ch == '<' || ch == '>'){
                aoPos = k;
                break;
            }
        }

        String left = curPredicate.substring(0, aoPos).trim();
        String[] split = left.split("\\.");
        String varName = split[0];
        String attrName = split[1];
        String value;

        if(curPredicate.charAt(aoPos + 1) == '='){
            value = curPredicate.substring(aoPos + 2).trim();
            if(curPredicate.charAt(aoPos) == '>'){
                cmp = ComparedOperator.GE;
            }else{
                cmp = ComparedOperator.LE;
            }
        }
        else{
            value = curPredicate.substring(aoPos + 1).trim();
            if(curPredicate.charAt(aoPos) == '>'){
                cmp = ComparedOperator.GT;
            }else{
                cmp = ComparedOperator.LT;
            }
        }

        IndependentConstraint ic = new IndependentConstraint(attrName, cmp, value, schema);
        if(pattern.getIcMap().containsKey(varName)){
            List<IndependentConstraint> icList = pattern.getIcMap().get(varName);
            icList.add(ic);
        }else{
            List<IndependentConstraint> icList = new ArrayList<>(4);
            icList.add(ic);
            pattern.getIcMap().put(varName,icList);
        }
    }

    private static void parseDC(QueryPattern pattern, String curPredicate, EventSchema schema){

        // If flag is true, it indicates case 2;
        // if flag is false, it indicates case 1
        boolean flag = curPredicate.contains("+") || curPredicate.contains("-") ||
                curPredicate.contains("*") || curPredicate.contains("/");

        if(flag){
            DependentConstraint dc = new DependentConstraint();
            for(int i = 0; i < curPredicate.length(); ++i){
                if(curPredicate.charAt(i) == '<' && curPredicate.charAt(i + 1) == '='){
                    dc.setCMP(ComparedOperator.LE);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 2), schema);
                    break;
                }else if(curPredicate.charAt(i) == '<'){
                    dc.setCMP(ComparedOperator.LT);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 1), schema);
                    break;
                }else if(curPredicate.charAt(i) == '>' && curPredicate.charAt(i + 1) == '='){
                    dc.setCMP(ComparedOperator.GE);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 2), schema);
                    break;
                }else if(curPredicate.charAt(i) == '>'){
                    dc.setCMP(ComparedOperator.GT);
                    dc.constructLeft(curPredicate.substring(0, i), schema);
                    dc.constructRight(curPredicate.substring(i + 1), schema);
                    break;
                }
            }
            pattern.getDcList().add(dc);
        }
        else{
            // format: a.open <= b.open
            // Record the positions of the first and second dots
            int firstDot = -1;
            int secondDot = -1;
            // Record the position of comparison symbols
            int opPos = -1;
            // Record the first variable name and the second variable name
            String firstVarName =null;
            String secondVarName = null;
            ComparedOperator op = null;
            for(int i = 0; i < curPredicate.length(); ++i){
                char ch = curPredicate.charAt(i);
                // first dot position
                if(ch == '.' && firstDot == -1){
                    firstDot = i;
                    // [0,i)
                    firstVarName = curPredicate.substring(0,firstDot).trim();
                }else if(ch == '>' || ch == '<' || ch == '='){
                    opPos = i;
                    if(ch == '='){
                        op = ComparedOperator.EQ;
                    }else if(curPredicate.charAt(i + 1) == '='){
                        op = ch == '<' ? ComparedOperator.LE : ComparedOperator.GE;
                        // skip a position
                        i++;
                    }else{
                        op = ch == '<' ? ComparedOperator.LT : ComparedOperator.GT;
                    }
                }else if(ch == '.'){
                    secondDot = i;
                    // [opPos + 1, secondDot)
                    if(op == ComparedOperator.GE || op == ComparedOperator.LE){
                        secondVarName = curPredicate.substring(opPos + 2, secondDot).trim();
                    }else{
                        secondVarName = curPredicate.substring(opPos + 1, secondDot).trim();
                    }
                }
            }

            String attrName1 = curPredicate.substring(firstDot + 1, opPos).trim();
            String attrName2 = curPredicate.substring(secondDot + 1).trim();

            if(attrName1.equals(attrName2)){
                DependentConstraint dc = new DependentConstraint(attrName1, firstVarName, secondVarName, op);
                pattern.getDcList().add(dc);
            }else{
                // If the names of two attributes are different
                // it indicates an error has occurred
                throw new RuntimeException("Dependent Constraint has error.");
            }
        }
    }
}
