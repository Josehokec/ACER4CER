package systems;

import common.EventSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;


public class CrimesPatternQuery {

    private static int resCount = 0;

    public static int crimesFirstQuery(List<byte[]> byteEvents, EventSchema schema) {
        /**
         * query statement 2:
         * PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)
         * FROM Crimes
         * USE skip-till-next-match
         * WHERE 2030 <= v1.beat <= 2060
         *   AND 2010 <= v2.beat <= 2080
         *   AND 2010 <= v3.beat <= 2080
         *   AND v2.beat >= v1.beat - 20
         *   AND v2.beat <= v1.beat + 20
         *   AND v2.beat >= v1.beat - 20
         *   AND v3.beat <= v1.beat + 20
         * WITHIN 30 minutes
         * RETURN COUNT(*)
         */
        resCount = 0;
        //System.out.println("query start...");
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent) throws Exception {
                        return crimesEvent.getPrimaryType().equals("ROBBERY") &&
                                crimesEvent.getBeat() >= 2030 && crimesEvent.getBeat() <= 2060;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("BATTERY") && crimesEvent.getBeat() >= 2010 && crimesEvent.getBeat() <= 2080;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 20 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 20;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> context) throws Exception {
                        boolean ic = event.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && event.getBeat() >= 2010 && event.getBeat() <= 2080;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = event.getBeat() >= v1Event.getBeat() - 20 &&
                                    event.getBeat() <= v1Event.getBeat() + 20;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<CrimesEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new CrimesEvent(record, schema));
        }
        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    resCount++;

                    CrimesEvent firstEvent = map1.get("v1").get(0);
                    CrimesEvent secondEvent = map1.get("v2").get(0);
                    CrimesEvent thirdEvent = map1.get("v3").get(0);
                    return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";
                    //return "";
                }
        );

        select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    public static int crimesFirstQuery(List<CrimesEvent> events) {
        /**
         * query statement 2:
         * PATTERN SEQ(ROBBERY v1, BATTERY v2, MOTOR_VEHICLE_THEFT v3)
         * FROM Crimes
         * USE skip-till-next-match
         * WHERE 2030 <= v1.beat <= 2060
         *   AND 2010 <= v2.beat <= 2080
         *   AND 2010 <= v3.beat <= 2080
         *   AND v2.beat >= v1.beat - 20
         *   AND v2.beat <= v1.beat + 20
         *   AND v2.beat >= v1.beat - 20
         *   AND v3.beat <= v1.beat + 20
         * WITHIN 30 minutes
         * RETURN COUNT(*)
         */
        resCount = 0;
        //System.out.println("query start...");
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent) throws Exception {
                        return crimesEvent.getPrimaryType().equals("ROBBERY") &&
                                crimesEvent.getBeat() >= 2030 && crimesEvent.getBeat() <= 2060;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("BATTERY") && crimesEvent.getBeat() >= 2010 && crimesEvent.getBeat() <= 2080;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 20 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 20;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<CrimesEvent>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> context) throws Exception {
                        boolean ic = event.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && event.getBeat() >= 2010 && event.getBeat() <= 2080;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = event.getBeat() >= v1Event.getBeat() - 20 &&
                                    event.getBeat() <= v1Event.getBeat() + 20;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    //CrimesEvent firstEvent = map1.get("v1").get(0);
                    //CrimesEvent secondEvent = map1.get("v2").get(0);
                    //CrimesEvent thirdEvent = map1.get("v3").get(0);
                    //return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";
                    resCount++;
                    return "";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    public static int crimesSecondQuery(List<byte[]> byteEvents, EventSchema schema) {
        resCount = 0;
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event) {
                        return event.getPrimaryType().equals("ROBBERY") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                        boolean ic = event.getPrimaryType().equals("BATTERY") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                        boolean dc = false;
                        for (CrimesEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getLongitude() >= preEvent.getLongitude() - 0.005 &&
                                    event.getLongitude() <= preEvent.getLongitude() + 0.005 &&
                                    event.getLatitude() >= preEvent.getLatitude() - 0.002 &&
                                    event.getLatitude() <= preEvent.getLatitude() + 0.002;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                        boolean ic = event.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                        boolean dc = false;
                        for (CrimesEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getLongitude() >= preEvent.getLongitude() - 0.005 &&
                                    event.getLongitude() <= preEvent.getLongitude() + 0.005 &&
                                    event.getLatitude() >= preEvent.getLatitude() - 0.002 &&
                                    event.getLatitude() <= preEvent.getLatitude() + 0.002;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<CrimesEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new CrimesEvent(record, schema));
        }
        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    //CrimesEvent firstEvent = map1.get("v1").get(0);
                    //CrimesEvent secondEvent = map1.get("v2").get(0);
                    //CrimesEvent thirdEvent = map1.get("v3").get(0);
                    resCount++;
                    return "";
                    //return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    public static int crimesSecondQuery(List<CrimesEvent> events) {
        resCount = 0;
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event) {
                        return event.getPrimaryType().equals("ROBBERY") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                        boolean ic = event.getPrimaryType().equals("BATTERY") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                        boolean dc = false;
                        for (CrimesEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getLongitude() >= preEvent.getLongitude() - 0.005 &&
                                    event.getLongitude() <= preEvent.getLongitude() + 0.005 &&
                                    event.getLatitude() >= preEvent.getLatitude() - 0.002 &&
                                    event.getLatitude() <= preEvent.getLatitude() + 0.002;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                        boolean ic = event.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && event.getBeat() >= 2000 && event.getBeat() <= 2100;
                        boolean dc = false;
                        for (CrimesEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getLongitude() >= preEvent.getLongitude() - 0.005 &&
                                    event.getLongitude() <= preEvent.getLongitude() + 0.005 &&
                                    event.getLatitude() >= preEvent.getLatitude() - 0.002 &&
                                    event.getLatitude() <= preEvent.getLatitude() + 0.002;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    //CrimesEvent firstEvent = map1.get("v1").get(0);
                    //CrimesEvent secondEvent = map1.get("v2").get(0);
                    //CrimesEvent thirdEvent = map1.get("v3").get(0);
                    resCount++;
                    return "";
                    //return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    // SEQ(ROBBERY v1, BATTERY v2, KP(NARCOTICS v3), MOTOR_VEHICLE_THEFT v4)
    public static int queryWithKP(List<byte[]> byteEvents, EventSchema schema){
        resCount = 0;
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent) throws Exception {
                        return crimesEvent.getPrimaryType().equals("ROBBERY") &&
                                crimesEvent.getBeat() >= 2000 && crimesEvent.getBeat() <= 2100;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("BATTERY") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").oneOrMore().where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("NARCOTICS") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }

        ).followedBy("v4").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<CrimesEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new CrimesEvent(record, schema));
        }
        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    resCount++;
                    return "";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    // SEQ(ROBBERY v1, BATTERY v2, KS(NARCOTICS v3), MOTOR_VEHICLE_THEFT v4)
    public static int queryWithKS(List<byte[]> byteEvents, EventSchema schema){
        resCount = 0;
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent) throws Exception {
                        return crimesEvent.getPrimaryType().equals("ROBBERY") &&
                                crimesEvent.getBeat() >= 2000 && crimesEvent.getBeat() <= 2100;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("BATTERY") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").oneOrMore().optional().where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("NARCOTICS") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }

        ).followedBy("v4").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<CrimesEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new CrimesEvent(record, schema));
        }
        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    resCount++;
                    return "";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    // SEQ(ROBBERY v1, BATTERY v2, NEG(NARCOTICS v3), MOTOR_VEHICLE_THEFT v4)
    public static int queryWithNEG(List<byte[]> byteEvents, EventSchema schema){
        resCount = 0;
        Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent) throws Exception {
                        return crimesEvent.getPrimaryType().equals("ROBBERY") &&
                                crimesEvent.getBeat() >= 2000 && crimesEvent.getBeat() <= 2100;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("BATTERY") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).notFollowedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("NARCOTICS") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }

        ).followedBy("v4").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(CrimesEvent crimesEvent, Context<CrimesEvent> context) throws Exception {
                        boolean ic = crimesEvent.getPrimaryType().equals("MOTOR_VEHICLE_THEFT") && crimesEvent.getBeat() >= 1900 && crimesEvent.getBeat() <= 2200;
                        boolean dc = false;
                        for (CrimesEvent v1Event : context.getEventsForPattern("v1")) {
                            dc = crimesEvent.getBeat() >= v1Event.getBeat() - 100 &&
                                    crimesEvent.getBeat() <= v1Event.getBeat() + 100;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1801));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<CrimesEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new CrimesEvent(record, schema));
        }
        DataStream<CrimesEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(CrimesEvent crimesEvent) {
                        return crimesEvent.getTimestamp();
                    }
                });

        PatternStream<CrimesEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<CrimesEvent, String>) map1 -> {
                    resCount++;
                    return "";
                }
        );

        //select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }
}
