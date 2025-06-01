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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class StockPatternQuery {

    private static int resCount = 0;

    public static int nasdaqFirstQuery(List<byte[]> byteEvents, EventSchema schema) {
        /**
         * PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
         * FROM NASDAQ
         * USE skip-till-next-match
         * WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
         * WITHIN 720 units
         * RETURN COUNT(*)
         */
        resCount = 0;
        Pattern<StockEvent, ?> pattern = Pattern.<StockEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("MSFT") &&
                                event.getOpen() >= 326 && event.getOpen() <= 334;
                    }
                }
        ).followedBy("v2").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("GOOG")
                                && event.getOpen() >= 120 && event.getOpen() <= 130;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("MSFT");
                        boolean dc = false;
                        for (StockEvent v1Event : ctx.getEventsForPattern("v1")) {
                            dc = event.getOpen() >= v1Event.getOpen() * 1.003;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v4").where(
                new IterativeCondition<StockEvent>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("GOOG");
                        boolean dc = false;
                        for(StockEvent v2Event : ctx.getEventsForPattern("v2")){
                            dc = event.getOpen() <= v2Event.getOpen() * 0.997;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(721));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        // set one thread
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<StockEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new StockEvent(record, schema));
        }

        DataStream<StockEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(StockEvent event){
                        return event.getTimestamp();
                    }
                });

        PatternStream<StockEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<StockEvent, String>) map1 -> {
                    //StockEvent firstEvent = map1.get("v1").get(0);
                    //StockEvent secondEvent = map1.get("v2").get(0);
                    //StockEvent thirdEvent = map1.get("v3").get(0);
                    //StockEvent fourthEvent = map1.get("v4").get(0);
                    //return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "|" + fourthEvent + "]";
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

    public static int nasdaqFirstQuery(List<StockEvent> events) {
        /**
         * PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
         * FROM NASDAQ
         * USE skip-till-next-match
         * WHERE 326 <= v1.open <= 334 AND 120 <= v2.open <= 130 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
         * WITHIN 720 units
         * RETURN COUNT(*)
         */
        resCount = 0;
        Pattern<StockEvent, ?> pattern = Pattern.<StockEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("MSFT") &&
                                event.getOpen() >= 326 && event.getOpen() <= 334;
                    }
                }
        ).followedBy("v2").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("GOOG")
                                && event.getOpen() >= 120 && event.getOpen() <= 130;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("MSFT");
                        boolean dc = false;
                        for (StockEvent v1Event : ctx.getEventsForPattern("v1")) {
                            dc = event.getOpen() >= v1Event.getOpen() * 1.003;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v4").where(
                new IterativeCondition<StockEvent>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("GOOG");
                        boolean dc = false;
                        for(StockEvent v2Event : ctx.getEventsForPattern("v2")){
                            dc = event.getOpen() <= v2Event.getOpen() * 0.997;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(721));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        // set one thread
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(StockEvent event){
                        return event.getTimestamp();
                    }
                });

        PatternStream<StockEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<StockEvent, String>) map1 -> {
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

    public static int nasdaqSecondQuery(List<byte[]> byteEvents, EventSchema schema) {
        /**
         * PATTERN SEQ(MSFT v1, GOOG v2, MSFT v3, GOOG v4)
         * FROM NASDAQ
         * USE SKIP_TILL_NEXT_MATCH
         * WHERE 324 <= v1.open <= 334 AND 110 <= v2.open <= 140 AND v3.open >= v1.open * 1.003 AND v4.open <= v2.open * 0.997
         * WITHIN 18 minutes
         * RETURN COUNT(*)
         */
        resCount = 0;
        Pattern<StockEvent, ?> pattern = Pattern.<StockEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("MSFT") &&
                                event.getOpen() >= 324 && event.getOpen() <= 334;
                    }
                }
        ).followedBy("v2").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("GOOG")
                                && event.getOpen() >= 110 && event.getOpen() <= 140;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("MSFT");
                        boolean dc = false;
                        for (StockEvent v1Event : ctx.getEventsForPattern("v1")) {
                            dc = event.getOpen() >= v1Event.getOpen() * 1.003;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v4").where(
                new IterativeCondition<StockEvent>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("GOOG");
                        boolean dc = false;
                        for(StockEvent v2Event : ctx.getEventsForPattern("v2")){
                            dc = event.getOpen() <= v2Event.getOpen() * 0.997;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1081));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        // set one thread
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<StockEvent> events = new ArrayList<>(byteEvents.size());
        for(byte[] record : byteEvents){
            events.add(new StockEvent(record, schema));
        }

        DataStream<StockEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(StockEvent event){
                        return event.getTimestamp();
                    }
                });

        PatternStream<StockEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<StockEvent, String>) map1 -> {
                    //StockEvent firstEvent = map1.get("v1").get(0);
                    //StockEvent secondEvent = map1.get("v2").get(0);
                    //StockEvent thirdEvent = map1.get("v3").get(0);
                    //StockEvent fourthEvent = map1.get("v4").get(0);
                    //return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "|" + fourthEvent + "]";
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

    public static int nasdaqSecondQuery(List<StockEvent> events) {
        resCount = 0;
        Pattern<StockEvent, ?> pattern = Pattern.<StockEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("MSFT") &&
                                event.getOpen() >= 324 && event.getOpen() <= 334;
                    }
                }
        ).followedBy("v2").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(StockEvent event) {
                        return event.getTicker().equals("GOOG")
                                && event.getOpen() >= 110 && event.getOpen() <= 140;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("MSFT");
                        boolean dc = false;
                        for (StockEvent v1Event : ctx.getEventsForPattern("v1")) {
                            dc = event.getOpen() >= v1Event.getOpen() * 1.003;
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v4").where(
                new IterativeCondition<StockEvent>() {
                    @Override
                    public boolean filter(StockEvent event, Context<StockEvent> ctx) throws Exception {
                        boolean ic = event.getTicker().equals("GOOG");
                        boolean dc = false;
                        for(StockEvent v2Event : ctx.getEventsForPattern("v2")){
                            dc = event.getOpen() <= v2Event.getOpen() * 0.997;
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(1081));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        // set one thread
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(StockEvent event){
                        return event.getTimestamp();
                    }
                });

        PatternStream<StockEvent> patternStream = CEP.pattern(inputEventStream, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<StockEvent, String>) map1 -> {
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

