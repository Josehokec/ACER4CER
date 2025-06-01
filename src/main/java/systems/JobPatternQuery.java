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

public class JobPatternQuery {
    private static int resCount = 0;

    public static int jobFirstQuery(List<byte[]> byteEvents, EventSchema schema){
        resCount = 0;
        /**
         * PATTERN SEQ(SUBMIT v1, SCHEDULE v2, KILL v3)
         * FROM job
         * USING SKIP_TILL_NEXT_MATCH
         * WHERE 2 <= v1.schedulingClass <= 2 AND 2 <= v2.schedulingClass <= 2 AND 2 <= v3.schedulingClass <= 2 AND v1.jobID = v2.jobID AND v2.jobID = v3.jobID
         * WITHIN 100 units
         * RETURN COUNT(*)
         */
        Pattern<JobEvent, ?> pattern = Pattern.<JobEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(JobEvent event) {
                        return event.getEventType().equals("SUBMIT") &&
                                event.getSchedulingClass() == 2;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("SCHEDULE") &&
                                event.getSchedulingClass() == 2;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("KILL") &&
                                event.getSchedulingClass() == 2;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v2")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(101));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<JobEvent> events = new ArrayList<>();
        for(byte[] event : byteEvents){
            events.add(new JobEvent(event, schema));
        }

        DataStream<JobEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(JobEvent element) {
                        return element.getTimestamp();
                    }
                });

        PatternStream<JobEvent> patternStream = CEP.pattern(inputEventStream.keyBy(JobEvent::getJobID), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<JobEvent, String>) map1 -> {
                    resCount++;
                    JobEvent firstEvent = map1.get("v1").get(0);
                    JobEvent secondEvent = map1.get("v2").get(0);
                    JobEvent thirdEvent = map1.get("v3").get(0);
                    return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";
                    // return "";
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

    public static int jobFirstQuery(List<JobEvent> events){
        resCount = 0;
        /**
         * PATTERN SEQ(SUBMIT v1, SCHEDULE v2, KILL v3)
         * FROM job
         * USING SKIP_TILL_NEXT_MATCH
         * WHERE 2 <= v1.schedulingClass <= 2 AND 2 <= v2.schedulingClass <= 2 AND 2 <= v3.schedulingClass <= 2 AND v1.jobID = v2.jobID AND v2.jobID = v3.jobID
         * WITHIN 100 units
         * RETURN COUNT(*)
         */
        Pattern<JobEvent, ?> pattern = Pattern.<JobEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(JobEvent event) {
                        return event.getEventType().equals("SUBMIT") &&
                                event.getSchedulingClass() == 2;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("SCHEDULE") &&
                                event.getSchedulingClass() == 2;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("KILL") &&
                                event.getSchedulingClass() == 2;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v2")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(101));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JobEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(JobEvent element) {
                        return element.getTimestamp();
                    }
                });

        PatternStream<JobEvent> patternStream = CEP.pattern(inputEventStream.keyBy(JobEvent::getJobID), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<JobEvent, String>) map1 -> {
                    resCount++;
                    // JobEvent firstEvent = map1.get("v1").get(0);
                    // JobEvent secondEvent = map1.get("v2").get(0);
                    // JobEvent thirdEvent = map1.get("v3").get(0);
                    // return "[" + firstEvent + "|" + secondEvent + "|" + thirdEvent + "]";

                    return "";
                }
        );

        // select.print();
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
        return resCount;
    }

    public static int jobSecondQuery(List<byte[]> byteEvents, EventSchema schema){
        resCount = 0;
        /**
         * PATTERN SEQ(SUBMIT v1, SCHEDULE v2, FINISH v3)
         * FROM job
         * USING SKIP_TILL_NEXT_MATCH
         * WHERE 0 <= v1.schedulingClass <= 0 AND 0 <= v2.schedulingClass <= 0 AND 0 <= v3.schedulingClass <= 0 AND v1.jobID = v2.jobID AND v2.jobID = v3.jobID
         * WITHIN 200 units
         * RETURN COUNT(*)
         */
        Pattern<JobEvent, ?> pattern = Pattern.<JobEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(JobEvent event) {
                        return event.getEventType().equals("SUBMIT") &&
                                event.getSchedulingClass() == 0;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("SCHEDULE") &&
                                event.getSchedulingClass() == 0;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("FINISH") &&
                                event.getSchedulingClass() == 0;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v2")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(201));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<JobEvent> events = new ArrayList<>();
        for(byte[] event : byteEvents){
            events.add(new JobEvent(event, schema));
        }

        DataStream<JobEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(JobEvent element) {
                        return element.getTimestamp();
                    }
                });

        PatternStream<JobEvent> patternStream = CEP.pattern(inputEventStream.keyBy(JobEvent::getJobID), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<JobEvent, String>) map1 -> {
                    //JobEvent firstEvent = map1.get("v1").get(0);
                    //JobEvent secondEvent = map1.get("v2").get(0);
                    //JobEvent thirdEvent = map1.get("v3").get(0);
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

    public static int jobSecondQuery(List<JobEvent> events){
        resCount = 0;
        /**
         * PATTERN SEQ(SUBMIT v1, SCHEDULE v2, FINISH v3)
         * FROM job
         * USING SKIP_TILL_NEXT_MATCH
         * WHERE 0 <= v1.schedulingClass <= 0 AND 0 <= v2.schedulingClass <= 0 AND 0 <= v3.schedulingClass <= 0 AND v1.jobID = v2.jobID AND v2.jobID = v3.jobID
         * WITHIN 200 units
         * RETURN COUNT(*)
         */
        Pattern<JobEvent, ?> pattern = Pattern.<JobEvent>begin("v1").where(
                new SimpleCondition<>() {
                    @Override
                    public boolean filter(JobEvent event) {
                        return event.getEventType().equals("SUBMIT") &&
                                event.getSchedulingClass() == 0;
                    }
                }
        ).followedBy("v2").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("SCHEDULE") &&
                                event.getSchedulingClass() == 0;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v1")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).followedBy("v3").where(
                new IterativeCondition<>() {
                    @Override
                    public boolean filter(JobEvent event, Context<JobEvent> ctx) throws Exception {
                        boolean ic = event.getEventType().equals("FINISH") &&
                                event.getSchedulingClass() == 0;
                        boolean dc = false;
                        for (JobEvent preEvent : ctx.getEventsForPattern("v2")) {
                            dc = event.getJobID().equals(preEvent.getJobID());
                        }
                        return dc && ic;
                    }
                }
        ).within(Time.milliseconds(201));

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // set event timestamp to time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JobEvent> inputEventStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<>() {
                    @Override
                    public long extractAscendingTimestamp(JobEvent element) {
                        return element.getTimestamp();
                    }
                });

        PatternStream<JobEvent> patternStream = CEP.pattern(inputEventStream.keyBy(JobEvent::getJobID), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(
                (PatternSelectFunction<JobEvent, String>) map1 -> {
                    //JobEvent firstEvent = map1.get("v1").get(0);
                    //JobEvent secondEvent = map1.get("v2").get(0);
                    //JobEvent thirdEvent = map1.get("v3").get(0);
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
}
