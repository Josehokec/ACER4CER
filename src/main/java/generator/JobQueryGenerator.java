package generator;

import net.sf.json.JSONArray;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JobQueryGenerator {

    public void generateQuery(String filePath, int queryNUm){
        Random random = new Random(10);
        // all query windows: 10ms, 50ms, 100ms, 1000ms
        long[] windows = {10_000L, 30_000L, 60_000L, 120_000L};
        List<String> queries = new ArrayList<>(queryNUm);

        // here we generate three pattern:
        // pattern 0: SEQ(SUBMIT[0] v0, SCHEDULE[1] v1, FINISH[4] v2)
        // pattern 1: SEQ(SUBMIT[0] v0, SCHEDULE[1] v1, KILL[5] v2)
        // pattern 2: SEQ(SUBMIT[0] v0, SCHEDULE[1] v1, EVICT[2] v2)
        for(int i = 0; i < queryNUm; ++i){
            // schedulingClass 0 ~ 3, without schedulingClass -> 4
            int schedulingClass = random.nextInt(5);
            long window = windows[random.nextInt(4)];
            int patternType = random.nextInt(3);

            StringBuffer query = new StringBuffer(256);
            switch(patternType){
                case 0 -> query.append("PATTERN SEQ(0 v0, 1 v1, 4 v2)\n");
                case 1 -> query.append("PATTERN SEQ(0 v0, 1 v1, 5 v2)\n");
                case 2 -> query.append("PATTERN SEQ(0 v0, 1 v1, 2 v2)\n");
            }

            query.append("FROM job\n");
            query.append("USING SKIP_TILL_NEXT_MATCH\n");
            query.append("WHERE ");

            // add predicate
            if(schedulingClass == 4){
                query.append("v0.jobID = v1.jobID");
                query.append(" AND ");
                query.append("v1.jobID = v2.jobID\n");
            }else{
                query.append(schedulingClass).append(" <= v").append(0).append(".schedulingClass").append(" <= ").append(schedulingClass);
                query.append(" AND ");
                query.append(schedulingClass).append(" <= v").append(1).append(".schedulingClass").append(" <= ").append(schedulingClass);
                query.append(" AND ");
                query.append(schedulingClass).append(" <= v").append(2).append(".schedulingClass").append(" <= ").append(schedulingClass);
                query.append(" AND ");
                query.append("v0.jobID = v1.jobID");
                query.append(" AND ");
                query.append("v1.jobID = v2.jobID\n");
            }

            query.append("WITHIN ").append(window).append(" units\n");
            query.append("RETURN COUNT(*)");
            System.out.println(query);
            queries.add(query.toString());

        }

        JSONArray jsonArray = JSONArray.fromObject(queries);
        // JSONObject jsonArray = JSONObject.fromObject(queries);
        FileWriter fileWriter = null;
        try{
            fileWriter = new FileWriter(filePath);
            fileWriter.write(jsonArray.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                fileWriter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args){
        JobQueryGenerator jqg = new JobQueryGenerator();
        // random generate 300 queries
        jqg.generateQuery("job_query.json", 500);
    }
}
