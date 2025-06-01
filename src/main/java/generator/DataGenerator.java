package generator;

import arrival.JsonMap;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.SyncFailedException;
import java.util.*;

/**
 * int -> Uniform, int -> uniform, double -> uniform, double -> uniform
 */
public class DataGenerator {
    private final String[] attrDataTypes;
    private final int eventTypeNum = 20;
    private final Random random;
    private long startTs = 1747405414L;

    private double[] probability = zipfProbability(1.2);
    // Cumulative Distribution Function
    private double[] cdf = new double[eventTypeNum];

    public DataGenerator(String[] attrDataTypes) {
        this.attrDataTypes = attrDataTypes;
        random = new Random(11);

        cdf[eventTypeNum - 1] = 1;
        cdf[0] = probability[0];
        for(int i = 1; i < eventTypeNum - 1; ++i){
            cdf[i] = cdf[i - 1] + probability[i];
            // System.out.println("cdf[" + i + "]: " + cdf[i]);
        }

        // write arrival
        HashMap<String, Double> arrivals = new HashMap<>(eventTypeNum * 2);

        for (int i = 0; i < eventTypeNum; i++) {
            String type = "TYPE_" + i;
            arrivals.put(type, probability[i]);
        }

        String dir = System.getProperty("user.dir");
        String jsonFilePath = dir + File.separator + "src" + File.separator + "main" + File.separator + "java"
                + File.separator + "arrival" + File.separator + "SYNTHETIC_arrivals.json";
        JsonMap.arrivalMapToJson(arrivals, jsonFilePath);
    }

    // batch size ==> 512
    public List<String[]> generateDataInBatch(int batchSize, boolean inOrder){
        List<String[]> data = new ArrayList<>(batchSize);
        for(int i = 0; i < batchSize; i++){
            String[] record = new String[attrDataTypes.length + 2];
            double pro = random.nextDouble();

            // here can use binary search to optimize
            int left = 0;
            int right = eventTypeNum - 1;
            int mid = (left + right) >> 1;
            while(left <= right){
                if(pro <= cdf[mid]){
                    if(mid == 0 || pro > cdf[mid - 1]){
                        break;
                    }else{
                        right = mid - 1;
                        mid = (left + right) >> 1;
                    }
                }else{
                    if(mid == eventTypeNum - 1){
                        break;
                    }else{
                        left = mid + 1;
                        mid = (left + right) >> 1;
                    }
                }
            }

            String type = "TYPE_" + mid;
            int a1 = random.nextInt(1, 1001);
            int a2 = random.nextInt(1, 1001);
            // double a3 = random.nextDouble(0, 10000);
            double a3 = random.nextGaussian(5000, 1000);
            if(a3 < 0){
                a3 = 0;
            }else if(a3 > 10000){
                a3 = 10000;
            }
            double a4 = random.nextGaussian(5000, 1000);
            if(a4 < 0){
                a4 = 0;
            }else if(a4 > 10000){
                a4 = 10000;
            }
            long ts = startTs++;

            record[0] = type;
            record[1] = String.valueOf(a1);
            record[2] = String.valueOf(a2);
            record[3] = String.format("%.1f", a3);
            record[4] = String.format("%.1f", a4);
            record[5] = String.valueOf(ts);
            data.add(record);
        }

        if(!inOrder){
            Collections.shuffle(data, random);
        }

        return data;
    }

    public void generateDataset(String filePath, int recordNum){
        long startTime = System.currentTimeMillis();
        int minVal = 0;
        int maxVal = 1000;

        File file = new File(filePath);

        try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, false))){
            // file header
            String header = "EventType(String),Attribute1(INT),Attribute2(INT),Attribute3(DOUBLE),Attribute4(DOUBLE),TIMESTAMP\n";
            out.write(header.getBytes());
            // event format: eventType,attribute^1,...,attribute^d,timestamp
            for(int i = 0; i < recordNum; ++i){
                StringBuilder record = new StringBuilder(256);

                double pro = random.nextDouble();
                // here can use binary search to optimize
                int left = 0;
                int right = eventTypeNum - 1;
                int mid = (left + right) >> 1;
                while(left <= right){
                    if(pro <= cdf[mid]){
                        if(mid == 0 || pro > cdf[mid - 1]){
                            break;
                        }else{
                            right = mid - 1;
                            mid = (left + right) >> 1;
                        }
                    }else{
                        if(mid == eventTypeNum - 1){
                            break;
                        }else{
                            left = mid + 1;
                            mid = (left + right) >> 1;
                        }
                    }
                }
                int typeId = mid;

                // System.out.println("pro: " + pro + " type_id: " + typeId);
                record.append("TYPE_").append(typeId).append(",");

                for (String attrDataType : attrDataTypes) {
                    int mu = 500;
                    int sigma = 150;
                    switch (attrDataType) {
                        case "INT_UNIFORM" -> record.append(getUniformInteger(minVal, maxVal)).append(",");
                        case "INT_GAUSSIAN" -> record.append(getGaussianInteger(minVal, maxVal, mu, sigma)).append(",");
                        case "DOUBLE_UNIFORM" -> {
                            String value = String.format("%.2f", getUniformDouble(minVal, maxVal));
                            record.append(value).append(",");
                        }
                        case "DOUBLE_GAUSSIAN" -> {
                            String value = String.format("%.2f", getGaussianDouble(minVal, maxVal, mu, sigma));
                            record.append(value).append(",");
                        }
                        default -> throw new RuntimeException("do not support this data type");
                    }
                }

                long curTime = startTime + i;
                record.append(curTime).append("\n");
                out.write(record.toString().getBytes());
                if((i + 1) % 100_000_000 == 0){
                    double display = (i + 1 + 0.0) / 100_000_000;
                    System.out.println("write " + display + "%...");
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * generate zipf distribution
     * @param   alpha   skew param
     * @return          probability
     */
    public double[] zipfProbability(double alpha){
        double[] ans = new double[eventTypeNum];
        double C = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            C += Math.pow((1.0 / i), alpha);
        }
        double sumPro = 0;
        for(int i = 1; i <= eventTypeNum; ++i){
            double pro = 1.0 / (Math.pow(i, alpha) * C);
            ans[i - 1] = pro;
            sumPro += pro;
        }

//        System.out.println("zipf skew: " + alpha);
//        System.out.print("zipf probability:\n[");
//        for(int i = 0; i < eventTypeNum - 1; ++i){
//            String value = String.format("%.4f", ans[i]);
//            System.out.print(value + ",");
//            if(i % 10 == 9){
//                System.out.println();
//            }
//        }
//        String value = String.format("%.4f", ans[eventTypeNum - 1]);
//        System.out.println(value + "]");
//        System.out.println("zipf sum probability: " + sumPro);

        return ans;
    }

    public final int getUniformInteger(int minVal, int maxVal){
        // [minVal, maxVal + 1) -> [minVal, maxVal]
        return random.nextInt(minVal, maxVal + 1);
    }

    public final int getGaussianInteger(int minVal, int maxVal, double mu, double sigma){
        // x ~(mu, sigma) & minVal <= x <= maxVal
        int value = (int) random.nextGaussian(mu, sigma);
        if(value < minVal){
            value = minVal;
        }else if(value > maxVal){
            value = maxVal;
        }
        return value;
    }

    public final double getUniformDouble(double minVal, double maxVal){
        return random.nextDouble(minVal, maxVal);
    }

    public final double getGaussianDouble(int minVal, int maxVal, double mu, double sigma){
        // x ~(mu, sigma) & minVal <= x <= maxVal
        double value =  random.nextGaussian(mu, sigma);
        if(value < minVal){
            value = minVal;
        }else if(value > maxVal){
            value = maxVal;
        }
        return value;
    }

    public static void main(String[] args){
        String[] attrDataTypes = {"INT_UNIFORM", "INT_UNIFORM", "DOUBLE_UNIFORM", "DOUBLE_UNIFORM"};


        //String dir = System.getProperty("user.dir") + File.separator + "src" + File.separator + "main";
        //String filePath = dir + File.separator + "dataset" + File.separator + "synthetic_100.csv";

        DataGenerator generator = new DataGenerator(attrDataTypes);
        //long startTime = System.currentTimeMillis();
        //generator.generateDataset(filePath, 100);
        List<String[]> records = generator.generateDataInBatch(8, false);
        for(String[] record : records){
            System.out.println("record: " + Arrays.toString(record));
        }
        //long endTime = System.currentTimeMillis();
        //System.out.println("cost " + (endTime - startTime) / 60000 + "min");
        //long startTime = System.currentTimeMillis();
        //generator.generateDataset(filePath, 100);

        System.out.println("-------test-------");

        DataGenerator generator2 = new DataGenerator(attrDataTypes);

        List<String[]> records2 = generator2.generateDataInBatch(8, false);
        for(String[] record : records2){
            System.out.println("record: " + Arrays.toString(record));
        }
    }
}

