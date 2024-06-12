package generator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.SyncFailedException;
import java.util.Random;

/**
 * int -> Uniform, int -> uniform, double -> uniform, double -> uniform
 */
public class DataGenerator {
    private final String[] attrDataTypes;
    private final int eventTypeNum;
    private final Random random;

    public DataGenerator(String[] attrDataTypes, int eventTypeNum) {
        this.attrDataTypes = attrDataTypes;
        this.eventTypeNum = eventTypeNum;
        random = new Random();
    }

    public void generateDataset(String filePath, int recordNum){
        long startTime = System.currentTimeMillis();
        double[] probability = zipfProbability(1.3);
        // Cumulative Distribution Function
        double[] cdf = new double[eventTypeNum];
        cdf[eventTypeNum - 1] = 1;
        cdf[0] = probability[0];

        int[] cnt = new int[50];

        for(int i = 1; i < eventTypeNum - 1; ++i){
            cdf[i] = cdf[i - 1] + probability[i];
            // System.out.println("cdf[" + i + "]: " + cdf[i]);
        }

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

                boolean noAppendType = true;
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
                cnt[mid]++;
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

            for(int i = 0; i < 50; ++i){
                System.out.println("cnt[" + i + "]: " + cnt[i]);
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

        System.out.println("zipf skew: " + alpha);
        System.out.print("zipf probability:\n[");
        for(int i = 0; i < eventTypeNum - 1; ++i){
            String value = String.format("%.4f", ans[i]);
            System.out.print(value + ",");
            if(i % 10 == 9){
                System.out.println();
            }
        }
        String value = String.format("%.4f", ans[eventTypeNum - 1]);
        System.out.println(value + "]");
        System.out.println("zipf sum probability: " + sumPro);

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
        int eventTypeNum = 50;

        String dir = System.getProperty("user.dir");
        String filePath = dir + File.separator + "src" + File.separator + "main" +
                File.separator + "dataset" + File.separator + "synthetic_1G.csv";

        DataGenerator generator = new DataGenerator(attrDataTypes, eventTypeNum);
        long startTime = System.currentTimeMillis();
        generator.generateDataset(filePath, 1_000_000_000);
        long endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - startTime) / 60000 + "min");
    }
}

