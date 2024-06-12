package common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampling{
    private final List<List<Long>> samples;
    private final int maxSampleNum;

    public ReservoirSampling(int indexNum){
        samples = new ArrayList<>();
        maxSampleNum = 5000;
        for(int i = 0; i < indexNum; ++i){
            samples.add(new ArrayList<>(maxSampleNum));
        }
    }

    public final void sampling(long[] indexAttrValues, int recordIndices){
        Random random = new Random();
        if(recordIndices < maxSampleNum){
            for(int i = 0; i < indexAttrValues.length; ++i){
                samples.get(i).add(indexAttrValues[i]);
            }
        }else{
            // [0, num)
            int r = random.nextInt(recordIndices + 1);
            if (r < maxSampleNum) {
                // replace
                for(int i = 0; i < indexAttrValues.length; ++i){
                    samples.get(i).set(r, indexAttrValues[i]);
                }
            }
        }
    }

    public final double selectivity(int indexId, long min, long max){
        int cnt = 0;
        List<Long> sampleAttrList = samples.get(indexId);
        int sampleNum = sampleAttrList.size();
        for(long value : sampleAttrList){
            if(value >= min && value <= max){
                cnt++;
            }
        }
        return (cnt + 0.0) / sampleNum;
    }
}
