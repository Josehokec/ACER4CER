
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

/*this class only test some simple code lines*/
public class Main {

    /*test range bitmap*/
    public static void main(String[] args) {
        int maxRange = 1000;
        Random rand = new Random(7);
        RangeBitmap.Appender appender = RangeBitmap.appender(maxRange);

        int keyNum = 50_000;
        for(int i = 0; i < keyNum; i++) {
            appender.add(rand.nextInt(maxRange));
        }



        RangeBitmap rb = appender.build();

        RoaringBitmap selBitmap = new RoaringBitmap();

        long startTime = System.nanoTime();
        int loop = 1;
        for(int i = 0; i < loop; i++){
            RoaringBitmap ans = rb.gte(-2);
            int card = ans.getCardinality();
            System.out.println("card: " + card);
        }
        long endTime = System.nanoTime();
        System.out.println("range query cost: " + (endTime - startTime) + "ns");///loop
        //rb.between(0, 100);


//        int size = appender.serializedSizeInBytes();
//        System.out.println("size:" + (size / 1024) + "KB");
//
//        // appenderList.get(i).serialize(buff);
//        System.out.println("hello world");
    }
}
