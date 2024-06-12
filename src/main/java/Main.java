import org.roaringbitmap.RangeBitmap;

import java.nio.ByteBuffer;
import java.util.Random;

public class Main {
    public static void main(String[] args) {
        // Long.MAX_VALUE -> size: 82339 average bits: 8
        // 82012 -> size: 81990 average bits: 8
        // 1024 -> size: 81969 average bits: 8
        RangeBitmap.Appender appender = RangeBitmap.appender(1024 * 10);

        Random random = new Random();
        // 10 K
        int keyNum = 1024 * 10;
        for(int i = 0 ; i < keyNum ; i++) {
            int key = random.nextInt(10_000);
            appender.add(key);
        }

        int size = appender.serializedSizeInBytes();
        ByteBuffer buff = ByteBuffer.allocate(size);
        appender.serialize(buff);
        buff.flip();

        System.out.println("size: " + size + " average bits: " + (size / keyNum));
    }
}