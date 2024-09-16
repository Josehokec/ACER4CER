//package store;
//
//import java.nio.ByteBuffer;
//
//public class EightBytesUtils {
//    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
//    public static byte[] longToBytes(long x){
//        buffer.putLong(0, x);
//        return buffer.array().clone();
//    }
//
//    public static long bytesToLong(byte[] bytes) {
//        buffer.put(bytes, 0, bytes.length);
//        //need flip
//        buffer.flip();
//        long ans = buffer.getLong();
//        buffer.clear();
//        return ans;
//    }
//
//    public static byte[] doubleToBytes(double x){
//        buffer.putDouble(0, x);
//        return buffer.array().clone();
//    }
//
//    public static double bytesToDouble(byte[] bytes) {
//        buffer.put(bytes, 0, bytes.length);
//        buffer.flip();
//        double ans = buffer.getDouble();
//        buffer.clear();
//        return ans;
//    }
//
//    public static void test(){
//        long v1 = -3;
//        long v2 = 5;
//        double v3 = 0.542;
//        double v4 = 1.534;
//        double v5 = -6.4;
//
//        byte[] array1 = EightBytesUtils.longToBytes(v1);
//        byte[] array2 = EightBytesUtils.longToBytes(v2);
//        byte[] array3 = EightBytesUtils.doubleToBytes(v3);
//        byte[] array4 = EightBytesUtils.doubleToBytes(v4);
//        byte[] array5 = EightBytesUtils.doubleToBytes(v5);
//
//        System.out.println("v1: " + EightBytesUtils.bytesToLong(array1));
//        System.out.println("v2: " + EightBytesUtils.bytesToLong(array2));
//        System.out.println("v3: " + EightBytesUtils.bytesToDouble(array3));
//        System.out.println("v4: " + EightBytesUtils.bytesToDouble(array4));
//        System.out.println("v5: " + EightBytesUtils.bytesToDouble(array5));
//    }
//}
