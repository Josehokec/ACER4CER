package common;

/**
 * Converter can
 * 1.convert byte array to long type
 * 2.convert Long type data into byte arrays
 */
public class Converter {
    /**
     * convert byte array to Long type
     * low bits in the presence of low bits
     * @param bytes byte array
     * @return long data
     */
    public static long bytesToLong(byte[] bytes){
        int len = bytes.length;

        switch (len) {
            case 0 -> {
                return 0;
            }
            case 1 -> {
                return bytes[0] & 0xff;
            }
            case 2 -> {
                return (bytes[1] & 0xff) << 8 | (bytes[0] & 0xff);
            }
            case 3 -> {
                return (bytes[2] & 0xff) << 16 | (bytes[1] & 0xff) << 8 | (bytes[0] & 0xff);
            }
            case 4 -> {
                return (bytes[3] & 0xffL) << 24 | (bytes[2] & 0xffL) << 16 | (bytes[1] & 0xffL) << 8 |
                        (bytes[0] & 0xffL);
            }
            case 5 -> {
                return (bytes[4] & 0xffL) << 32 | (bytes[3] & 0xffL) << 24 | (bytes[2] & 0xffL) << 16 |
                        (bytes[1] & 0xffL) << 8 | (bytes[0] & 0xffL);
            }
            case 6 -> {
                return (bytes[5] & 0xffL) << 40 | (bytes[4] & 0xffL) << 32 | (bytes[3] & 0xffL) << 24 |
                        (bytes[2] & 0xffL) << 16 | (bytes[1] & 0xffL) << 8 | (bytes[0] & 0xffL);
            }
            case 7 -> {
                return (bytes[6] & 0xffL) << 48 | (bytes[5] & 0xffL) << 40 | (bytes[4] & 0xffL) << 32 |
                        (bytes[3] & 0xffL) << 24 | (bytes[2] & 0xffL) << 16 | (bytes[1] & 0xffL) << 8 |
                        (bytes[0] & 0xffL);
            }
            case 8 -> {
                return (bytes[7] & 0xffL) << 56 | (bytes[6] & 0xffL) << 48 | (bytes[5] & 0xffL) << 40 |
                        (bytes[4] & 0xffL) << 32 | (bytes[3] & 0xffL) << 24 | (bytes[2] & 0xffL) << 16 |
                        (bytes[1] & 0xffL) << 8 | (bytes[0] & 0xffL);
            }
            default -> {
                System.out.println("Byte array too long, can not convert to long.");
                return 0;
            }
        }
    }

    /**
     * convert the low bitLen bits of timestamp into byte arrays
     * @param timestamp timestamp
     * @param bitLen the length of the low bit of the timestamp
     * @return byte array
     */
    public static byte[] longToBytes(long timestamp, int bitLen){
        int byteLen;
        if((bitLen & 0x07) == 0){
            byteLen = (bitLen >> 3);
        }else{
            byteLen = (bitLen >> 3) + 1;
        }
        long tsRight = timestamp & ((1L << bitLen) - 1);

        byte[] ans = new byte[byteLen];
        for(int i = 0; i < byteLen; ++i){
            ans[i] = (byte) (tsRight & 0xffL);
            tsRight >>= 8;
        }
        return ans;
    }

    /**
    public static void main(String[] args){
        //11000011010011011111001000000011101010000
        long x = 1677652658000L;
        byte[] tsRight = longToByteArray(x, 24);
        long t = byteArrayToLong(tsRight);
        System.out.println("t: " + t);

    }
     **/

    public static byte[] longToBytes(long x) {
        byte[] bytes=new byte[8];
        bytes[7]=(byte) (x>>56);
        bytes[6]=(byte) (x>>48);
        bytes[5]=(byte) (x>>40);
        bytes[4]=(byte) (x>>32);
        bytes[3]=(byte) (x>>24);
        bytes[2]=(byte) (x>>16);
        bytes[1]=(byte) (x>>8);
        bytes[0]=(byte) x;
        return bytes;
    }

    public static byte[] intToBytes(int integer) {
        byte[] bytes=new byte[4];
        bytes[3]=(byte) (integer>>24);
        bytes[2]=(byte) (integer>>16);
        bytes[1]=(byte) (integer>>8);
        bytes[0]=(byte) integer;

        return bytes;
    }

    public static int bytesToInt(byte[] bytes ){
        int int1=bytes[0]&0xff;
        int int2=(bytes[1]&0xff)<<8;
        int int3=(bytes[2]&0xff)<<16;
        int int4=(bytes[3]&0xff)<<24;

        return int1|int2|int3|int4;
    }


}
