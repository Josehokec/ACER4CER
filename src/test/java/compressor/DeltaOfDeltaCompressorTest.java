package compressor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Random;

class DeltaOfDeltaCompressorTest {

    public static void verifyDelta2Correctness(long[] timestamps) {
        System.out.println("Original data size: " + timestamps.length * 8+ "bytes");

        LongArrayOutput output = new LongArrayOutput(timestamps.length);
        DeltaOfDeltaCompressor compressor = new DeltaOfDeltaCompressor(timestamps[0], output);

        Arrays.stream(timestamps).forEach(compressor::addValue);
        // we first close, then get long array
        compressor.close();

        // create ByteBuffer
        long[] compressedValues = output.getLongArray();
        int len = compressedValues.length;
        System.out.println("compressed data size: " + len * 8 + "bytes");
        System.out.println("compression ratio: " + (len + 0.0) / (timestamps.length));

        // starting read data... long = 64 byte
        ByteBuffer byteBuffer = ByteBuffer.allocate(len << 6);
        for (long compressedValue : compressedValues) {
            byteBuffer.putLong(compressedValue);
        }
        // here we need to flip
        byteBuffer.flip();
        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);

        DeltaOfDeltaDecompressor decoder = new DeltaOfDeltaDecompressor(input);

        // Replace with stream once GorillaDecompressor supports it
        for (long timestamp : timestamps) {
            long ts = decoder.readTimestamp();
            if (ts != timestamp) {
                System.out.println("Timestamp did not match");
            }

        }
        if(decoder.readTimestamp() != -1){
            System.out.println("without end");
        }
    }

    public static void verifyDelta2BatchCorrectness(long[] timestamps) {
        System.out.println("-----------delta + zigzag + delta-----------");
        List<Long> tsList = new ArrayList<>(timestamps.length);
        for(long timestamp : timestamps) {
            tsList.add(timestamp);
        }

        long startTime = System.nanoTime();
        long[] compressedValues = DeltaOfDeltaCompressor.compress(tsList);
        long endTime = System.nanoTime();
        System.out.println("compression took " + (endTime - startTime) + " ns");

        int len = compressedValues.length;
        String compressedRatio = String.format ("%.4f", (len + 0.0) / (timestamps.length));
        System.out.println("Original data size: " + timestamps.length * 8 + "bytes, compressed data size: " +
                len * 8 + "bytes, compression ratio: " + compressedRatio);
        // starting read data... long = 64 byte
        ByteBuffer byteBuffer = ByteBuffer.allocate(len << 6);
        for (long compressedValue : compressedValues) {
            byteBuffer.putLong(compressedValue);
        }

        byteBuffer.flip();
        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);

        startTime = System.nanoTime();
        long[] decodedValues = DeltaOfDeltaDecompressor.decompress(input, timestamps.length);
        endTime = System.nanoTime();
        System.out.println("decompression took " + (endTime - startTime) + " ns");

        for (int i = 0; i < timestamps.length; i++) {
            if (decodedValues[i] != timestamps[i]) {
                throw new RuntimeException("Timestamp did not match, position: " + i + ", timestamp: " + timestamps[i] + ", decoded value: " + decodedValues[i]);
            }
        }
    }

    public static void verifyDeltaVarIntBatchCorrectness(long[] timestamps) {
        System.out.println("-----------delta + zigzag + varint-----------");
        List<Long> tsList = new ArrayList<>(timestamps.length);
        for(long timestamp : timestamps) {
            tsList.add(timestamp);
        }

        long startTime = System.nanoTime();
        long[] compressedValues = DeltaVarIntCompressor.compress(tsList);
        long endTime = System.nanoTime();
        System.out.println("compression took " + (endTime - startTime) + " ns");

        int len = compressedValues.length;
        String compressedRatio = String.format ("%.4f", (len + 0.0) / (timestamps.length));
        System.out.println("Original data size: " + timestamps.length * 8 + "bytes, compressed data size: " +
                len * 8 + "bytes, compression ratio: " + compressedRatio);

        // starting read data... long = 64 byte
        ByteBuffer byteBuffer = ByteBuffer.allocate(len << 6);
        for (long compressedValue : compressedValues) {
            byteBuffer.putLong(compressedValue);
        }
        // here we need to flip
        byteBuffer.flip();
        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);

        startTime = System.nanoTime();
        long[] decodedValues = DeltaVarIntDecompressor.decompress(input, timestamps.length);
        endTime = System.nanoTime();
        System.out.println("decompression took " + (endTime - startTime) + " ns");

        for (int i = 0; i < timestamps.length; i++) {
            if (decodedValues[i] != timestamps[i]) {
                System.out.println("Timestamp did not match, decoded value: " + decodedValues[i] + ", original timestamp: " + timestamps[i]);
            }
        }


    }

    public static void verifyDeltaSimple8BBatchCorrectness(long[] timestamps) {
        System.out.println("-----------delta + zigzag + simple8b-----------");
        List<Long> tsList = new ArrayList<>(timestamps.length);
        for(long timestamp : timestamps) {
            tsList.add(timestamp);
        }

        long startTime = System.nanoTime();
        long[] compressedValues = DeltaSimple8BCompressor.compress(tsList);
        long endTime = System.nanoTime();
        System.out.println("compression took " + (endTime - startTime) + " ns");

        // len = 66
        int len = compressedValues.length;
        String compressedRatio = String.format ("%.4f", (len + 0.0) / (timestamps.length));
        System.out.println("Original data size: " + timestamps.length * 8 + "bytes, compressed data size: " +
                len * 8 + "bytes, compression ratio: " + compressedRatio);

        // long = 8 byte
        ByteBuffer byteBuffer = ByteBuffer.allocate(len << 3);
        for (long compressedValue : compressedValues) {
            byteBuffer.putLong(compressedValue);
        }
        // here we need to flip
        byteBuffer.flip();
        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);

        startTime = System.nanoTime();
        long[] decodedValues = DeltaSimple8BDecompressor.decompress(input, timestamps.length);
        endTime = System.nanoTime();
        System.out.println("decompression took " + (endTime - startTime) + " ns");

        for (int i = 0; i < timestamps.length; i++) {
            if (decodedValues[i] != timestamps[i]) {
                System.out.println("Timestamp did not match, decoded value: " + decodedValues[i] + ", original timestamp: " + timestamps[i]);
            }
        }


    }

    public static void verifyDeltaBatchCorrectness(long[] timestamps){
        System.out.println("-----------simple delta compression-----------");
        List<Long> tsList = new ArrayList<>(timestamps.length);
        for(long timestamp : timestamps) {
            tsList.add(timestamp);
        }

        long startTime = System.nanoTime();
        long[] compressedValues = DeltaCompressor.compress(tsList);
        long endTime = System.nanoTime();
        System.out.println("compression took " + (endTime - startTime) + " ns");

        // len = 66
        int len = compressedValues.length;
        String compressedRatio = String.format ("%.4f", (len + 0.0) / (timestamps.length));
        System.out.println("Original data size: " + timestamps.length * 8 + "bytes, compressed data size: " +
                len * 8 + "bytes, compression ratio: " + compressedRatio);

        // long = 8 byte
        ByteBuffer byteBuffer = ByteBuffer.allocate(len << 3);
        for (long compressedValue : compressedValues) {
            byteBuffer.putLong(compressedValue);
        }
        // here we need to flip
        byteBuffer.flip();
        byteBuffer.rewind();
        //ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);

        startTime = System.nanoTime();
        long[] decodedValues = DeltaDecompressor.decompress(byteBuffer, timestamps.length);
        endTime = System.nanoTime();
        System.out.println("decompression took " + (endTime - startTime) + " ns");

        for (int i = 0; i < timestamps.length; i++) {
            if (decodedValues[i] != timestamps[i]) {
                System.out.println("Timestamp did not match, decoded value: " + decodedValues[i] + ", original timestamp: " + timestamps[i]);
            }
        }
    }

    @org.junit.jupiter.api.Test
    public void demoTest1(){
        // in order insertion test...
        System.out.println("\ntest in-order insertion...");
        long[] inOrderedTimestamps = {
                1500405481623L, 1500405488693L, 1500405495993L, 1500405503743L, 1500405511623L,
                1500405519803L, 1500405528313L, 1500405537233L, 1500405546453L, 1500405556103L,
                1500405566143L, 1500405576163L, 1500405586173L, 1500405596183L, 1500405606213L,
                1500405616163L, 1500405625813L, 1500405635763L, 1500405645634L, 1500405655633L,
                1500405665623L, 1500405675623L, 1500405685723L, 1500405695663L, 1500405705743L,
                1500405715813L, 1500405725773L, 1500405735883L, 1500405745903L, 1500405755863L,
                1500405765843L, 1500405775773L, 1500405785883L, 1500405795843L, 1500405805733L,
                1500405815853L, 1500405825963L, 1500405836004L, 1500405845953L, 1500405855913L,
                1500405865963L, 1500405875953L, 1500405885943L, 1500405896033L, 1500405906094L,
                1500405916063L, 1500405926143L, 1500405936123L, 1500405946033L, 1500405956023L,
                1500405966023L, 1500405976043L, 1500405986063L, 1500405996123L, 1500406006123L,
                1500406016174L, 1500406026173L, 1500406036004L, 1500406045964L, 1500406056043L,
                1500406066123L, 1500406076113L, 1500406086133L, 1500406096123L, 1500406106193L,
                1500406116213L, 1500406126073L, 1500406136103L, 1500406146183L, 1500406156383L,
                1500406166313L, 1500406176414L, 1500406186613L, 1500406196543L, 1500406206483L,
                1500406216483L, 1500406226433L, 1500406236403L, 1500406246413L, 1500406256494L,
                1500406266413L, 1500406276303L, 1500406286213L, 1500406296183L, 1500406306103L,
                1500406316073L, 1500406326143L, 1500406336153L, 1500406346113L, 1500406356133L,
                1500406366065L, 1500406376074L, 1500406386184L, 1500406396113L, 1500406406094L,
                1500406416203L, 1500406426323L, 1500406436343L, 1500406446323L, 1500406456344L,
                1500406466393L, 1500406476493L, 1500406486524L, 1500406496453L, 1500406506453L,
                1500406516433L, 1500406526433L, 1500406536413L, 1500406546383L, 1500406556473L,
                1500406566413L, 1500406576513L, 1500406586523L, 1500406596553L, 1500406606603L,
                1500406616623L, 1500406626623L, 1500406636723L, 1500406646723L, 1500406656723L,
                1500406666783L, 1500406676685L, 1500406686713L, 1500406696673L, 1500406706743L,
                1500406716724L, 1500406726753L, 1500406736813L, 1500406746803L, 1500406756833L,
                1500406766924L, 1500406777113L, 1500406787113L, 1500406797093L, 1500406807113L,
                1500406817283L, 1500406827284L, 1500406837323L, 1500406847463L, 1500406857513L,
                1500406867523L, 1500406877523L, 1500406887623L, 1500406897703L, 1500406907663L,
                1500406917603L, 1500406927633L, 1500406937623L, 1500406947693L, 1500406957703L,
                1500406967673L, 1500406977723L, 1500406987663L, 1500406997573L, 1500407007494L,
                1500407017493L, 1500407027503L, 1500407037523L, 1500407047603L, 1500407057473L,
                1500407067553L, 1500407077463L, 1500407087463L, 1500407097443L, 1500407107473L,
                1500407117453L, 1500407127413L, 1500407137363L, 1500407147343L, 1500407157363L,
                1500407167403L, 1500407177473L, 1500407187523L, 1500407197495L, 1500407207453L,
                1500407217413L, 1500407227443L, 1500407237463L, 1500407247523L, 1500407257513L,
                1500407267584L, 1500407277573L, 1500407287723L, 1500407297723L, 1500407307673L,
                1500407317613L, 1500407327553L, 1500407337503L, 1500407347423L, 1500407357383L,
                1500407367333L, 1500407377373L, 1500407387443L, 1500407397453L, 1500407407543L,
                1500407417583L, 1500407427453L, 1500407437433L, 1500407447603L, 1500407457513L,
                1500407467564L, 1500407477563L, 1500407487593L, 1500407497584L, 1500407507623L,
                1500407517613L, 1500407527673L, 1500407537963L, 1500407548023L, 1500407558033L,
                1500407568113L, 1500407578164L, 1500407588213L, 1500407598163L, 1500407608163L,
                1500407618223L, 1500407628143L, 1500407638223L, 1500407648173L, 1500407658023L,
                1500407667903L, 1500407677903L, 1500407687923L, 1500407697913L, 1500407708003L,
                1500407718083L, 1500407728034L, 1500407738083L, 1500407748034L, 1500407758003L,
                1500407768033L, 1500407778083L, 1500407788043L, 1500407798023L, 1500407808033L,
                1500407817983L, 1500407828063L, 1500407838213L, 1500407848203L, 1500407858253L,
                1500407868073L, 1500407878053L, 1500407888073L, 1500407898033L, 1500407908113L,
                1500407918213L, 1500407928234L, 1500407938253L, 1500407948293L, 1500407958234L,
                1500407968073L, 1500407978023L, 1500407987923L, 1500407997833L};

        // verifyDelta2Correctness(inOrderedTimestamps);
        verifyDeltaBatchCorrectness(inOrderedTimestamps);
        verifyDeltaSimple8BBatchCorrectness(inOrderedTimestamps);
        verifyDeltaVarIntBatchCorrectness(inOrderedTimestamps);
        verifyDelta2BatchCorrectness(inOrderedTimestamps);
    }

    @org.junit.jupiter.api.Test
    public void demoTest2(){
        // out-of-order insertion test...
        System.out.println("\ntest out-of-order insertion...");
        Random random = new Random(1);
        long startTimestamp = 1500405481623L;
        long[] outOfOrderTimestamp = new long[1000];
        outOfOrderTimestamp[0] = startTimestamp;
        for(int i = 1; i < 1000; ++i){
            int nextInt = random.nextInt(-10, 100);
            startTimestamp += nextInt;
            outOfOrderTimestamp[i] = startTimestamp;
        }
        verifyDeltaBatchCorrectness(outOfOrderTimestamp);
        verifyDeltaSimple8BBatchCorrectness(outOfOrderTimestamp);
        verifyDeltaVarIntBatchCorrectness(outOfOrderTimestamp);
        verifyDelta2BatchCorrectness(outOfOrderTimestamp);
    }

    @org.junit.jupiter.api.Test
    public void pressureTest(){
        System.out.println("\ntest large dataset insertion...");
        Random random = new Random(1);
        long startTimestamp = 1500405481623L;

        int testSize = 60_000;
        long[] largeDatasetTimestamps = new long[testSize];
        largeDatasetTimestamps[0] = startTimestamp;
        for(int i = 1; i < testSize; ++i){
            int nextInt = random.nextInt(-100, 1000);
            startTimestamp += nextInt;
            largeDatasetTimestamps[i] = startTimestamp;
        }

        for(int i = 0; i < 10; i++){
            verifyDeltaBatchCorrectness(largeDatasetTimestamps);
            verifyDeltaSimple8BBatchCorrectness(largeDatasetTimestamps);
            verifyDeltaVarIntBatchCorrectness(largeDatasetTimestamps);
            verifyDelta2BatchCorrectness(largeDatasetTimestamps);
        }
    }
}