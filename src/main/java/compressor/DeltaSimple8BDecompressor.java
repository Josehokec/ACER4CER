package compressor;

/*
 * Copyright 2017-2018 Michael Burman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// url: https://github.com/burmanm/compression-int/blob/master/src/main/java/fi/iki/yak/compression/integer/Simple8.java

import java.util.Arrays;

public class DeltaSimple8BDecompressor {

    //
    public static long[] decompress(BitInput in, int itemNum) {
        long startTimestamp = in.getLong(64);
        int compressedLen = (int) in.getLong(64);

        long[] input = new long[compressedLen];

        for(int i = 0; i < compressedLen; i++) {
            input[i] = in.getLong(64);
        }

        // zigZagDeltaOutput
        long[] output = new long[itemNum];
        decompress(input, 0, compressedLen, output, 0);
        output[0] += startTimestamp;
        for(int i = 1; i < itemNum; i++) {
            // delta + previous value
            output[i] = ZigZagCompressor.decodeZigZag32((int) output[i]) + output[i - 1];
        }
        return output;
    }

    public static void decompress(long[] input, int inputPos, int amount, long[] output, int outputPos) {

        for (int endPos = inputPos + amount; inputPos < endPos; inputPos++) {
            int selector = (int) (input[inputPos] >>> 60);

            switch (selector) {
                case 0:
                    decode0(input, inputPos, output, outputPos);
                    outputPos += 240;
                    break;
                case 1:
                    decode1(input, inputPos, output, outputPos);
                    outputPos += 120;
                    break;
                case 2:
                    decode2(input, inputPos, output, outputPos);
                    outputPos += 60;
                    break;
                case 3:
                    decode3(input, inputPos, output, outputPos);
                    outputPos += 30;
                    break;
                case 4:
                    decode4(input, inputPos, output, outputPos);
                    outputPos += 20;
                    break;
                case 5:
                    decode5(input, inputPos, output, outputPos);
                    outputPos += 15;
                    break;
                case 6:
                    decode6(input, inputPos, output, outputPos);
                    outputPos += 12;
                    break;
                case 7:
                    decode7(input, inputPos, output, outputPos);
                    outputPos += 10;
                    break;
                case 8:
                    decode8(input, inputPos, output, outputPos);
                    outputPos += 8;
                    break;
                case 9:
                    decode9(input, inputPos, output, outputPos);
                    outputPos += 7;
                    break;
                case 10:
                    decode10(input, inputPos, output, outputPos);
                    outputPos += 6;
                    break;
                case 11:
                    decode11(input, inputPos, output, outputPos);
                    outputPos += 5;
                    break;
                case 12:
                    decode12(input, inputPos, output, outputPos);
                    outputPos += 4;
                    break;
                case 13:
                    decode13(input, inputPos, output, outputPos);
                    outputPos += 3;
                    break;
                case 14:
                    decode14(input, inputPos, output, outputPos);
                    outputPos += 2;
                    break;
                case 15:
                    decode15(input, inputPos, output, outputPos);
                    outputPos += 1;
                    break;
            }
        }
    }
    // Decode functions
    private static void decode0(final long[] input, int startPos, final long[] output, int outputPos) {
        Arrays.fill(output, outputPos, outputPos+240, 0);
    }

    private static void decode1(final long[] input, int startPos, final long[] output, int outputPos) {
        Arrays.fill(output, outputPos, outputPos+120, 0);
    }

    private static void decode2(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 59) & 1;
        output[outputPos+1] = (input[startPos] >>> 58) & 1;
        output[outputPos+2] = (input[startPos] >>> 57) & 1;
        output[outputPos+3] = (input[startPos] >>> 56) & 1;
        output[outputPos+4] = (input[startPos] >>> 55) & 1;
        output[outputPos+5] = (input[startPos] >>> 54) & 1;
        output[outputPos+6] = (input[startPos] >>> 53) & 1;
        output[outputPos+7] = (input[startPos] >>> 52) & 1;
        output[outputPos+8] = (input[startPos] >>> 51) & 1;
        output[outputPos+9] = (input[startPos] >>> 50) & 1;
        output[outputPos+10] = (input[startPos] >>> 49) & 1;
        output[outputPos+11] = (input[startPos] >>> 48) & 1;
        output[outputPos+12] = (input[startPos] >>> 47) & 1;
        output[outputPos+13] = (input[startPos] >>> 46) & 1;
        output[outputPos+14] = (input[startPos] >>> 45) & 1;
        output[outputPos+15] = (input[startPos] >>> 44) & 1;
        output[outputPos+16] = (input[startPos] >>> 43) & 1;
        output[outputPos+17] = (input[startPos] >>> 42) & 1;
        output[outputPos+18] = (input[startPos] >>> 41) & 1;
        output[outputPos+19] = (input[startPos] >>> 40) & 1;
        output[outputPos+20] = (input[startPos] >>> 39) & 1;
        output[outputPos+21] = (input[startPos] >>> 38) & 1;
        output[outputPos+22] = (input[startPos] >>> 37) & 1;
        output[outputPos+23] = (input[startPos] >>> 36) & 1;
        output[outputPos+24] = (input[startPos] >>> 35) & 1;
        output[outputPos+25] = (input[startPos] >>> 34) & 1;
        output[outputPos+26] = (input[startPos] >>> 33) & 1;
        output[outputPos+27] = (input[startPos] >>> 32) & 1;
        output[outputPos+28] = (input[startPos] >>> 31) & 1;
        output[outputPos+29] = (input[startPos] >>> 30) & 1;
        output[outputPos+30] = (input[startPos] >>> 29) & 1;
        output[outputPos+31] = (input[startPos] >>> 28) & 1;
        output[outputPos+32] = (input[startPos] >>> 27) & 1;
        output[outputPos+33] = (input[startPos] >>> 26) & 1;
        output[outputPos+34] = (input[startPos] >>> 25) & 1;
        output[outputPos+35] = (input[startPos] >>> 24) & 1;
        output[outputPos+36] = (input[startPos] >>> 23) & 1;
        output[outputPos+37] = (input[startPos] >>> 22) & 1;
        output[outputPos+38] = (input[startPos] >>> 21) & 1;
        output[outputPos+39] = (input[startPos] >>> 20) & 1;
        output[outputPos+40] = (input[startPos] >>> 19) & 1;
        output[outputPos+41] = (input[startPos] >>> 18) & 1;
        output[outputPos+42] = (input[startPos] >>> 17) & 1;
        output[outputPos+43] = (input[startPos] >>> 16) & 1;
        output[outputPos+44] = (input[startPos] >>> 15) & 1;
        output[outputPos+45] = (input[startPos] >>> 14) & 1;
        output[outputPos+46] = (input[startPos] >>> 13) & 1;
        output[outputPos+47] = (input[startPos] >>> 12) & 1;
        output[outputPos+48] = (input[startPos] >>> 11) & 1;
        output[outputPos+49] = (input[startPos] >>> 10) & 1;
        output[outputPos+50] = (input[startPos] >>> 9) & 1;
        output[outputPos+51] = (input[startPos] >>> 8) & 1;
        output[outputPos+52] = (input[startPos] >>> 7) & 1;
        output[outputPos+53] = (input[startPos] >>> 6) & 1;
        output[outputPos+54] = (input[startPos] >>> 5) & 1;
        output[outputPos+55] = (input[startPos] >>> 4) & 1;
        output[outputPos+56] = (input[startPos] >>> 3) & 1;
        output[outputPos+57] = (input[startPos] >>> 2) & 1;
        output[outputPos+58] = (input[startPos] >>> 1) & 1;
        output[outputPos+59] = input[startPos] & 1;
    }

    private static void decode3(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 58) & 3;
        output[outputPos+1] = (input[startPos] >>> 56) & 3;
        output[outputPos+2] = (input[startPos] >>> 54) & 3;
        output[outputPos+3] = (input[startPos] >>> 52) & 3;
        output[outputPos+4] = (input[startPos] >>> 50) & 3;
        output[outputPos+5] = (input[startPos] >>> 48) & 3;
        output[outputPos+6] = (input[startPos] >>> 46) & 3;
        output[outputPos+7] = (input[startPos] >>> 44) & 3;
        output[outputPos+8] = (input[startPos] >>> 42) & 3;
        output[outputPos+9] = (input[startPos] >>> 40) & 3;
        output[outputPos+10] = (input[startPos] >>> 38) & 3;
        output[outputPos+11] = (input[startPos] >>> 36) & 3;
        output[outputPos+12] = (input[startPos] >>> 34) & 3;
        output[outputPos+13] = (input[startPos] >>> 32) & 3;
        output[outputPos+14] = (input[startPos] >>> 30) & 3;
        output[outputPos+15] = (input[startPos] >>> 28) & 3;
        output[outputPos+16] = (input[startPos] >>> 26) & 3;
        output[outputPos+17] = (input[startPos] >>> 24) & 3;
        output[outputPos+18] = (input[startPos] >>> 22) & 3;
        output[outputPos+19] = (input[startPos] >>> 20) & 3;
        output[outputPos+20] = (input[startPos] >>> 18) & 3;
        output[outputPos+21] = (input[startPos] >>> 16) & 3;
        output[outputPos+22] = (input[startPos] >>> 14) & 3;
        output[outputPos+23] = (input[startPos] >>> 12) & 3;
        output[outputPos+24] = (input[startPos] >>> 10) & 3;
        output[outputPos+25] = (input[startPos] >>> 8) & 3;
        output[outputPos+26] = (input[startPos] >>> 6) & 3;
        output[outputPos+27] = (input[startPos] >>> 4) & 3;
        output[outputPos+28] = (input[startPos] >>> 2) & 3;
        output[outputPos+29] = input[startPos] & 3;
    }

    private static void decode4(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 57) & 7;
        output[outputPos+1] = (input[startPos] >>> 54) & 7;
        output[outputPos+2] = (input[startPos] >>> 51) & 7;
        output[outputPos+3] = (input[startPos] >>> 48) & 7;
        output[outputPos+4] = (input[startPos] >>> 45) & 7;
        output[outputPos+5] = (input[startPos] >>> 42) & 7;
        output[outputPos+6] = (input[startPos] >>> 39) & 7;
        output[outputPos+7] = (input[startPos] >>> 36) & 7;
        output[outputPos+8] = (input[startPos] >>> 33) & 7;
        output[outputPos+9] = (input[startPos] >>> 30) & 7;
        output[outputPos+10] = (input[startPos] >>> 27) & 7;
        output[outputPos+11] = (input[startPos] >>> 24) & 7;
        output[outputPos+12] = (input[startPos] >>> 21) & 7;
        output[outputPos+13] = (input[startPos] >>> 18) & 7;
        output[outputPos+14] = (input[startPos] >>> 15) & 7;
        output[outputPos+15] = (input[startPos] >>> 12) & 7;
        output[outputPos+16] = (input[startPos] >>> 9) & 7;
        output[outputPos+17] = (input[startPos] >>> 6) & 7;
        output[outputPos+18] = (input[startPos] >>> 3) & 7;
        output[outputPos+19] = input[startPos] & 7;
    }

    private static void decode5(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 56) & 15;
        output[outputPos+1] = (input[startPos] >>> 52) & 15;
        output[outputPos+2] = (input[startPos] >>> 48) & 15;
        output[outputPos+3] = (input[startPos] >>> 44) & 15;
        output[outputPos+4] = (input[startPos] >>> 40) & 15;
        output[outputPos+5] = (input[startPos] >>> 36) & 15;
        output[outputPos+6] = (input[startPos] >>> 32) & 15;
        output[outputPos+7] = (input[startPos] >>> 28) & 15;
        output[outputPos+8] = (input[startPos] >>> 24) & 15;
        output[outputPos+9] = (input[startPos] >>> 20) & 15;
        output[outputPos+10] = (input[startPos] >>> 16) & 15;
        output[outputPos+11] = (input[startPos] >>> 12) & 15;
        output[outputPos+12] = (input[startPos] >>> 8) & 15;
        output[outputPos+13] = (input[startPos] >>> 4) & 15;
        output[outputPos+14] = input[startPos] & 15;
    }

    private static void decode6(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 55) & 31;
        output[outputPos+1] = (input[startPos] >>> 50) & 31;
        output[outputPos+2] = (input[startPos] >>> 45) & 31;
        output[outputPos+3] = (input[startPos] >>> 40) & 31;
        output[outputPos+4] = (input[startPos] >>> 35) & 31;
        output[outputPos+5] = (input[startPos] >>> 30) & 31;
        output[outputPos+6] = (input[startPos] >>> 25) & 31;
        output[outputPos+7] = (input[startPos] >>> 20) & 31;
        output[outputPos+8] = (input[startPos] >>> 15) & 31;
        output[outputPos+9] = (input[startPos] >>> 10) & 31;
        output[outputPos+10] = (input[startPos] >>> 5) & 31;
        output[outputPos+11] = input[startPos] & 31;
    }

    private static void decode7(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 54) & 63;
        output[outputPos+1] = (input[startPos] >>> 48) & 63;
        output[outputPos+2] = (input[startPos] >>> 42) & 63;
        output[outputPos+3] = (input[startPos] >>> 36) & 63;
        output[outputPos+4] = (input[startPos] >>> 30) & 63;
        output[outputPos+5] = (input[startPos] >>> 24) & 63;
        output[outputPos+6] = (input[startPos] >>> 18) & 63;
        output[outputPos+7] = (input[startPos] >>> 12) & 63;
        output[outputPos+8] = (input[startPos] >>> 6) & 63;
        output[outputPos+9] = input[startPos] & 63;
    }

    private static void decode8(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 49) & 127;
        output[outputPos+1] = (input[startPos] >>> 42) & 127;
        output[outputPos+2] = (input[startPos] >>> 35) & 127;
        output[outputPos+3] = (input[startPos] >>> 28) & 127;
        output[outputPos+4] = (input[startPos] >>> 21) & 127;
        output[outputPos+5] = (input[startPos] >>> 14) & 127;
        output[outputPos+6] = (input[startPos] >>> 7) & 127;
        output[outputPos+7] = input[startPos] & 127;
    }

    private static void decode9(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 48) & 255;
        output[outputPos+1] = (input[startPos] >>> 40) & 255;
        output[outputPos+2] = (input[startPos] >>> 32) & 255;
        output[outputPos+3] = (input[startPos] >>> 24) & 255;
        output[outputPos+4] = (input[startPos] >>> 16) & 255;
        output[outputPos+5] = (input[startPos] >>> 8) & 255;
        output[outputPos+6] = input[startPos] & 255;
    }

    private static void decode10(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 50) & 1023;
        output[outputPos+1] = (input[startPos] >>> 40) & 1023;
        output[outputPos+2] = (input[startPos] >>> 30) & 1023;
        output[outputPos+3] = (input[startPos] >>> 20) & 1023;
        output[outputPos+4] = (input[startPos] >>> 10) & 1023;
        output[outputPos+5] = input[startPos] & 1023;
    }

    private static void decode11(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 48) & 4095;
        output[outputPos+1] = (input[startPos] >>> 36) & 4095;
        output[outputPos+2] = (input[startPos] >>> 24) & 4095;
        output[outputPos+3] = (input[startPos] >>> 12) & 4095;
        output[outputPos+4] = input[startPos] & 4095;
    }

    private static void decode12(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 45) & 32767;
        output[outputPos+1] = (input[startPos] >>> 30) & 32767;
        output[outputPos+2] = (input[startPos] >>> 15) & 32767;
        output[outputPos+3] = input[startPos] & 32767;
    }

    private static void decode13(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 40) & 1048575;
        output[outputPos+1] = (input[startPos] >>> 20) & 1048575;
        output[outputPos+2] = input[startPos] & 1048575;
    }

    private static void decode14(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = (input[startPos] >>> 30) & 1073741823;
        output[outputPos+1] = input[startPos] & 1073741823;
    }

    private static void decode15(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = input[startPos] & 1152921504606846975L;
    }
}
