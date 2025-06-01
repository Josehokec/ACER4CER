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
import java.util.List;

/*
https://github.com/influxdata/influxdb/blob/v1.11.6/pkg/encoding/simple8b/encoding.go
┌──────────────┬─────────────────────────────────────────────────────────────┐
│   Selector   │       0    1   2   3   4   5   6   7  8  9 10 11 12 13 14 15│
├──────────────┼─────────────────────────────────────────────────────────────┤
│     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
├──────────────┼─────────────────────────────────────────────────────────────┤
│      N       │     240  120  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
├──────────────┼─────────────────────────────────────────────────────────────┤
│   Wasted Bits│      60   60   0   0   0   0   0   0  4  4  0  0  0  0  0  0│
└──────────────┴─────────────────────────────────────────────────────────────┘
*/
public class DeltaSimple8BCompressor {

    private static int[] AVAILABLE_BITS = {
            120, 60, 60, 60, 60, 60, 60, 56, 56, 54, 60, 55, 60, 52, 56, 60, 48, 51, 54, 57,
            60, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 31, 32, 33, 34, 35, 36, 37, 38, 39,
            40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            60, -1, -1, -1, -1};

    private static int[] BITS_TO_COUNT = {
            120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 6, 5, 5, 4, 4, 4, 3, 3, 3, 3,
            3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, -1, -1, -1, -1};

    public static int bits(long value) {
        return Long.SIZE - Long.numberOfLeadingZeros(value);
    }

    public static long[] compress(List<Long> valueList){
        int len = valueList.size();
        // here may encounter out of array index
        long[] output = new long[len + 2];
        long[] zigzagDeltaInput = new long[len];
        // write start timestamp
        output[0] = valueList.get(0);
        // initial zigzagDelta array
        zigzagDeltaInput[0] = 0;
        for (int i = 1; i < len; i++) {
            if(valueList.get(i) - valueList.get(i - 1) > Integer.MAX_VALUE){
                throw new ArithmeticException("greater than Integer.MAX_VALUE");
            }
            zigzagDeltaInput[i] = ZigZagCompressor.encodeZigZag32((int) (valueList.get(i) - valueList.get(i - 1)));
        }
        int compressedLen = compress(zigzagDeltaInput, 0, len, output, 2);
        output[1] = compressedLen;

        return Arrays.copyOf(output, compressedLen + 2);
    }

    /**
     * Compress the input array to output array
     * @param input  Values to be compressed
     * @param output Array to store the results
     * @return Amount of words written
     */
    public static int compress(long[] input, long[] output) {
        output[0] = input[0];
        long[] zigzagDeltaInput = new long[input.length];
        int len = input.length;
        for (int i = 0; i < len; i++) {
            zigzagDeltaInput[i] = ZigZagCompressor.encodeZigZag32((int) (input[i] - output[i]));
        }
        return 1 + compress(zigzagDeltaInput, 0, len, output, 1);
    }

    /**
     * Compress the given array to a destination array
     * @param input     Values to be compressed
     * @param inputPos  Starting position of the input to compress
     * @param amount    How many values to compress from input
     * @param output    Output array to store the values
     * @param outputPos Starting position of the output where to store the data
     * @return Amount of words written
     */
    public static int compress(long[] input, int inputPos, int amount, long[] output, int outputPos) {
        int startOutputPos = outputPos;
        for (int endPos = inputPos + amount; inputPos < endPos;) {
            int integersToCompress = 0; // How many integers to compress to next word
            int maxBitsRequired, nextBitsRequired, toCompressBits; // How many bits per integer will be required

            // Find the maximum from following values
            for (toCompressBits = nextBitsRequired = bits(input[inputPos]);
                 ((integersToCompress + 1) * (maxBitsRequired = Math.max(nextBitsRequired, toCompressBits))
                         <= AVAILABLE_BITS[maxBitsRequired]);
            ) {
                toCompressBits = maxBitsRequired;
                if(inputPos + integersToCompress + 1 < endPos) {
                    nextBitsRequired = bits(input[inputPos + ++integersToCompress]);
                } else {
                    ++integersToCompress;
                    break;
                }
            }

            // We don't have enough integers to fill the whole array with current bit length, so forward to nearest
            // optimal bit length
            while (integersToCompress < BITS_TO_COUNT[toCompressBits]) {
                toCompressBits++;
            }

            switch (toCompressBits) {
                case 0:
                    if(integersToCompress == 240) {
                        encode0(input, inputPos, output, outputPos);
                        inputPos += 240;
                    } else {
                        encode1(input, inputPos, output, outputPos);
                        inputPos += 120;
                    }
                    break;
                case 1:
                    encode2(input, inputPos, output, outputPos);
                    inputPos += 60;
                    break;
                case 2:
                    encode3(input, inputPos, output, outputPos);
                    inputPos += 30;
                    break;
                case 3:
                    encode4(input, inputPos, output, outputPos);
                    inputPos += 20;
                    break;
                case 4:
                    encode5(input, inputPos, output, outputPos);
                    inputPos += 15;
                    break;
                case 5:
                    encode6(input, inputPos, output, outputPos);
                    inputPos += 12;
                    break;
                case 6:
                    encode7(input, inputPos, output, outputPos);
                    inputPos += 10;
                    break;
                case 7:
                    encode8(input, inputPos, output, outputPos);
                    inputPos += 8;
                    break;
                case 8:
                    encode9(input, inputPos, output, outputPos);
                    inputPos += 7;
                    break;
                case 9:
                case 10:
                    encode10(input, inputPos, output, outputPos);
                    inputPos += 6;
                    break;
                case 11:
                case 12:
                    encode11(input, inputPos, output, outputPos);
                    inputPos += 5;
                    break;
                case 13:
                case 14:
                case 15:
                    encode12(input, inputPos, output, outputPos);
                    inputPos += 4;
                    break;
                case 16:
                case 17:
                case 18:
                case 19:
                case 20:
                    encode13(input, inputPos, output, outputPos);
                    inputPos += 3;
                    break;
                case 21:
                case 22:
                case 23:
                case 24:
                case 25:
                case 26:
                case 27:
                case 28:
                case 29:
                case 30:
                    encode14(input, inputPos, output, outputPos);
                    inputPos += 2;
                    break;
                case 31:
                case 32:
                case 33:
                case 34:
                case 35:
                case 36:
                case 37:
                case 38:
                case 39:
                case 40:
                case 41:
                case 42:
                case 43:
                case 44:
                case 45:
                case 46:
                case 47:
                case 48:
                case 49:
                case 50:
                case 51:
                case 52:
                case 53:
                case 54:
                case 55:
                case 56:
                case 57:
                case 58:
                case 59:
                case 60:
                    encode15(input, inputPos, output, outputPos);
                    inputPos += 1;
                    break;
                default:
                    System.out.println("input: " + Arrays.toString(input));
                    throw new RuntimeException("This compressor does not support values larger than 2^60");
            }

            outputPos++;
        }
        return outputPos - startOutputPos;
    }

    // Encode functions - without mask as we already check the length of leadingZeros
    private static void encode0(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] = 0;
    }

    private static void encode1(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 1L << 60;
    }

    private static void encode2(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 2L << 60;
        output[outputPos] |= (input[startPos]) << 59;
        output[outputPos] |= (input[startPos + 1]) << 58;
        output[outputPos] |= (input[startPos + 2]) << 57;
        output[outputPos] |= (input[startPos + 3]) << 56;
        output[outputPos] |= (input[startPos + 4]) << 55;
        output[outputPos] |= (input[startPos + 5]) << 54;
        output[outputPos] |= (input[startPos + 6]) << 53;
        output[outputPos] |= (input[startPos + 7]) << 52;
        output[outputPos] |= (input[startPos + 8]) << 51;
        output[outputPos] |= (input[startPos + 9]) << 50;
        output[outputPos] |= (input[startPos + 10]) << 49;
        output[outputPos] |= (input[startPos + 11]) << 48;
        output[outputPos] |= (input[startPos + 12]) << 47;
        output[outputPos] |= (input[startPos + 13]) << 46;
        output[outputPos] |= (input[startPos + 14]) << 45;
        output[outputPos] |= (input[startPos + 15]) << 44;
        output[outputPos] |= (input[startPos + 16]) << 43;
        output[outputPos] |= (input[startPos + 17]) << 42;
        output[outputPos] |= (input[startPos + 18]) << 41;
        output[outputPos] |= (input[startPos + 19]) << 40;
        output[outputPos] |= (input[startPos + 20]) << 39;
        output[outputPos] |= (input[startPos + 21]) << 38;
        output[outputPos] |= (input[startPos + 22]) << 37;
        output[outputPos] |= (input[startPos + 23]) << 36;
        output[outputPos] |= (input[startPos + 24]) << 35;
        output[outputPos] |= (input[startPos + 25]) << 34;
        output[outputPos] |= (input[startPos + 26]) << 33;
        output[outputPos] |= (input[startPos + 27]) << 32;
        output[outputPos] |= (input[startPos + 28]) << 31;
        output[outputPos] |= (input[startPos + 29]) << 30;
        output[outputPos] |= (input[startPos + 30]) << 29;
        output[outputPos] |= (input[startPos + 31]) << 28;
        output[outputPos] |= (input[startPos + 32]) << 27;
        output[outputPos] |= (input[startPos + 33]) << 26;
        output[outputPos] |= (input[startPos + 34]) << 25;
        output[outputPos] |= (input[startPos + 35]) << 24;
        output[outputPos] |= (input[startPos + 36]) << 23;
        output[outputPos] |= (input[startPos + 37]) << 22;
        output[outputPos] |= (input[startPos + 38]) << 21;
        output[outputPos] |= (input[startPos + 39]) << 20;
        output[outputPos] |= (input[startPos + 40]) << 19;
        output[outputPos] |= (input[startPos + 41]) << 18;
        output[outputPos] |= (input[startPos + 42]) << 17;
        output[outputPos] |= (input[startPos + 43]) << 16;
        output[outputPos] |= (input[startPos + 44]) << 15;
        output[outputPos] |= (input[startPos + 45]) << 14;
        output[outputPos] |= (input[startPos + 46]) << 13;
        output[outputPos] |= (input[startPos + 47]) << 12;
        output[outputPos] |= (input[startPos + 48]) << 11;
        output[outputPos] |= (input[startPos + 49]) << 10;
        output[outputPos] |= (input[startPos + 50]) << 9;
        output[outputPos] |= (input[startPos + 51]) << 8;
        output[outputPos] |= (input[startPos + 52]) << 7;
        output[outputPos] |= (input[startPos + 53]) << 6;
        output[outputPos] |= (input[startPos + 54]) << 5;
        output[outputPos] |= (input[startPos + 55]) << 4;
        output[outputPos] |= (input[startPos + 56]) << 3;
        output[outputPos] |= (input[startPos + 57]) << 2;
        output[outputPos] |= (input[startPos + 58]) << 1;
        output[outputPos] |= (input[startPos + 59]);
    }

    private static void encode3(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 3L << 60;
        output[outputPos] |= (input[startPos]) << 58;
        output[outputPos] |= (input[startPos + 1]) << 56;
        output[outputPos] |= (input[startPos + 2]) << 54;
        output[outputPos] |= (input[startPos + 3]) << 52;
        output[outputPos] |= (input[startPos + 4]) << 50;
        output[outputPos] |= (input[startPos + 5]) << 48;
        output[outputPos] |= (input[startPos + 6]) << 46;
        output[outputPos] |= (input[startPos + 7]) << 44;
        output[outputPos] |= (input[startPos + 8]) << 42;
        output[outputPos] |= (input[startPos + 9]) << 40;
        output[outputPos] |= (input[startPos + 10]) << 38;
        output[outputPos] |= (input[startPos + 11]) << 36;
        output[outputPos] |= (input[startPos + 12]) << 34;
        output[outputPos] |= (input[startPos + 13]) << 32;
        output[outputPos] |= (input[startPos + 14]) << 30;
        output[outputPos] |= (input[startPos + 15]) << 28;
        output[outputPos] |= (input[startPos + 16]) << 26;
        output[outputPos] |= (input[startPos + 17]) << 24;
        output[outputPos] |= (input[startPos + 18]) << 22;
        output[outputPos] |= (input[startPos + 19]) << 20;
        output[outputPos] |= (input[startPos + 20]) << 18;
        output[outputPos] |= (input[startPos + 21]) << 16;
        output[outputPos] |= (input[startPos + 22]) << 14;
        output[outputPos] |= (input[startPos + 23]) << 12;
        output[outputPos] |= (input[startPos + 24]) << 10;
        output[outputPos] |= (input[startPos + 25]) << 8;
        output[outputPos] |= (input[startPos + 26]) << 6;
        output[outputPos] |= (input[startPos + 27]) << 4;
        output[outputPos] |= (input[startPos + 28]) << 2;
        output[outputPos] |= (input[startPos + 29]);
    }

    private static void encode4(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 4L << 60;
        output[outputPos] |= (input[startPos]) << 57;
        output[outputPos] |= (input[startPos + 1]) << 54;
        output[outputPos] |= (input[startPos + 2]) << 51;
        output[outputPos] |= (input[startPos + 3]) << 48;
        output[outputPos] |= (input[startPos + 4]) << 45;
        output[outputPos] |= (input[startPos + 5]) << 42;
        output[outputPos] |= (input[startPos + 6]) << 39;
        output[outputPos] |= (input[startPos + 7]) << 36;
        output[outputPos] |= (input[startPos + 8]) << 33;
        output[outputPos] |= (input[startPos + 9]) << 30;
        output[outputPos] |= (input[startPos + 10]) << 27;
        output[outputPos] |= (input[startPos + 11]) << 24;
        output[outputPos] |= (input[startPos + 12]) << 21;
        output[outputPos] |= (input[startPos + 13]) << 18;
        output[outputPos] |= (input[startPos + 14]) << 15;
        output[outputPos] |= (input[startPos + 15]) << 12;
        output[outputPos] |= (input[startPos + 16]) << 9;
        output[outputPos] |= (input[startPos + 17]) << 6;
        output[outputPos] |= (input[startPos + 18]) << 3;
        output[outputPos] |= (input[startPos + 19]);
    }

    private static void encode5(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 5L << 60;
        output[outputPos] |= (input[startPos]) << 56;
        output[outputPos] |= (input[startPos + 1]) << 52;
        output[outputPos] |= (input[startPos + 2]) << 48;
        output[outputPos] |= (input[startPos + 3]) << 44;
        output[outputPos] |= (input[startPos + 4]) << 40;
        output[outputPos] |= (input[startPos + 5]) << 36;
        output[outputPos] |= (input[startPos + 6]) << 32;
        output[outputPos] |= (input[startPos + 7]) << 28;
        output[outputPos] |= (input[startPos + 8]) << 24;
        output[outputPos] |= (input[startPos + 9]) << 20;
        output[outputPos] |= (input[startPos + 10]) << 16;
        output[outputPos] |= (input[startPos + 11]) << 12;
        output[outputPos] |= (input[startPos + 12]) << 8;
        output[outputPos] |= (input[startPos + 13]) << 4;
        output[outputPos] |= (input[startPos + 14]);
    }

    private static void encode6(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 6L << 60;
        output[outputPos] |= (input[startPos]) << 55;
        output[outputPos] |= (input[startPos + 1]) << 50;
        output[outputPos] |= (input[startPos + 2]) << 45;
        output[outputPos] |= (input[startPos + 3]) << 40;
        output[outputPos] |= (input[startPos + 4]) << 35;
        output[outputPos] |= (input[startPos + 5]) << 30;
        output[outputPos] |= (input[startPos + 6]) << 25;
        output[outputPos] |= (input[startPos + 7]) << 20;
        output[outputPos] |= (input[startPos + 8]) << 15;
        output[outputPos] |= (input[startPos + 9]) << 10;
        output[outputPos] |= (input[startPos + 10]) << 5;
        output[outputPos] |= (input[startPos + 11]);
    }

    private static void encode7(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 7L << 60;
        output[outputPos] |= (input[startPos]) << 54;
        output[outputPos] |= (input[startPos + 1]) << 48;
        output[outputPos] |= (input[startPos + 2]) << 42;
        output[outputPos] |= (input[startPos + 3]) << 36;
        output[outputPos] |= (input[startPos + 4]) << 30;
        output[outputPos] |= (input[startPos + 5]) << 24;
        output[outputPos] |= (input[startPos + 6]) << 18;
        output[outputPos] |= (input[startPos + 7]) << 12;
        output[outputPos] |= (input[startPos + 8]) << 6;
        output[outputPos] |= (input[startPos + 9]);
    }

    private static void encode8(long[] input, int startPos, long[] output, int outputPos) {
        output[outputPos] |= 8L << 60;
        output[outputPos] |= (input[startPos]) << 49;
        output[outputPos] |= (input[startPos + 1]) << 42;
        output[outputPos] |= (input[startPos + 2]) << 35;
        output[outputPos] |= (input[startPos + 3]) << 28;
        output[outputPos] |= (input[startPos + 4]) << 21;
        output[outputPos] |= (input[startPos + 5]) << 14;
        output[outputPos] |= (input[startPos + 6]) << 7;
        output[outputPos] |= (input[startPos + 7]);
    }

    private static void encode9(long[] input, int startPos, long[] output, int outputPos) {
        output[outputPos] |= 9L << 60;
        output[outputPos] |= (input[startPos]) << 48;
        output[outputPos] |= (input[startPos + 1]) << 40;
        output[outputPos] |= (input[startPos + 2]) << 32;
        output[outputPos] |= (input[startPos + 3]) << 24;
        output[outputPos] |= (input[startPos + 4]) << 16;
        output[outputPos] |= (input[startPos + 5]) << 8;
        output[outputPos] |= (input[startPos + 6]);
    }

    private static void encode10(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 10L << 60;
        output[outputPos] |= (input[startPos]) << 50;
        output[outputPos] |= (input[startPos + 1]) << 40;
        output[outputPos] |= (input[startPos + 2]) << 30;
        output[outputPos] |= (input[startPos + 3]) << 20;
        output[outputPos] |= (input[startPos + 4]) << 10;
        output[outputPos] |= (input[startPos + 5]);
    }

    private static void encode11(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 11L << 60;
        output[outputPos] |= (input[startPos]) << 48;
        output[outputPos] |= (input[startPos + 1]) << 36;
        output[outputPos] |= (input[startPos + 2]) << 24;
        output[outputPos] |= (input[startPos + 3]) << 12;
        output[outputPos] |= (input[startPos + 4]);
    }

    private static void encode12(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 12L << 60;
        output[outputPos] |= (input[startPos]) << 45;
        output[outputPos] |= (input[startPos + 1]) << 30;
        output[outputPos] |= (input[startPos + 2]) << 15;
        output[outputPos] |= (input[startPos + 3]);
    }

    private static void encode13(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 13L << 60;
        output[outputPos] |= (input[startPos]) << 40;
        output[outputPos] |= (input[startPos + 1]) << 20;
        output[outputPos] |= (input[startPos + 2]);
    }

    private static void encode14(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 14L << 60;
        output[outputPos] |= (input[startPos]) << 30;
        output[outputPos] |= (input[startPos + 1]);
    }

    private static void encode15(final long[] input, int startPos, final long[] output, int outputPos) {
        output[outputPos] |= 15L << 60;
        output[outputPos] |= (input[startPos]);
    }
}
