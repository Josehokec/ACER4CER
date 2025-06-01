package acer;

/**
 * [updated] we support two layouts
 * ---------------------------------------------------------------------------------------
 * CASE 1: if OPTIMIZED_LAYOUT = true, then length of sizes is N + 2g
 * where N is the number of range bitmaps, g is the number of clusters
 * format : |rbSize_1|....|rbSize_N|tsSize_1|ridSize_1|...|tsSize_g|ridSize_g|
 * ---------------------------------------------------------------------------------------
 * CASE 2: otherwise, then length of sizes is N + 2
 * format : |rbSize_1|....|rbSize_N|tsSize_1|ridSize_1|
 * ---------------------------------------------------------------------------------------
 * @param storagePosition           file start position
 * @param blockSize                 index block size
 * @param sizes                     range bitmap size list, ts & rid list
 */
public record IdxBlkMetaInfo(long storagePosition, int blockSize, int[] sizes) {

    public void print(int indexAttrNum){
        StringBuilder str = new StringBuilder(160);
        str.append("|storageRange: [").append(storagePosition).append(",").append(storagePosition + blockSize).append("]");

        str.append("|rangeBitmapSizes: [");
        int curOffset = 0;
        for(int i = 0; i < indexAttrNum; i++){
            if(i != indexAttrNum - 1){
                str.append(sizes[i]).append(",");
            }else{
                str.append(sizes[i]).append("]");
            }
            curOffset += sizes[i];
        }

        if(Parameters.OPTIMIZED_LAYOUT){
            int g = (sizes.length - indexAttrNum) >> 1;
            str.append("|#<tsSize, ridSize> is: ").append(g).append(" values: [");
            for(int i = 0; i < g; i++){
                if(i != g - 1){
                    str.append("<").append(sizes[i * 2]).append(",").append(sizes[i * 2 + 1]).append(">").append(",");
                }else{
                    str.append("<").append(sizes[i * 2]).append(",").append(sizes[i * 2 + 1]).append(">").append("]");
                }
            }
        }else{
            if(sizes.length != indexAttrNum + 2){
                throw new RuntimeException("meta info has a wrong state.");
            }
            str.append("|tsListRange: [").append(curOffset).append(",").append(curOffset + sizes[indexAttrNum]).append("]");
            curOffset += sizes[indexAttrNum];
            str.append("|ridListRange: [").append(curOffset).append(",").append(curOffset + sizes[indexAttrNum + 1]).append("]");
        }

        System.out.println(str);
    }
}
