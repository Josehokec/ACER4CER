package common;

import store.EventStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The attNameMap is used to find where the attribute name exists
 * For example: ticker TYPE, open FLOAT. 2, volume INT, time TIMESTAMP
 * AttNameMap. get (ticker)=0; AttNameMap. get (volume)=2; AttNameMap.get (time)=3;
 * TypeMap is used to convert event types of string types into integers
 * Note that the maximum and minimum values are converted
 */
public class EventSchema {
    private int hasAssignedId;                          // event type is String, so we assign an integer id to an event type
    private int maxEventTypeBitLen;                     // number of bits required for event types
    private short storeRecordSize;                      // number of bytes required to store a record
    private String schemaName;                          // schema name
    private String[] attrNames;                         // attribute name
    private String[] attrTypes;                         // attribute type, like INT, DOUBLE, LONG, TIMESTAMP
    private int[] decimalLens;                          // decimal lengths
    private StorePos[] positions;                       // type store position
    private long[] attrMinValues;                       // minimum value for each attribute
    private long[] attrMaxValues;                       // maximum value for each attribute
    private final List<String> allEventTypes;           // typeId corresponding to an event type
    private EventStore store;                           // stored file
    private final HashMap<String, Integer> attrNameMap; // attNameMap is used to find where the attribute name exists
    private final HashMap<String, Integer> typeMap;     //event type map

    public EventSchema() {
        attrNameMap = new HashMap<>();
        typeMap = new HashMap<>();
        hasAssignedId = 0;
        maxEventTypeBitLen = -1;
        storeRecordSize = -1;
        allEventTypes = new ArrayList<>();
        // Because we assign IDs starting from 1, we add null
        allEventTypes.add("null");
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    public short getStoreRecordSize() {
        return storeRecordSize;
    }

    public EventStore getStore() {
        return store;
    }

    public void setStore(EventStore store) {
        this.store = store;
    }

    public String[] getAttrNames() {
        return attrNames;
    }

    public void setAttrNames(String[] attrNames) {
        this.attrNames = attrNames;
    }

    public String[] getAttrTypes() {
        return attrTypes;
    }

    public void setAttrTypes(String[] attrTypes) {
        this.attrTypes = attrTypes;
        positions = new StorePos[attrTypes.length];
        short startPos = 0;
        //int occupies 4bytes，Type occupies 4 bytes，timestamp occupies 8 bytes，float and double occupy 8 bytes
        for(int i = 0; i < attrTypes.length; ++i){
            if(attrTypes[i].equals("INT") || attrTypes[i].equals("TYPE")){
                positions[i] = new StorePos(startPos, 4);
                startPos += 4;
            }else if(attrTypes[i].contains("FLOAT") || attrTypes[i].contains("DOUBLE") || attrTypes[i].equals("TIMESTAMP")){
                positions[i] = new StorePos(startPos, 8);
                startPos += 8;
            }else{
                throw new RuntimeException("Do not support this type'" + attrTypes[i] + "'.");
            }
        }
        storeRecordSize = startPos;
        //System.out.println("recordSize: " + storeRecordSize);
    }

    public void setDecimalLens(int[] decimalLens) {
        this.decimalLens = decimalLens;
    }

    public void setAttrMinValues(long[] attrMinValues) {
        this.attrMinValues = attrMinValues;
    }

    public void setAttrMaxValues(long[] attrMaxValues) {
        this.attrMaxValues = attrMaxValues;
    }

    public void insertAttrName(String attrName, int idx){
        attrNameMap.put(attrName, idx);
    }

    public int getAttrNameIdx(String attrName){
        return attrNameMap.get(attrName);
    }

    public String getIthAttrType(int idx){
        return attrTypes[idx];
    }

    public long getIthAttrMinValue(int idx){
        return attrMinValues[idx];
    }

    public void setIthAttrMinValue(int idx, long minValue){
        attrMinValues[idx] = minValue;
    }

    public long getIthAttrMaxValue(int idx){
        return attrMaxValues[idx];
    }

    public void setIthAttrMaxValue(int idx, long minValue){
        attrMaxValues[idx] = minValue;
    }

    public int  getIthDecimalLens(int idx) {
        return decimalLens[idx];
    }

    /**
     * If this event type has been stored before,
     * it will be returned directly, otherwise an ID needs to be assigned
     * @param eventType event type
     * @return assigned id
     */
    public int getTypeId(String eventType){
        if(typeMap.containsKey(eventType)){
            return typeMap.get(eventType);
        }else{
            allEventTypes.add(eventType);
            typeMap.put(eventType, ++hasAssignedId);
            return hasAssignedId;
        }
    }

    public int getTypeIdLen(){
        return maxEventTypeBitLen == -1 ? calTypeIdBitLen() : maxEventTypeBitLen;
    }

    public int getPageStoreRecordNum(){
        int pageSize = store.getPageSize();
        return pageSize / storeRecordSize;
    }

    public int getMaxEventTypeNum(){
        for(int i = 0; i< attrTypes.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                return (int) attrMaxValues[i];
            }
        }
        return -1;
    }

    public int calTypeIdBitLen(){
        int ans = 0;
        long maxEventTypeNum = Integer.MAX_VALUE;
        for(int i = 0; i< attrTypes.length; ++i){
            if(attrTypes[i].equals("TYPE")){
                maxEventTypeNum = attrMaxValues[i];
                break;
            }
        }

        while(maxEventTypeNum != 0){
            maxEventTypeNum >>= 1;
            ans++;
        }
        maxEventTypeBitLen = ans;
        return ans;
    }

    public int getTimestampIdx(){
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("TIMESTAMP")){
                return i;
            }
        }
        throw new IllegalStateException("This schema is missing the timestamp attribute.");
    }


    public int getTypeIdx(){
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("TYPE")){
                return i;
            }
        }
        throw new IllegalStateException("This schema is missing the timestamp attribute.");
    }

    /**
     * convert string type records to byte type arrays
     */
    public byte[] convertToBytes(String[] attrValues){

        if(storeRecordSize == -1){
            throw new RuntimeException("wrong state");
        }
        byte[] ans = new byte[storeRecordSize];
        int ptr = 0;
        for(int i = 0; i < attrTypes.length; ++i){
            String attrType = attrTypes[i];
            if(attrType.equals("INT")){
                int value = Integer.parseInt(attrValues[i]);
                byte[] b = Converter.intToBytes(value);
                System.arraycopy(b, 0, ans, ptr, 4);
                ptr += 4;
            }else if(attrType.contains("FLOAT")){
                float value = Float.parseFloat(attrValues[i]);
                int magnification = (int) Math.pow(10, getIthDecimalLens(i));
                long newValue = (long) (value * magnification);
                byte[] b = Converter.longToBytes(newValue);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else if(attrType.contains("DOUBLE")){
                double value = Double.parseDouble(attrValues[i]);
                int magnification = (int) Math.pow(10, getIthDecimalLens(i));
                long newValue = (long) (value * magnification);
                byte[] b = Converter.longToBytes(newValue);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else if(attrType.contains("TYPE")){
                String eventType = attrValues[i];
                int type = getTypeId(eventType);
                byte[] b = Converter.intToBytes(type);
                System.arraycopy(b, 0, ans, ptr, 4);
                ptr += 4;
            }else if(attrType.equals("TIMESTAMP")){
                long timestamp = Long.parseLong(attrValues[i]);
                byte[] b = Converter.longToBytes(timestamp);
                System.arraycopy(b, 0, ans, ptr, 8);
                ptr += 8;
            }else{
                throw new RuntimeException("Do not support this type'" + attrType + "'.");
            }
        }
        if(storeRecordSize != ptr){
            throw new RuntimeException("convert has exception.");
        }
        return ans;
    }

    /**
     * obtaining event types from byte records
     * @param record    byte record
     * @param typeIdx   column number where the type is located
     * @return          event type
     */
    public final String getTypeFromBytesRecord(byte[] record, int typeIdx){
        int start = positions[typeIdx].startPos();
        int offset = positions[typeIdx].offset();
        byte[] bytes = new byte[offset];
        System.arraycopy(record, start, bytes, 0, offset);
        int v = Converter.bytesToInt(bytes);
        // convert int to string
        return allEventTypes.get(v);
    }

    public final long getValueFromBytesRecord(byte[] record, int colIdx){
        int start = positions[colIdx].startPos();
        int offset = positions[colIdx].offset();
        byte[] bytes = new byte[offset];
        System.arraycopy(record, start, bytes, 0, offset);
        return Converter.bytesToLong(bytes);
    }

    /**
     * convert byte arrays into string records
     * @param record byte array record
     * @return record string
     */
    public String byteEventToString(byte[] record){
        String ans = "";

        int ptr = 0;
        for(int i = 0; i < attrTypes.length; ++i) {
            String attrType = attrTypes[i];
            if (attrType.equals("INT")) {
                byte[] b = new byte[4];
                System.arraycopy(record, ptr, b, 0, 4);
                int v = Converter.bytesToInt(b);
                ans = (i == 0) ? (ans + v) : (ans + "," + v);
                ptr += 4;
            } else if (attrType.contains("FLOAT")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                float scale = (float) Math.pow(10, getIthDecimalLens(i));
                float rawValue = v / scale;
                ans = (i == 0) ? (ans + rawValue) : (ans + "," + rawValue);
                ptr += 8;
            } else if (attrType.contains("DOUBLE")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                double scale = Math.pow(10, getIthDecimalLens(i));
                double rawValue = v / scale;
                ans = (i == 0) ? (ans + rawValue) : (ans + "," + rawValue);
                ptr += 8;
            } else if (attrType.contains("TYPE")) {
                byte[] b = new byte[4];
                System.arraycopy(record, ptr, b, 0, 4);
                int v = Converter.bytesToInt(b);

                String type = allEventTypes.get(v);
                ans = (i == 0) ? (ans + type) : (ans + "," + type);
                ptr += 4;
            } else if (attrType.equals("TIMESTAMP")) {
                byte[] b = new byte[8];
                System.arraycopy(record, ptr, b, 0, 8);
                long v = Converter.bytesToLong(b);
                ans = (i == 0) ? (ans + v) : (ans + "," + v);
                ptr += 8;
            } else {
                throw new RuntimeException("Do not support this type'" + attrType + "'.");
            }
        }
        return ans;
    }

    /**
     * obtain the byte array corresponding to the i-th attribute,
     * and then convert it to the corresponding variable based on the type
     * @param record byte array record
     * @param i      query i-th attribute
     * @return       i-th attribute byte array
     */
    public byte[] getIthAttrBytes(byte[] record, int i){
        int start = positions[i].startPos();
        int len = positions[i].offset();
        byte[] ans = new byte[len];
        System.arraycopy(record, start, ans, 0, len);
        return ans;
    }

    public void print(){
        System.out.println("schema_name: '" +  schemaName + "'");
        System.out.println("-------------------------------------------------------------");
        System.out.printf("|%-12s|", "AttrName");
        System.out.printf("%-12s|", "AttrType");
        System.out.printf("%-16s|", "MinValue");
        System.out.printf("%-16s|%n", "MaxValue");
        System.out.println("-------------------------------------------------------------");

        for(int i = 0; i < attrNames.length; i++){
            System.out.printf("|%-12s|", attrNames[i]);
            System.out.printf("%-12s|", attrTypes[i]);
            System.out.printf("%-16s|", attrMinValues[i]);
            System.out.printf("%-16s|%n", attrMaxValues[i]);
        }
        System.out.println("-------------------------------------------------------------");

        typeMap.forEach((k, v) -> System.out.println("Event type: " + k + " type id: " + v));

    }

}
