package common;

import baselines.FullScanPlus;
import store.EventStore;
import store.RID;

class StatementParserTest {

    @org.junit.jupiter.api.Test
    void parseCreateTable() {

        String createTableStr = "CREATE TABLE test (type TYPE, a1 INT, a2 DOUBLE.3, a3 CHAR[16], timestamp TIMESTAMP)";
        String str = StatementParser.convert(createTableStr);
        StatementParser.createTable(str);

        String schemaName = "TEST";
        Metadata metadata = Metadata.getInstance();
        EventSchema schema = metadata.getEventSchema(schemaName);
        FullScanPlus fullScanPlus = new FullScanPlus(schema);

        String[] records = {"A,1,2.317,0000-0001,1", "B,2,8.3,0000-0002,2", "A,5,0.321,0000-0003,3", "C,-1,0.1,0000-0004,4",
                "D,-999,3.0,0000-0005,5", "B,2,0.113,0000-0006,6", "A,5,71.2,0000-0007,7", "C,-9,0.3,0000-0008,8"};
        for (String record : records) {
            fullScanPlus.insertOrDeleteRecord(record, false);
        }
        int curPage = 0;
        short curOffset = 0;
        short recordSize = schema.getFixedRecordSize();
        EventStore store = schema.getStore();
        int pageSize = store.getPageSize();
        for(int i = 0; i < records.length; ++i) {
            if (curOffset + recordSize > pageSize) {
                curPage++;
                curOffset = 0;
            }
            RID rid = new RID(curPage, curOffset);
            curOffset += recordSize;
            // read record from store
            byte[] event = store.readByteRecord(rid);
            String readStr = schema.byteEventToString(event);
            if(!readStr.equals(records[i])) {
                System.out.println("different results ==> [readStr]: " + readStr + " , [record]: " + records[i]);
            }
            System.out.println(readStr);
        }
    }
}