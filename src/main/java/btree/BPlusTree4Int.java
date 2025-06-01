package btree;

import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Entry;
import com.github.davidmoten.bplustree.Serializer;
import common.IndexValuePair;
import org.apache.commons.io.FileUtils;
import store.RID;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree4Int implements BPlusTreeInterface{
    private BPlusTree<Integer, byte[]> bPlusTree;

    public BPlusTree4Int(String attrName, String schemaName){
        String dir = System.getProperty("user.dir");
        String storePath = new File(dir).getParent() + File.separator + "store";

        String indexDirectory = storePath + File.separator + schemaName + File.separator + attrName;
        File file = new File(indexDirectory);
        if(file.exists()){
            try {
                FileUtils.cleanDirectory(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        file.mkdirs();

        // value: <long key, RID(int pageNum, short offset)> -> 8 + 4 + 2 = 14
        // please note that original code set segmentSizeMB(1), we found it slow down the performance
        bPlusTree = BPlusTree.file().directory(file).maxKeys(128)
                .keySerializer(Serializer.INTEGER).
                valueSerializer(Serializer.bytes(14)).
                naturalOrder();
    }

    @Override
    public void insert(long key, IndexValuePair value) {
        ByteBuffer buffer = ByteBuffer.allocate(14);
        buffer.putLong(value.timestamp());
        buffer.putInt(value.rid().page());
        buffer.putShort(value.rid().offset());
        byte[] values = buffer.array();
        int newKey = (int) key;
        bPlusTree.insert(newKey, values);
    }

    @Override
    public List<IndexValuePair> rangeQuery(long min, long max) {
        // we found that query the range [min, max] returns uncorrected results
        // here we query a lager range and filter
        int queryMin = (min == Long.MIN_VALUE) ? Integer.MIN_VALUE : (int) (min - 1);
        int queryMax = (max == Long.MAX_VALUE) ? Integer.MAX_VALUE : (int) (max + 1);
        Iterable<Entry<Integer, byte[]>> entries = bPlusTree.findEntries(queryMin, queryMax);
        List<IndexValuePair> ans = new ArrayList<>();
        for(Entry<Integer, byte[]> entry : entries){
            long curKey = entry.key();
            if(curKey >= min && curKey <= max){
                ByteBuffer buff = ByteBuffer.wrap(entry.value());
                ans.add(new IndexValuePair(buff.getLong(), new RID(buff.getInt(), buff.getShort())));
            }
        }

        return ans;
    }
}
