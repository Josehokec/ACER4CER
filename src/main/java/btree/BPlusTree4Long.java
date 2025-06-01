package btree;

import common.IndexValuePair;
import store.RID;
import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Entry;
import com.github.davidmoten.bplustree.Serializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * the BPlusTree that is stored in disk
 * key: attribute value, long: 8 bytes
 * value: <long, rid>, 8 + 6 = 14 bytes
 * note that even if attribute value is integer, we also store long value
 * as same setting can facilitate our management
 * in fact, when using prefix compression techniques,
 * we can ensure their space overhead is same
 */
public class BPlusTree4Long implements BPlusTreeInterface{
    private BPlusTree<Long, byte[]> bPlusTree;

    public BPlusTree4Long(String attrName, String schemaName){
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
                .keySerializer(Serializer.LONG).
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
        bPlusTree.insert(key, values);
    }

    @Override
    public List<IndexValuePair> rangeQuery(long min, long max) {
        // we found that query the range [min, max] returns uncorrected results
        // here we query a lager range and filter
        long queryMin = (min == Long.MIN_VALUE) ? min : min - 1;
        long queryMax = (max == Long.MAX_VALUE) ? max : max + 1;

        Iterable<Entry<Long, byte[]>> entries = bPlusTree.findEntries(queryMin, queryMax);

        List<IndexValuePair> ans = new ArrayList<>();

        for(Entry<Long, byte[]> entry : entries){
            long curKey = entry.key();
            if(curKey >= min && curKey <= max){
                ByteBuffer buff = ByteBuffer.wrap(entry.value());
                ans.add(new IndexValuePair(buff.getLong(), new RID(buff.getInt(), buff.getShort())));
            }
        }

        return ans;
    }
}
