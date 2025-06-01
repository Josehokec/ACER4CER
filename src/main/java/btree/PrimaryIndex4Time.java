package btree;

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
 * interval scan has a primary index and multiple secondary indexes
 */
public class PrimaryIndex4Time {
    // key is timestamp (8 byte), value is RID (6 byte)
    private final BPlusTree<Long, byte[]> bPlusTree;

    public PrimaryIndex4Time(String attrName, String schemaName){
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
        bPlusTree = BPlusTree.file().directory(file)
                .maxLeafKeys(128).maxNonLeafKeys(128)
                .keySerializer(Serializer.LONG).valueSerializer(Serializer.bytes(6)).naturalOrder();
    }

    public void insert(long key, RID rid) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(rid.page());
        buffer.putShort(rid.offset());
        bPlusTree.insert(key, buffer.array());
    }

    public List<RID> rangeQuery(long min, long max) {
        long queryMin = (min == Long.MIN_VALUE) ? min : min - 1;
        long queryMax = (max == Long.MAX_VALUE) ? max : max + 1;

        Iterable<Entry<Long, byte[]>> entries = bPlusTree.findEntries(queryMin, queryMax);

        List<RID> ans = new ArrayList<>();
        for(Entry<Long, byte[]> entry : entries){
            long curKey = entry.key();
            if(curKey >= min && curKey <= max){
                ByteBuffer buff = ByteBuffer.wrap(entry.value());
                ans.add(new RID(buff.getInt(), buff.getShort()));
            }
        }
        return ans;
    }
}
