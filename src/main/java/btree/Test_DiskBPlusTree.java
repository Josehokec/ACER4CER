package btree;

import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Entry;
import com.github.davidmoten.bplustree.Serializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Test_DiskBPlusTree {

    public static void main(String[] args){
        String dir = System.getProperty("user.dir");
        // System.out.println("dir: " + dir);
        String indexDirectory = dir + File.separator + "test";

        File file = new File(indexDirectory);
        if(file.exists()){
            try {
                FileUtils.cleanDirectory(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        file.mkdirs();
        // 128 vs. 255
        BPlusTree<Long, byte[]> tree = BPlusTree.file().directory(file).
                maxKeys(128).
                keySerializer(Serializer.LONG).valueSerializer(Serializer.bytes(16)).naturalOrder();

        tree.print();

        // insert 10000000 events cost 52097ms
        int num = 10_000_00;//10_000_000

        long startInsertTime = System.currentTimeMillis();
        for(int i = 0; i < num; ++i){
            long key = i % 10_000;
            byte[] record = new byte[16];
            tree.insert(key, record);
            if((i + 1) % (num / 10) == 0){
                System.out.println("finish " + ((i + 1.0) / num) + "...");
            }
        }
        long endInsertTime = System.currentTimeMillis();

        // whether it must commit?
        tree.commit();
        System.out.println("insert " + num + " events cost " + (endInsertTime - startInsertTime) + "ms");

        long queryStartTime = System.currentTimeMillis();
        {
            long min = 2;
            long max = 202;
            Iterable<Entry<Long, byte[]>> entries = tree.findEntries(min - 1, max + 1);

            // Iterable<byte[]> records = tree.find(0L, 2L);
            int cnt = 0;
            for(Entry<Long, byte[]> entry : entries){
                if(entry.key() >= min && entry.key() <= max){
                    cnt++;
                }

            }
            System.out.println("cnt: " + cnt + " selectivity: " + ((cnt + 0.0)/ num));
        }
        {
            long min = 2;
            long max = 1002;
            Iterable<Entry<Long, byte[]>> entries = tree.findEntries(min - 1, max + 1);

            // Iterable<byte[]> records = tree.find(0L, 2L);
            int cnt = 0;
            for(Entry<Long, byte[]> entry : entries){
                if(entry.key() >= min && entry.key() <= max){
                    cnt++;
                }

            }
            System.out.println("cnt: " + cnt + " selectivity: " + ((cnt + 0.0)/ num));
        }
        long queryEndTime = System.currentTimeMillis();
        System.out.println("query cost " + (queryEndTime - queryStartTime) + "ms");
    }
}

/*
BPlusTree<Long, byte[]> tree = BPlusTree.file().directory(file).maxKeys(128).segmentSizeMB(1)
                .keySerializer(Serializer.LONG).valueSerializer(Serializer.bytes(16)).naturalOrder();
insert 10000000 events cost 52097ms
cnt: 201000 selectivity: 0.0201
cnt: 1001000 selectivity: 0.1001
query: 76ms

we found that remove .segmentSizeMB(1)
insert 10000000 events cost 5653ms
cnt: 201000 selectivity: 0.0201
cnt: 1001000 selectivity: 0.1001
query cost 665ms
 */
