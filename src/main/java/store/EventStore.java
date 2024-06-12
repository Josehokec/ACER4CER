package store;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Event Storage Class
 * Storing Byte Type Records
 * filename=schemaName.store
 * Caching function not implemented
 */
public class EventStore {
    private final int recordSize;                   // Byte size of a record
    private int curPage;                            // Current buffered pages
    private final int pageSize;                     // page size
    private int pageNum;                            // page number of the last inserted record
    private short offset;                           // offset
    private final String schemaName;                // schema name
    private File file;                              // file
    private final ByteBuffer buf;                   // byte buffer
    private MappedByteBuffer readMappedBuffer;      // mapped buffer

    public EventStore(String schemaName, int recordSize){
        String dir = System.getProperty("user.dir");
        // new File(dir).getParent()
        String storePath = dir + File.separator + "store";
        String filename = schemaName + ".binary_file";
        String filePath = storePath + File.separator + filename;
        System.out.println("store filePath: " + filePath);
        file = new File(filePath);
        // If this file has existed before, we clear the content
        if(file.exists()){
            if(file.delete()){
                System.out.println("file: '"+ filename + "' exists in disk, we clear the file content.");
            }
        }

        this.recordSize = recordSize;
        this.schemaName = schemaName;
        // page size is set to 8K
        pageSize = 8 * 1024;
        buf = ByteBuffer.allocate(pageSize);
        curPage = -1;
        pageNum = 0;
        offset = 0;
        readMappedBuffer = null;
    }

    public final int getPageSize(){
        return pageSize;
    }

    public int getPageNum(){
        return pageNum;
    }

    /**
     * insert a record to file
     * byte[] content = new byte[len];
     * System.arraycopy(array, 0, content, 0, len);
     * @param record byte array record
     * @return RID pointer
     */
    public final RID insertByteRecord(byte[] record){
        RID rid;
        // If the cache can no longer hold data, it will be flushed to a file
        if(offset + recordSize > pageSize){
            // Lock the content and then flush the data into the file
            buf.flip();
            try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
                // note that this page may not be full, but it doesn't matter
                byte[] array = buf.array();
                out.write(array);
                out.flush();
            }catch (Exception e) {
                e.printStackTrace();
            }
            pageNum++;
            offset = 0;
        }
        buf.put(record);
        rid = new RID(pageNum, offset);
        offset += recordSize;
        return rid;
    }

    /**
     * Read bytes based on the value of rid
     * It is possible that the current page has been read before, so there is no need to map again
     * if rid.page = curPage, then direct read
     * @param rid RID pointer
     * @return record
     */
    public final byte[] readByteRecord(RID rid){
        // Before reading, it is necessary to ensure that all data in the buffer is written in the file
        if(buf.hasRemaining()){
            buf.flip();
            int len = buf.limit();
            try(BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, true))){
                byte[] array = buf.array();
                byte[] content = new byte[len];
                System.arraycopy(array, 0, content, 0, len);
                out.write(content);
                out.flush();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        // System.out.println(file.getAbsoluteFile());
        byte[] byteRecord = new byte[recordSize];
        // This page is already in memory and can be directly read and returned
        if(curPage == rid.page()){
            readMappedBuffer.get(rid.offset(), byteRecord);
            return byteRecord;
        }

        RandomAccessFile raf;
        try{
            int queryPage = rid.page();
            raf = new RandomAccessFile(file, "rw");
            long startPos = (long) queryPage * pageSize;

            //long readStart = System.nanoTime();
            // Obtain the file channel and then send the data from the specified location to memory
            FileChannel fileChannel = raf.getChannel();
            readMappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startPos, pageSize);
            curPage = queryPage;
            //long readEnd = System.nanoTime();
            readMappedBuffer.get(rid.offset(), byteRecord);
            //System.out.println("read a page cost: " + (readEnd - readStart) + "ns");
            // If not closed, it may cause exceptions
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return byteRecord;
    }

    public final MappedByteBuffer getMappedBuffer(int queryPage){
        if(queryPage > pageNum){
            throw new RuntimeException("this page does not exist!");
        }
        RandomAccessFile raf;
        MappedByteBuffer ans = null;
        try{
            raf = new RandomAccessFile(file, "rw");
            long startPos = (long) queryPage * pageSize;
            FileChannel fileChannel = raf.getChannel();
            ans = fileChannel.map(FileChannel.MapMode.READ_WRITE, startPos, pageSize);
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ans;
    }
}

