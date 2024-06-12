## Description
```FastInDisk``` package aims to store fast index persistently (store fast index into disk)


FAST (disk version) has four components:
1. BufferPool (in memory): buffers event
 
2. BlockCache (in memory): caches index blocks, when cache is full,
we use LRU algorithm to replace index block

3. SynopsisTable (in memory): records block storage position for each event type
 
4. IndexBlock (in disk): stores range bitmaps, timestamps, RID pointers


```FastModule``` package also is fast index, but we implement it in memory. 
FAST (memory version) has three components:

1. BufferPool (in memory): buffers event

2. SynopsisTable (in memory): records block storage position for each event type

3. IndexPartition (in memory): stores range bitmap, timestamps, RID pointers 
(here we used different names to distinguish between memory and disk indexes)


