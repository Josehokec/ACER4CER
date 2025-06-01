package btree;

import common.IndexValuePair;

import java.util.List;

public interface BPlusTreeInterface{

    void insert(long key, IndexValuePair value);

    List<IndexValuePair> rangeQuery(long min, long max);
}

