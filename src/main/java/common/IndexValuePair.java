package common;


import store.RID;

/**
 * List of values for column indexing
 * The original paper uses triples <int recordSeq, long timestamp, RID rid>
 * record sequence + the timestamp of the event + RID pointing to the record
 * Due to some assumptions in this file, it is not necessary to use the record Sequence variable
 */
public record IndexValuePair(long timestamp, RID rid) {

    @Override
    public String toString(){
        return "timestamp: " + timestamp + " | page: " + rid.page() + " offset: " + rid.offset();
    }
}
