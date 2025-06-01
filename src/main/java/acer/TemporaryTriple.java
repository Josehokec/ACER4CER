package acer;

import store.RID;

/**
 * [updated] temporary triple in ACER
 * An event record -> a temporary quad in buffer
 * It store record timestamp, record rid, byte record and attribute values <br>
 * Note that :the attribute values of the index have been converted
 */
public record TemporaryTriple(long timestamp, RID rid, long[] attrValues) {}
