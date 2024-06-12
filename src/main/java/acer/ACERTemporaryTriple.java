package acer;

import store.RID;

/**
 * Fast Temporary Quad <br>
 * An event record -> a temporary triple in buffer <br>
 * It store record timestamp, record rid, byte record and index attribute value <br>
 * Note that the attribute values of the index have been converted
 */
public record ACERTemporaryTriple(boolean flag, long timestamp, RID rid, long[] attrValues) { }
