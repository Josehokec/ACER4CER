package acer;

import store.RID;

/**
 * ACER Temporary Quad <br>
 * An event record -> a temporary quad in buffer <br>
 * It store record deletion flag, timestamp, record rid, byte record and index attribute value <br>
 * Note that :<br>
 * 1. flag = false means insertion operation,
 *    flg = true means deletion operation
 * 2. the attribute values of the index have been converted
 */
public record ACERTemporaryQuad(boolean flag, long timestamp, RID rid, long[] attrValues) { }
