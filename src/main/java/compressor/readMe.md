
Note that:
* a original timestamp need 8 bytes to store
* an RID need 6 bytes to store <page (4 bytes) + offset (2 bytes)>

We always assume that delta value less than Integer.MAX_VALUE;
given a data point list: $<t1_1, ..., ts_k>$

| Algorithm        | Details                                                                                                      |
|------------------|--------------------------------------------------------------------------------------------------------------|
| Simple delta     | let $t_0 = min{t1_1, ..., ts_k}$, then we store <ts_0, ts_1', ..., ts_k'>, where $ts_i' = ts_i - ts_0$.      |
| delta of delta   | let $ts_i' = ts_i - ts_{i-1}}$, then we get the delta value list: zigzag.encode(ts_i'), store delta of delta |
| delta + varint   | get delta list and use zigzag code, then 7 bits value: 0XXX XXXX, 14 bits: 1XXX XXXX 0XXX XXXX, ...          |
| delta + simple8b | get delta list and use zigzag code, finally use simple8b to compress                                         |

To deal with in-order-insertion and out-of-order-insertion,
we need to use ZigZig to transform negative number to positive number


# delta compression

Suppose we have k data points, $x_1, ..., x_k$, then the output of delta compression is
$x_0, x_1', ..., x_k'$, where $x_0=min{x_1, ..., x_k}, x_i'=x_i-x_0$.

Then, a compressed timestamp is stored in 4 bytes (we have verified that compressed timestamp without overflow),
a compressed RID is stored in 4 bytes (compressed page (2 bytes) + original offset (2 bytes))

index block granularity

# delta of delta compression (with ZigZig)
cluster granularity





