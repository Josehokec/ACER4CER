
When run FlinkCEP under Java 17, you may encounter exception as follows:
```
Exception in thread "main" java.lang.reflect.InaccessibleObjectException: Unable to make field private final byte[] java.lang.String.value accessible: module java.base does not "opens java.lang" to unnamed module @130d63be
    at java.base/java.lang.reflect.AccessibleObject.checkCanSetAccessible(AccessibleObject.java:354)
    at java.base/java.lang.reflect.AccessibleObject.checkCanSetAccessible(AccessibleObject.java:297)
    ...
```

Flink's ClosureCleaner uses reflection when serializing and deserializing user code to properly transfer user-defined functions
(such as lambda expressions, anonymous classes) to the Flink cluster.
In some cases, it may attempt to access internal fields of the Java core library, triggering this error.

**Solution:** To avoid exception, you need to add `--add-opens`  option
to the JVM startup parameters to explicitly allow Flink to access the internals of the java.lang module.

### Workflow： Configuration --> Edit --> Modify options --> Add VM options
`--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED`




### Appendix
We used a leading commercial database (referred to as `DBMS` here due to copyright issues) 
to perform some queries on the three real-world datasets.
and we found that the query results were different for `DBMS` and `Flink`.
(Please note that our evaluation engine has same query results as Flink)

Notably, to support skip irrelevant events between two variables,
`DBMS` needs to use wildcards (e.g.`*`) for querying.
When wildcard matches are used, extreme greedy strategies are used for matching,
which is **not equivalent to** `skip-till-next-match`.
DBMS currently does not seem to support `skip-till-any-match` strategy.

More query examples for DBMS:
### DBMS example for CRIMES dataset

**Create schema**

```sql
CREATE TABLE Crimes( 
  primary_type VARCHAR(64), 
  id int, 
  beat int NOT NULL, 
  dis int NOT NULL, 
  lat Binary_double, 
  lon Binary_double, 
  timestamp NUMBER NOT NULL 
);
```

**Original Query (poor efficiency)**
Notably, Crimes is timestamp-ordered, so we remove `ORDER BY timestamp`.

```sql
SELECT * FROM Crimes MATCH_RECOGNIZE(
    MEASURES R.id AS RID, B.id AS BID, M.id AS MID
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (R Z* B Z* M)
    DEFINE 
        R AS R.primary_type = 'ROBBERY',
        B AS B.primary_type = 'BATTERY'
            AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02,
        M AS M.primary_type = 'MOTOR_VEHICLE_THEFT'
            AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
            AND M.timestamp - R.timestamp <= 1800
);
```

**Improved Query**

```sql
WITH filter_events AS(
    SELECT primary_type, id, lat, lon, timestamp
    FROM Crimes
    WHERE primary_type = 'ROBBERY' 
       OR primary_type = 'BATTERY' 
       OR primary_type = 'MOTOR_VEHICLE_THEFT'
)
SELECT * 
FROM filter_events MATCH_RECOGNIZE(
    MEASURES R.id AS RID, B.id AS BID, M.id AS MID
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (R Z* B Z* M)
    DEFINE 
        R AS R.primary_type = 'ROBBERY',
        B AS B.primary_type = 'BATTERY'
            AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
            AND B.timestamp - R.timestamp <= 1800,
        M AS M.primary_type = 'MOTOR_VEHICLE_THEFT'
            AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
            AND M.timestamp - R.timestamp <= 1800
);
```

**Join-based Query (please see VLDB'23 - High-Performance Row Pattern Recognition Using Joins)**

```sql
WITH input_bucketized AS(
  SELECT primary_type, id, lat, lon, timestamp, FLOOR(timestamp / 1800) AS bk
  FROM Crimes
), filter_r AS(
  SELECT FLOOR(timestamp / 1800) AS bk
  FROM Crimes
  WHERE primary_type = 'ROBBERY'
), filter_m AS(
  SELECT FLOOR(timestamp / 1800) AS bk
  FROM Crimes
  WHERE primary_type = 'MOTOR_VEHICLE_THEFT'
), ranges AS(
  SELECT R.bk AS bk_s, M.bk AS bk_e
  FROM filter_r R, filter_m M
  WHERE R.bk = M.bk
  UNION
  SELECT R.bk AS bk_s, M.bk AS bk_e
  FROM filter_r R, filter_m M
  WHERE R.bk + 1 = M.bk
), buckets AS(
  SELECT DISTINCT n FROM ranges
  CROSS JOIN generate_series(bk_s, bk_e)
  -- ORDER by n
), prefilter AS(
  SELECT i.*
 FROM input_bucketized i, buckets b
 WHERE i.bk = b.n
)
SELECT *
FROM prefilter MATCH_RECOGNIZE(
    MEASURES R.id AS RID, B.id AS BID, M.id AS MID
    ORDER BY timestamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (R Z* B Z* M)
    DEFINE 
        R AS R.primary_type = 'ROBBERY',
        B AS B.primary_type = 'BATTERY'
            AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
            AND B.timestamp - R.timestamp <= 1800,
        M AS M.primary_type = 'MOTOR_VEHICLE_THEFT'
            AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
            AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
            AND M.timestamp - R.timestamp <= 1800
);
```

### Flink example for CRIMES dataset

See our code [CrimesPatternQuery.java](./CrimesPatternQuery.java).