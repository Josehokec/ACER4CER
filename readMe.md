# ACER: Accelerating Complex Event Recognition via Two-Phase Filtering under Range Bitmap-Based Indexes

### Summary

**ACER is a simple, but efficient method (the greatest truths is concise) that uses index structures to accelerate complex event recognition.**

**ACER key ideas:**

* Using range bitmap to index attribute value
* Aggregating same type events as a cluster, building synopsis information for each cluster to skip unnecessary access
* Developing two-phase filtering algorithm to avoid unnecessary disk access in indexes and events

**ACER characteristic：**

* Easy to implement
* Window-aware filtering
* Low insertion latency
* Low query latency
* Low storage overhead

**.bib**

```tex
@inproceedings{Liu2024,
  author = {Shizhe Liu and Haipeng Dai and Shaoxu Song and Meng Li and Jingsong Dai and Rong Gu and Guihai Chen},
  title = {{ACER}: Accelerating Complex Event Recognition via Two-Phase Filtering under Range Bitmap-Based Indexes},
  year = {2024},
  booktitle = {Proceedings of ACM SIGKDD conference on Knowledge Discovery and Data Mining},
  pages = {},
  doi={https://doi.org/10.1145/3637528.3671814}
}
```

## Section 1 Running

Firstly, create `store` folder. Secondly, uncompress data. Thirdly, open `ACER4CER` folder with IDEA and run the target java file.

**Example**. Suppose you want to run ```Test_ACER_Nasdaq.java```,then you will see the code in the main function.

```java
public class Test_ACER_Nasdaq {
    //....
    public static void main(String[] args) throws FileNotFoundException {
        // ... set prefixPath
        // 1. create table, alter attribute range
        Test_ACER_Nasdaq.createSchema();
        // 2. create index
        Index index = Test_ACER_Nasdaq.createIndex();
        // 3. store events
        String filename = "nasdaq.csv";
        String dataFilePath = prefixPath + "dataset" + sep + filename;
        long buildCost = Test_ACER_Nasdaq.storeEvents(dataFilePath, index);

        // 4. read query file
        String queryFilePath = prefixPath + "java" + sep + "Query" + sep + "nasdaq_query.json";
        String jsonFileContent = JsonReader.getJson(queryFilePath);
        JSONArray jsonArray = JSONArray.fromObject(jsonFileContent);

        // 5.1 choose NFA engine to match
        final PrintStream consoleOut = System.out;
        System.out.println("use NFA to match");
        MatchEngine engine1 = MatchEngine.NFA;
        PrintStream printStream1 = new PrintStream(prefixPath + "output" + sep + "acer_nasdaq_nfa.txt");
        System.setOut(printStream1);
        System.out.println("choose NFA engine to match");
        System.out.println("build cost: " + buildCost +"ms");
        Test_ACER_Nasdaq.indexBatchQuery(index, jsonArray, engine1);

        // ...
    }
}
```

**Code Explanations**
* It first creates event schema, index structures, and constraint property value ranges based on the statement.
* Next, it loads the record file.
* Then it loads the query statement `.json` file.
* Finally, it executes a specific method for querying.

**Result Explanations**
```
122-th query starting...                  // 122-th query pattern
filter cost: 4.077ms                      // cost of pre-filtering out unrelated events
scan cost: 3.934ms                        // cost of scaning disk to obtain events
match cost: 9.219ms                       // cost of matching in evaluation engines
number of tuples: 268                     // number of matched tuples  
122-th query cost: 18ms.                  // pattern query latency
```

## Section 2 Program `ACER4CER`

### Section 2.1: Package

| Name      | Explanation                                                                |
|-----------|----------------------------------------------------------------------------|
| acer      | our solution for accelerating CER                                          |
| arrival   | Arrivals parameters (all indexes methods dependent on event type arrivals) |
| automaton | Non-determine finite automaton (NFA) for CER                               |
| common    | Frequently used classes or tool classes                                    |
| condition | Predicate constraints                                                      |
| fullscan  | Full scan method                                                           |
| generator | It can automatically generate synthetic data or queries                    |
| join      | Match engine (also termed as evaluation engine) using join                 |
| method    | offer a base class for index                                               |
| pattern   | complex event query pattern                                                |
| query     | Query statement, all queryies stored in json file                          |
| store     | Store event into a file                                                    |
| Output    | Result file                                                                |

### Section 2.2: Key class for running

| Dataset                   | java class                                                |
| ------------------------- |-----------------------------------------------------------|
| Cluster (also called job) | Test_ACER_Cluster.java<br>Test_FullScan_Cluster.java      |
| Crimes                    | Test_ACER_Crimes.java<br/>Test_FullScan_Crimes.java       |
| NASDAQ                    | Test_ACER_Nasdaq.java<br/>Test_FullScan_Nasdaq.java       |
| Synthetic                 | Test_ACER_Synthetic.java<br/>Test_FullScan_Synthetic.java |

## Section 3 Base-2 Bit Sliced Range Encoded Bitmap (Range Bitmap)

[Range bitmap blog url](https://richardstartin.github.io/posts/range-bitmap-index#implementing-a-range-index)

**Range bitmap paper:**

[1] Chee Yong Chan, Yannis E. Ioannidis. Bitmap Index Design and Evaluation. SIGMOD. 1998, p355-366.

## Section 4: Experimental Datasets

Our paper used both synthetic and real datasets. We provided the download URLs for the real dataset. You can also choose to send emails to the author to inquire about the real datasets. We will be very happy to receive your emails and will respond promptly.

| Dataset   | Method       | Indexed columns                                             |
| --------- |--------------| ----------------------------------------------------------- |
| Cluster   | ACER         | scheduling Class                                            |
|           | IntervalScan | scheduling Class, type, timestamp                           |
|           | NaiveIndex   | scheduling Class, type                                      |
| NASDAQ    | ACER         | open, vol                                                   |
|           | IntervalScan | open, vol, ticker, date                                     |
|           | NaiveIndex   | open, vol, ticker                                           |
| Crimes    | ACER         | beat, district, latitude, longtitude                        |
|           | IntervalScan | beat, district, latitude, longitude, primaryType, timestamp |
|           | NaiveIndex   | beat, district, latitude, longitude, primaryType            |
| Synthetic | ACER         | a1, a2, a3                                                  |
|           | IntervalScan | a1, a2, a3, type, timestamp                                 |
|           | NaiveIndex   | a1, a2, a3, type                                            |

###  Section 4.1 Real-world datasets

**Real-world dataset overview**

| Name                                                         | Columns                                                    | Event number | File size |
| ------------------------------------------------------------ | ---------------------------------------------------------- | ------------ | --------- |
| [Google cluster](https://github.com/google/cluster-data)     | type,jobID, schedulingClass, timestamp                     | 2.0M         | 57.4MB    |
| [Crimes](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g) | ticker,open, high, low, close, vol, date                   | 7.6M         | 493.5MB   |
| [NASDAQ](https://www.eoddata.com)                            | primaryType, ID, beat, district, latitude, longitude, date | 8.7M         | 418MB     |

For `Google cluster` dataset, we choose four attributes from the job table: `event type, job ID, scheduling class,  time`. Then we choose one attribute to index: `scheduling class` (when using tree index, we need to index type).

For `Crime` dataset, we choose 7 columns ```Primary Type (String), ID (int), Beat (int), District(int), Latitude (Double.9), Longitude (Double.9), Date (Date format)```. We transform the date to a timestamp and sort the record according to the timestamp. We choose index attributes as follows: `Beat, District, Latitude, Longitude` (when using tree index, we need to index type).

For `NASDAQ` dataset, we only choose 15 famous stocks (*e,g,* MSFT, GOOG, AAPL, TSLA, *et.al.*) rather than all stocks. The dataset records each stock price change per minute in trade time.

### Section 4.2  Synthetic dataset

We have written a synthetic data generator to automatically generate synthetic data of a specified size. `Generator` folder has a `SyntheticQueryGenerator.java` file that can generate synthetic datasets.

**Schema: `(String type,int a1,int a2,float a3,float a4,long timestamp)`**

**Generator setting**

```
zipf alpha= 1.3
a1 ~ Uniform[0,1000]
a2 ~ Uniform[0,1000]
a3 ~ Uniform[0,1000]
a4 ~ Uniform[0,1000]

each variable has 1~3 independent constraints (selectivity range: 0.01~0.2)
each query has 1~3 dependent constraints
float-point value in a3 and a4 have up to two digits after the decimal point
```

Suppose the probability of occurrence of event types follows a `Zipf` distribution. The difference in timestamps for each adjacent record is 1. We generated 3 synthetic datasets, they contain 10_000_000 (10M), 100_000_000 (100M) and 1_000_000_000 (1G) events, respectively.


### Section 4: DBMS and Flink Experiments

We used a leading commercial database (referred to as `DBMS` here due to copyright issues) and `Flink` to perform some queries on the Crimes dataset, and we found that the query results were different from `DBMS` and `Flink`. However, this does not mean that our code has a bug. The reasons for different query results are as follows:

* In order to support `skip-till-next-match`, `DBMS` needs to use wildcards (e.g.`*`) for querying. When wildcard matches are used, extreme greedy strategies are used for matching, which is not equivalent to  `skip-till-next-match`. DBMS currently does not seem to support `skip-till-any-match` strategy.
* For  `skip-till-next-match` and `skip-till-any-match`, `Flink` first sorts events based on their timestamps. When two different events have the same timestamp, their sorting position may differ from the initial order, resulting in mismatched results. **If the timestamp of each event is different, then the answer queried by Flink must be the same as the answer queried by ACER.**

### Section 4.1 DBMS example for crimes dataset

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
  ORDER by n
), prefilter AS(
  SELECT i.*
 FROM input_bucketized i, buckets b
 WHERE i.bk = b.n
)
SELECT *
FROM prefilter MATCH_RECOGNIZE(
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

### Section 4.2 Flink example for crimes dataset

```java
Pattern<CrimesEvent, ?> pattern = Pattern.<CrimesEvent>begin("v0").where(
        new SimpleCondition<CrimesEvent>() {
            @Override
            public boolean filter(CrimesEvent event){
                boolean independent  = event.getPrimaryType().equals("ROBBERY");
                return independent;
            }
        }
).followedByAny("v1").where(
        new IterativeCondition<CrimesEvent>() {
            @Override
            public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                boolean ic = event.getPrimaryType().equals("BATTERY");
                boolean dc = false;
                for(CrimesEvent v0Event : ctx.getEventsForPattern("v0")){
                    dc = event.getLongitude() >= v0Event.getLongitude() - 0.05 &&
                            event.getLongitude() <= v0Event.getLongitude() + 0.05 &&
                            event.getLatitude() >= v0Event.getLatitude() - 0.02 &&
                            event.getLatitude() <= v0Event.getLatitude() + 0.02;
                }
                return dc && ic;
            }
        }
).followedByAny("v2").where(
        new IterativeCondition<CrimesEvent>() {
            @Override
            public boolean filter(CrimesEvent event, Context<CrimesEvent> ctx) throws Exception {
                boolean ic = event.getPrimaryType().equals("MOTOR_VEHICLE_THEFT");
                boolean dc = false;
                for(CrimesEvent v0Event : ctx.getEventsForPattern("v0")){
                    dc = event.getLongitude() >= v0Event.getLongitude() - 0.05 &&
                            event.getLongitude() <= v0Event.getLongitude() + 0.05 &&
                            event.getLatitude() >= v0Event.getLatitude() - 0.02 &&
                            event.getLatitude() <= v0Event.getLatitude() + 0.02;
                }
                return dc && ic;
            }
        }
).within(Time.milliseconds(108001));
```

### Section 4.3 DBMS example for NASDAQ dataset

**Create schema**

```sql
CREATE TABLE Crimes( 
  ticker VARCHAR(16), 
  open Binary_double, 
  high Binary_double,
  low Binary_double,
  close Binary_double,
  vol int NOT NULL, 
  timestamp NUMBER NOT NULL 
);
```

**Original Query (poor efficiency)**

```sql
SELECT * FROM NASDAQ MATCH_RECOGNIZE(
    MEASURES V1.timestamp AS T1, V2.timestamp AS T2, V3.timestamp AS T3, V4.timestamp AS T4
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (V1 Z* V2 Z* V3 Z* V4)
    DEFINE 
        V1 AS V1.ticker = 'MSFT' AND V1.open BETWEEN 326 AND 334,
        V2 AS V2.ticker = 'GOOG' AND V2.open BETWEEN 120 AND 130,
  		  V3 AS V3.ticker = 'MSFT' AND V3.open >= V1.open * 1.007,
        V4 AS V4.ticker = 'GOOG' AND V4.open <= V2.open * 0.993,
            AND V4.timestamp - V1.timestamp <= 720
);
```

**Improved Query**

```sql
WITH filter_events AS(
    SELECT ticker, open, timestamp
    FROM NASDAQ
    WHERE primary_type = 'MSFT' OR primary_type = 'GOOG'
)
SELECT * 
FROM filter_events MATCH_RECOGNIZE(
    MEASURES V1.timestamp AS T1, V2.timestamp AS T2, V3.timestamp AS T3, V4.timestamp AS T4
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (V1 Z* V2 Z* V3 Z* V4)
    DEFINE 
        V1 AS V1.ticker = 'MSFT' 
            AND V1.open BETWEEN 326 AND 334,
        V2 AS V2.ticker = 'GOOG' 
            AND V2.open BETWEEN 120 AND 130
            AND V2.timestamp - V1.timestamp <= 720,
  		  V3 AS V3.ticker = 'MSFT' 
            AND V3.open >= V1.open * 1.007
            AND V3.timestamp - V1.timestamp <= 720,
        V3 AS V3.ticker = 'GOOG' 
            AND V4.open <= V2.open * 0.993 
            AND V4.timestamp - V1.timestamp <= 720
);
```

**Join-based  Query (please see VLDB'23 - High-Performance Row Pattern Recognition Using Joins)**

```sql
WITH input_bucketized AS(
  SELECT ticker, open, timestamp, FLOOR(timestamp / 720) AS bk
  FROM NASDAQ
), filter_v1 AS(
  SELECT bk
  FROM input_bucketized
  WHERE ticker = 'MSFT' AND open BETWEEN 325 AND 335
), filter_v3 AS(
  SELECT bk
  FROM input_bucketized
  WHERE ticker = 'GOOG' AND open BETWEEN 120 AND 130
), ranges AS(
  SELECT V1.bk AS bk_start, V3.bk AS bk_end
  FROM filter_v1 V1, filter_v3 V3
  WHERE V1.bk = V3.bk
  UNION
  SELECT V1.bk AS bk_start, V3.bk AS bk_end
  FROM filter_v1 V1, filter_v3 V3
  WHERE V1.bk + 1 = V3.bk
), buckets AS(
  SELECT DISTINCT n FROM ranges
  CROSS JOIN generate_series(bk_start, bk_end + 1)
), prefilter AS(
  SELECT i.*
  FROM input_bucketized i, buckets b
  WHERE i.bk = b.n
  ORDER BY timestamp
)
SELECT COUNT(*) 
FROM prefilter MATCH_RECOGNIZE(
    MEASURES V1.timestamp AS T1, V2.timestamp AS T2, V3.timestamp AS T3, V4.timestamp AS T4
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (V1 Z* V2 Z* V3 Z* V4)
    DEFINE 
        V1 AS V1.ticker = 'MSFT' AND V1.open BETWEEN 325 AND 335,
        V2 AS V2.ticker = 'GOOG' AND V2.open BETWEEN 120 AND 130,
        V3 AS V3.ticker = 'MSFT' AND V3.open >= V1.open * 1.003,
        V4 AS V4.ticker = 'GOOG' AND V4.open <= V2.open * 0.997
            AND V4.timestamp - V1.timestamp <= 720
);
```

### Section 4.4 Inconsistent evaluation results

Given an event set and a complex event query, the matched tuples obtained from NFA and Join Tree may differ. Besides, the evaluation results from Flink and commercial databases may also differ. The inconsistent results of two evaluation engines are caused by events with the same timestamp, while the inconsistent results of existing systems are caused by different selection strategies. The inconsistency in results between the two evaluation engines stems from events sharing the same timestamp. In contrast, the inconsistency in existing systems' results arises from varying selection strategies.

For example, consider an event stream $E_2$: $\{e_1=(B,1), e_2=(A,1), e_3=(C,2), e_4=(B,3), e_5=(B,4)\}$, where the first column of event represents the event type and the second column of event represents the event occurrence timestamp. Now, given a complex event query, $Q_2$: \texttt{SEQ(A a, B b) USE skip-till-next-match WITHIN 4}, the matched tuple from the NFA is $(e_2,e_4)$, while the matched tuple from the Join Tree is $(e_2,e_1)$.

In the SQL standard, a wildcard character ($*$) is defined to support skipping unrelated events. Commercial databases utilize greedy matching for the wildcard character to search for matched tuples. Thus, the selection strategy in commercial databases does not adhere to the *skip-till-next-match* semantics, resulting in inconsistent evaluation results when compared to FlinkCEP. For example, given the event stream $E_2$ and query $Q_2$, the matched tuple in the commercial database is $(e_2,e_5)$, whereas the matching tuple in FlinkCEP is $(e_2,e_4)$.

The selection strategy we have implemented fully aligns with the semantics of *skip-till-next-match* and *skip-till-any-match*. Thus, if there are no two events with identical timestamps, our query results can be consistent with the FlinkCEP query results.
