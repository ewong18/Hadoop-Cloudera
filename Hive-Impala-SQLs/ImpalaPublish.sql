Invalidate Metadata impaladb.table1;

Compute Stats hivedb.table1;

DROP TABLE IF EXISTS impaladb.table1;

-- Recreate Presentation table
CREATE TABLE impaladb.table1  
(
col_1 string,
col_2 int,
col_3 timestamp,
col_4 decimal(18,2)
)
STORED AS PARQUET;

-- Load all from Hive table
insert into impaladb.table1
SELECT 
a.col_1,
b.column_3 as col_2,
a.col_3,
a.col_4
FROM hivedb.table1 as a
    left outer join hivedb.table2 as b
        on a.col_1 = b.col_2;

-- Use native Impala function to collect generic stats
compute stats impaladb.table1;
