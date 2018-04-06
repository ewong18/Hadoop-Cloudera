DROP TABLE IF EXISTS database.tablename;

CREATE EXTERNAL TABLE database.tablename
(
column_1 string,
column_2 int,
column_3 decimal(18,2),
column_4 timestamp
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '<hdfs path>';
