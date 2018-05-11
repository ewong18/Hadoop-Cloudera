import sys
from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#from pyspark.sql.functions import *

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print >> sys.stderr, "Usage: ParquetToHive.py <source path> <database.desired table name>"
		sys.exit(-1)
		　
	path = sys.argv[1]
  db_tblname = sys.argv[2]
  
  df = spark.read.parquet('/data/nap/stage/hdfs/ew_test/CDF_FLEX_TERR_PLN_MO_STG_V1')
  
  df.write.format("hive").saveAsTable("db_tblname")
  
