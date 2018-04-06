import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
	if len(sys.argv) != 6:
		print >> sys.stderr, "Usage: dlmtToHive.py <source path> <desired table name> <delimiter> <infer schema? true/false> <has header? true/false>"
		sys.exit(-1)
		
#NOTE: if the delimiter is "|", you will need to enter the argument as $"|"; if it is a \t, you will neet to enter the argument $"\t"

	path = sys.argv[1]
	tblname = sys.argv[2]
	delimiter = sys.argv[3]
	schema = sys.argv[4]
	hasHeader = sys.argv[5]

	print ''
	print 'Spark resource allocation...'

	spark = SparkSession.builder.appName("dlmtToHive.py").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	print 'Reading ' + path + ' file...'
	print " (Please wait...)"
	df = spark.read.load(path, format="csv", sep=delimiter, inferSchema = schema, header = hasHeader)

	print ''
	print 'Dropping table if it exists...'
	spark.sql("DROP TABLE IF EXISTS + tblname)
  
	print ''
	print 'Saving DataFrame to Hive table ' + tblname 
	print " (Please wait...)"
	df.write.format("hive").saveAsTable(tblname)

	print ' '
	print 'Complete'
