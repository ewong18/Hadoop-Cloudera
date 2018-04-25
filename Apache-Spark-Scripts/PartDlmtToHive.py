#Script that combines raw, delimited "part" HDFS directory files
# and saves creates tables in Hive

#Purpose is mainly to show how to combine and read the "part" directories
#In practice, schema would also need to be defined 

import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



if __name__ == "__main__":
	if len(sys.argv) != 4:
		print >> sys.stderr, "Usage: dlmtToHive.py <HDFS source path> <database.desired table name> <delimiter> <infer schema? true/false> <has header? true/false>"
		sys.exit(-1)
		
#NOTE: if the delimiter is "|", you will need to enter the argument as $"|"; if it is a \t, you will neet to enter the argument $"\t"

	path = sys.argv[1]
	tblname = sys.argv[2]
	delimiter = sys.argv[3]

	def run_cmd(args_list):
		proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		s_output, s_err = proc.communicate()
		s_return =  proc.returncode
		return s_return, s_output, s_err 

	#Define schema
	custom_Schema =  StructType([
		StructField("column1", StringType(), True),
		StructField("column2", IntegerType(), False)])
		StructField("column3", DecimalType(18,2), True)])
		
	print ''
	print 'Spark resource allocation...'

	spark = SparkSession.builder.appName("dlmtToHive.py").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	
	try:
		print 'Removing old output file if exists...'
		(ret,out,err) = run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', (path + '/Combined')])
		print '   Deleted old output file.'
	except:
		print '   Old output file did not exist.'

	print ''
	print 'Combining file parts and saving as new output file...'
	spark.sparkContext.textFile(path + '/part*').coalesce(1).saveAsTextFile(path +'/Combined')
	
	#Optional: Remove old part files to save space
	#(ret,out,err) = run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', (path + '/part*')])
	
	print 'Reading ' + path + ' file...'
	print " (Please wait...)"
	df = spark.read.load((path +'/combined.txt'), format="csv", sep=delimiter, schema=custom_schema)

	print ''
	print 'Dropping table if it exists...'
	spark.sql("DROP TABLE IF EXISTS " + tblname)

	print ''
	print 'Saving DataFrame to Hive table ' + tblname 
	print " (Please wait...)"
	df.write.format("hive").saveAsTable(tblname)

	print ' '
	print 'Complete'

