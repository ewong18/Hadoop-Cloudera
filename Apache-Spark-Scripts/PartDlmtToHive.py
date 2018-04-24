#Generic script that combines raw, delimited "part" HDFS directory files
# and saves creates tables in Hive

import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



if __name__ == "__main__":
	if len(sys.argv) != 6:
		print >> sys.stderr, "Usage: dlmtToHive.py <HDFS source path> <database.desired table name> <delimiter> <infer schema? true/false> <has header? true/false>"
		sys.exit(-1)
		
#NOTE: if the delimiter is "|", you will need to enter the argument as $"|"; if it is a \t, you will neet to enter the argument $"\t"

	path = sys.argv[1]
	tblname = sys.argv[2]
	delimiter = sys.argv[3]
	schema = sys.argv[4]
	hasHeader = sys.argv[5]

	def run_cmd(args_list):
		proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		s_output, s_err = proc.communicate()
		s_return =  proc.returncode
		return s_return, s_output, s_err 

	print ''
	print 'Spark resource allocation...'

	spark = SparkSession.builder.appName("dlmtToHive.py").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	
	try:
		print 'Removing old output file if exists...'
		(ret,out,err) = run_cmd(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', (path + '/combined.txt')])
		print '   Deleted old output file.'
	except:
		print '   Old output file did not exist.'

	print ''
	print 'Combining file parts and saving as new output file...'
	spark.sparkContext.textFile(path + '/part*').coalesce(1).saveAsTextFile(path +'/combined.txt')
	
	print 'Reading ' + path + ' file...'
	print " (Please wait...)"
	df = spark.read.load((path +'/combined.txt'), format="csv", sep=delimiter, inferSchema = schema, header = hasHeader)

	print ''
	print 'Dropping table if it exists...'
	spark.sql("DROP TABLE IF EXISTS " + tblname)

	print ''
	print 'Saving DataFrame to Hive table ' + tblname 
	print " (Please wait...)"
	df.write.format("hive").saveAsTable(tblname)

	print ' '
	print 'Complete'

