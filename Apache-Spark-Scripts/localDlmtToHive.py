#Generic script that takes raw, delimited file from local filesystem, 
# puts it into HDFS, and runs Spark command to create Hive table

#Demonstrates how to run command line prompt from within a Spark script

import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
if __name__ == "__main__":
	if len(sys.argv) != 6:
		print >> sys.stderr, "Usage: localDelimitedToHive.py <local source path> <desired table name> <delimiter> <inferSchema?> <has header?>"
		sys.exit(-1)

#NOTE: if the delimiter is "|", you will need to enter the argument as $"|"; if it is a \t, you will neet to enter the argument $"\t"
		
	sourcePath = sys.argv[1]
	tblname = sys.argv[2]
	delimiter = sys.argv[3]
	schema = sys.argv[4]
	hasHeader = sys.argv[5]

	#Function to run HDFS -put command
	def run_cmd(args_list):
		proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		s_output, s_err = proc.communicate()
		s_return =  proc.returncode
		return s_return, s_output, s_err 
	
	print 'Copying local file to HDFS...'
	(ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', '-f', sourcePath, '<hdfs path>'])
	
	#get filename and create HDFS source path
	tempFilePath = sourcePath.split('/')
	filenameIDX = len(tempFilePath) -1
	filename = tempFilePath[filenameIDX]
	hdfsSource = '<hdfs path>' + filename
	
	print ''
	print 'Spark resource allocation...'

	spark = SparkSession.builder.appName("dlmtToHive.py").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	print 'Reading file: ' + filename + '...'
	print " (Please wait...)"
	df = spark.read.load(hdfsSource, format="csv", sep=delimiter, inferSchema = schema, header = hasHeader)

	print ''
	print 'Dropping table if it already exists...'
	spark.sql("DROP TABLE IF EXISTS " + tblname)

	print ''
	print 'Saving DataFrame to Hive table as ' + tblname 
	print " (Please wait...)"
	df.write.format("hive").saveAsTable(tblname)

	readtmp = spark.sql("SELECT * FROM " + tblname)
	rwcnt = readtmp.count()

	print ' '
	print 'Complete'
