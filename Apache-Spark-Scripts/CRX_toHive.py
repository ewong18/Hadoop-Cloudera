import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#This script can read a variety of flat '|' delimited files
#Schema is define within the script for each type of file
#Demonstrates how a column can be dropped
#Demonstrates how a hardcoded column can be created and moved to the front
#Able to take source date from file name
#Appends data to existing table with source date

#Function to check if table exists in Hive database
def tableexists(tablename):
	test = spark.sql("show tables from <dbname> like '"+ tablename+ "'")
	if test.count() == 0:
		return False
	else:
		return True

#Gets data's cycle ID based on filename
def getMo_ID(sourcePath):
	tmpLst1 = path.split('/')
	filename = tmpLst1[(len(tmpLst1)-1)] #file_name_YYYYMMDD.txt
	tmpLst2 = filename.split('_')
	cyc_ext = tmpLst2[(len(tmpLst2)-1)] #YYYYMMDD.txt
	tmpLst3 = cyc_ext.split('.')
	MO_ID = tmpLst3[0] #YYYYMMDD
	return int(MO_ID)

#Checks if there is a columns representing Month ID
def hasMo_ID(schema):
	hasID = False
	for elem in schema:
		if "MO_ID" in elem.name:
			hasID = True
		elif "MONTH_ID" in elem.name:
			hasID = True
	return hasID

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, "Usage: CRX_ToHive.py <HDFS source path>"
		sys.exit(-1)
		
	path = sys.argv[1]
	
	custom_Schema =  StructType([
		StructField("column1", StringType(), True),
		StructField("MO_ID", IntegerType(), False)])
		StructField("column3", DecimalType(18,2), True)])

	if "dataset1" in path:
		headerSchema = custom_Schema
		tblname = "table1"
	#elif... 
		
	spark = SparkSession.builder.appName("CRX_ToHive.py").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	
	print 'Reading file...'
	tempDF1 = spark.read.load(path, format="csv", sep="|", schema = headerSchema)
	
	#filter rows, keeping only those where column1 == 'D'
	tempDF1.createOrReplaceTempView("table0")
	tempDF2 = spark.sql("select * from table0 where column1 == 'D'")
	
	#drop "column1" column
	df = tempDF2.drop("column1")
	
	#hardcode cycle id as MO_ID and add column to DataFrame
	if not hasMo_ID(headerSchema):
		df = df.withColumn("MO_ID", lit(getMo_ID(path)))

	#spark.sql("drop table if exists dbname."+ tblname) //ONLY FOR TESTING
	
	print ''
	print 'Converting table to Hive...'

	if tableexists(tblname):
		df.write.format("hive").insertInto("dbname." +tblname)
		print 'Appended to existing Hive table: dbname.' + tblname
	else:
		df.write.format("hive").saveAsTable("dbname." +tblname)
		print 'Created new Hive table: dbname.' + tblname

	print ''
	print 'Complete'
