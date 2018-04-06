#I'm posting this mainly as example code
#This does not seem to be the cleanest solution

#Originally, this code was written to read messy, unstructured log files
#line-by-line and distribute them into buckets based on specific error types

import sys
import time
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#Puts log into the "INFO" bucket labeled with a timestamp of when the log arrived
def saveInfoRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/INFO--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))

#Puts log into the "DEBUG" bucket labeled with a timestamp of when the log arrived
def saveDebugRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/DEBUG--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))    

#Puts log into the "ERROR" bucket labeled with a timestamp of when the log arrived
def saveErrorRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/ERROR--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))    

#Puts log into the "MISC" bucket labeled with a timestamp of when the log arrived
def saveMiscRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/MISC--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))

#Puts log into the "SIP_FAULT" bucket labeled with a timestamp of when the log arrived
def saveSipFaultRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/SIP_FAULT--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))

#Puts log into the "WS_REJECT" bucket labeled with a timestamp of when the log arrived
def saveWSRejectionRDD(rdd): 
    if rdd.count() > 0:
	    rdd.saveAsTextFile("/path/WS_REJECT--"+datetime.datetime.now().strftime("%Y%m%d-%H.%M.%S.%f"))  

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: StreamingLogs.py <hostname> <port>"
        sys.exit(-1)
    
    # get hostname and port of data source from application arguments
    hostname = sys.argv[1]
    port = int(sys.argv[2])
     
    # Create a new SparkContext
    sc = SparkContext()

    # Set log level to ERROR to avoid distracting extra output
    sc.setLogLevel("ERROR")

    # Create and configure a new Streaming Context 
    # with a 1 second batch duration
    ssc = StreamingContext(sc,1)

    # Create a DStream of log data from the server and port specified    
    logs = ssc.socketTextStream(hostname,port)

    # Filter logs based on error type and run corresponding function to sort into buckets
    SipFaultDS = logs.filter(lambda line: ">SIP FAULT<" in line)
    SipFaultDS.foreachRDD(lambda t,r: saveSipFaultRDD(r))

    WSRejectDS = logs.filter(lambda line: ">WS REJECTION<" in line)
    WSRejectDS.foreachRDD(lambda t,r: saveWSRejectionRDD(r))

    InfoDS = logs.filter(lambda line: "INFO" in line and (">SIP FAULT<" not in line and ">WS REJECTION<" not in line))	
    InfoDS.foreachRDD(lambda t,r: saveInfoRDD(r))
	
    DebugDS = logs.filter(lambda line: "DEBUG" in line)
    DebugDS.foreachRDD(lambda t,r: saveDebugRDD(r))
	
    ErrorDS = logs.filter(lambda line: " ERROR " in line)
    ErrorDS.foreachRDD(lambda t,r: saveErrorRDD(r))
 	
    MiscDS = logs.filter(lambda line: "INFO" not in line and "DEBUG" not in line and "ERROR" not in line)
    MiscDS.foreachRDD(lambda t,r: saveMiscRDD(r))
	
    # Start the streaming context and then wait for application to terminate
    ssc.start()
    ssc.awaitTermination()
