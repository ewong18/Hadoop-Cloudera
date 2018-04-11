NOTES TO SELF:

>>Linux CLI<<
Show listing in HDFS directory with file sizes:
hdfs dfs -du <path>

deleting non-empty directory:
hdfs dfs -rm -r -skipTrash <path>

View content in hdfs file
hdfs dfs -cat  <path>

Run shell script
/bin/bash resetFolders.sh

Create script in VIM
vim filename

Exit and save VIM
esc + ZZ (shift)

Exit without save in VIM
esc + :q!

View processes
ps

Kill processes
kill <processID>

Kill processes (if kill doesn't work)
kill -9 <processID>

View processes listening on port
netstat -tulpn

Run spark application
spark2-submit <path>

Append all output to log
&>>path/to/log.txt

>>Spark Shell<<
Start Spark2:
pyspark2

Open .py script in pyspark2
execfile('path')
