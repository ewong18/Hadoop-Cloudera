NOTES TO SELF:

<b>Linux CLI</b>

<i>Show listing in HDFS directory with file sizes:</i>

hdfs dfs -du <path>

<i>deleting non-empty directory:</i>

hdfs dfs -rm -r -skipTrash <path>

<i>View content in hdfs file:</i>

hdfs dfs -cat  <path>

<i>Run shell script:</i>

/bin/bash resetFolders.sh

<i>Create script in VIM:</i>

vim <filename>

<i>Exit and save VIM:</i>

esc + ZZ (shift)

<i>Exit without save in VIM:</i>

esc + :q!

<i>View processes:</i>

ps

<i>Kill processes:</i>

kill <processID>

<i>Kill processes (if kill doesn't work):</i>

kill -9 <processID>

<i>View processes listening on port:</i>

netstat -tulpn

<i>Run spark application:</i>

spark2-submit <path>

<i>Append all output to log:</i>

&>>path/to/log.txt

<b>Spark Shell</b>

<i>Start Spark2:</i>

pyspark2

<i>Open .py script in pyspark2:</i>

execfile('path')
