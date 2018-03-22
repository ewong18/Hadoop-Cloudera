#1bin/bash

#Put files into HDFS

for filename in </path/*.txt>; do
	hdfs dfs -put -f $filename <dest path>
done

echo File write complete

for fn in `hdfs dfs -ls </path> | awk '{print$NF}' | grep .txt$ | tr '\n' ' '`; do
	spark2-submit </path.py> $fn 2</dev/null
done 
