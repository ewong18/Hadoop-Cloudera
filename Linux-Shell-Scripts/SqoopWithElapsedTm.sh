#This script uses Sqoop to sqoop data from an existing table
#from sources like Oracle or Teradata into HDFS
#Displays time elapsed for job in seconds
#Oracle Connenction string: jdbc:oracle:thin:@<HostName>:<Port>/<ServiceName>
#Teradata Connection String: jdbc:teradata://<DatabaseServerName>/DATABASE=<database name><,CHARSET=UTF8>

STARTTIME=$(date +%s)

#This would be hardcoded based on your source
DB_Connect="<connection string>"
DB_User="<database username>"
DB_PW="<database pw>"
HDFS_Path="<target dir>"
Src_DB="<database name>"

#All that would need to be changed here is the Object_NM and the Split_Field variables
#as well as the delimiter, if necessary
Object_NM="<table name>"
Split_Field="<field name> "
Object_Ref="$Src_DB.$Object_NM"
Target_Dir="$HDFS_Path$Object_NM"
sqoop import --$DB_Connect --username $DB_User -password $DB_PW --table $Object_Ref --fields-terminated-by "^" --null-non-string '\\N' --null-string '\\N' --delete-target-dir --target-dir $Target_Dir -m 4


ENDTIME=$(date +%s)
DIFF=$(($ENDTIME - $STARTTIME))
printf 'Time elapsed: %02dh:%02dm:%02ds\n' $(($DIFF/3600)) $(($DIFF%3600/60)) $(($DIFF%60))
