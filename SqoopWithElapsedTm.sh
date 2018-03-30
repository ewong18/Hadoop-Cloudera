STARTTIME=$(date +%s)

DB_Connect="<connection string>"
DB_User="<database username>"
DB_PW="<database pw>"
HDFS_Path="<target dir>"
Src_DB="<database name>"

Object_NM="<table name>"
Split_Field="<field name> "
Object_Ref="$Src_DB.$Object_NM"
Target_Dir="$HDFS_Path$Object_NM"
sqoop import --$DB_Connect --username $DB_User -password $DB_PW --table $Object_Ref --fields-terminated-by "^" --null-non-string '\\N' --null-string '\\N' --delete-target-dir --target-dir $Target_Dir -m 1


ENDTIME=$(date +%s)
DIFF=$(($ENDTIME - $STARTTIME))
echo "Time elapsed (seconds): $DIFF"
