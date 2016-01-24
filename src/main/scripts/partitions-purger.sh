#!/usr/bin/env bash

if [ "$(whoami)" != "hdfs" ]
then
    echo "Must be run as hdfs user!"
    exit 125
fi

if [ -z $1 ]
then
   echo "Usage: $0 table_name"
   exit
fi

table=$1
#table=zefix_sogc

hive -e "show partitions ${table}" | while read line;
do
  spec=$(echo $line | sed -e "s/\//',/g" -e "s/=/='/g");
  echo "-->$spec'<--";
  hdfslocation=$(hive -e "describe formatted ${table} partition(${spec}')" | grep "Location:" | sed -e "s/Location:\s*//");
  hdfs dfs -test -d $hdfslocation || echo "ALTER TABLE ${table} DROP PARTITION(${spec}');" | tee -a /tmp/hive-sync-${table};
done

echo "Here is all the DDL commands to run to purge partitions for table ${table}"

cat /tmp/hive-sync-${table}
