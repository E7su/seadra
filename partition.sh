#! /bin/bash

end_date=$(date -d '2017-04-24' +'%Y%m%d')
start_date=$(date -d '2013-01-01' +'%Y%m%d')
gen_path=$'/folder/hdfs_schema_name/table/date_part='

while [[ $start_date -lt $end_date ]]
do
echo ------------------------------------------------------- ${start_date}
path=$gen_path$start_date
start_date=$(date +%Y%m%d -d "$start_date")

sqoop import -libjars vertica-jdbc-8.0.1-0.jar --driver com.vertica.jdbc.Driver \
--connect jdbc:vertica://localhost:5433/database --username username --password password \
--query "SELECT * FROM schema_name.table WHERE \$CONDITIONS AND trunc(column) = to_timestamp('${start_date}', 'YYYYMMDD')" \
--where "column = TO_DATE($start_date, 'YYYYMMDD')" --as-textfile \
--delete-target-dir --target-dir $path \
-m 3 --split-by column \
--hive-import --hive-table hive_schema_name.table

start_date=$(date +%Y%m%d -d "$start_date + 1 day")
done
echo 'done'
