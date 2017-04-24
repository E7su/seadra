#!/bin/bash

sqoop import -libjars vertica-jdbc-8.0.1-0.jar --driver com.vertica.jdbc.Driver \
    --connect jdbc:vertica://localhost:5433/database --username username password password \
    --query 'SELECT * FROM schema.table WHERE $CONDITIONS' --as-textfile \
    --delete-target-dir --target-dir '/folder/database_name/folder/table'  \
    --split-by column -m 7 --hive-import --hive-database database_name --hive-table table
