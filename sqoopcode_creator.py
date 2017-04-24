#!/usr/bin/env python
# -*- coding: utf-8 -*-

import vertica_python

# коннектимся к базе
conn_vert = vertica_python.connect(host="localhost", port=5433,
                                   user="user", password="password", database="database")
cur_vert = conn_vert.cursor()

# достаём названия всех таблиц в базе в различных схемах
sql_name = """SELECT DISTINCT
                  anchor_table_schema,
                  anchor_table_name
              FROM v_monitor.column_storage
              ORDER BY 1, 2"""
cur_vert.execute(sql_name)
result = cur_vert.fetchall()

message = ''
# full - таблицы, которые надо заливать целиком в DB
full = []  # [['schema_name', 'table_name']]
res = []

# res_2 - таблицы, у которых несколько полей, похожих на дату
# выбираем нужное поле для разбиения на партиции и хардкодим
res_2 = [('table_name', 'date_column'),
         ('table_name', 'date_column'),
         ('table_name', 'date_column'),
         ('table_name', 'date_column')]

# по всем найдённым табличкам из DB достаём названия столбцов с типом данных, похожим на date (для заливки партициями)
for strings in result:
    schema, table = strings
    # name = "{}.{}".format(schema, table)

    sql_date = """SELECT DISTINCT
                      column_name
                  FROM columns
                  WHERE table_schema = '{schema}' AND table_name = '{table}'
                      AND data_type IN ('timestamp', 'timestamptz', 'date(11)', 'date')
                      AND table_schema != 'dictionaries'
                  ORDER by 1""".format(schema=schema, table=table)

    cur_vert.execute(sql_date)
    result = cur_vert.fetchall()

    # если столбец с датой один, то пишем в res
    if result:
        if len(result) == 1:
            res_tmp = schema, table, result[0][0]
            res.append(res_tmp)
    else:
        full.append([schema, table])

# собираем рукописный список с автоматическим
# получится один список таблиц, которые будут выгружаться партициями
res = res + res_2

# две строки для минимальной и максимальной даты в столбце по которому заливаем (для цикла)
max_date = ''
min_date = ''
part_table = []

# генерируем bash скрипт для переливки партициями
for strings in res:
    schema, table, column = strings
    name = '{}.{}'.format(schema, table)

    sql_min = """SELECT MIN({}) FROM {};""".format(column, name)
    cur_vert.execute(sql_min)
    min_dat = cur_vert.fetchall()
    min_dat = min_dat[0][0]
    min_dat = min_dat.strftime('%Y-%m-%d')

    sql_max = """SELECT MAX({}) - 1 FROM {};""".format(column, name)
    cur_vert.execute(sql_max)
    max_dat = cur_vert.fetchall()
    max_dat = max_dat[0][0]
    max_dat = max_dat.strftime('%Y-%m-%d')

    bash_part_str = """#! /bin/bash

end_date=$(date -d '{max_dat}' +'%Y%m%d')
start_date=$(date -d '{min_dat}' +'%Y%m%d')
gen_path=$'/folder/hdfs_schema_name/{table}/date_part='
while [[ $start_date -lt $end_date ]]
do
echo ------------------------------------------------------- ${start_date}
path=$gen_path$start_date
start_date=$(date +%Y%m%d -d "$start_date")
sqoop import -libjars vertica-jdbc-8.0.1-0.jar --driver com.vertica.jdbc.Driver \
--connect jdbc:vertica://localhost:5433/database --username username --password password \
--query "SELECT * FROM {name} WHERE \$CONDITIONS AND trunc({column}) = to_timestamp('${start_date}', 'YYYYMMDD')" \
--where "{column} = TO_DATE($start_date, 'YYYYMMDD')" --as-textfile \
--delete-target-dir --target-dir $path \
-m 3 --split-by {column} \
--hive-import --hive-table hive_schema_name.{table}
start_date=$(date +%Y%m%d -d "$start_date + 1 day")
done

echo 'done'
""".format(max_dat=max_dat, min_dat=min_dat, table=table, name=name, column=column, start_date='{start_date}')
    
    path_file = '/home/scripts/bash/partition/to_hdfs_{table}.sh'.format(table=table)
    f = open(path_file, 'w')
    f.write(str_1 + '\n')
    
    part_table.append(bash_part_str)
    print(bash_part_str)
    print('=' * 80)

# генерируем bash скрипт для загрузки таблиц целиком
full_table = []
for strings in full:
    schema, table = strings
    name = '{}.{}'.format(schema, table)
    sqoop_str = """sqoop import -libjars vertica-jdbc-8.0.1-0.jar --driver com.vertica.jdbc.Driver \
    --connect jdbc:vertica://localhost:5433/database --username username password password \
    --query 'SELECT * FROM {name} WHERE $CONDITIONS' --as-textfile \
    --delete-target-dir --target-dir '/folder/database_name/folder/{table}'  \
    --split-by {column} -m 7 --hive-import --hive-database database_name --hive-table {table}""".format(
        name=name, table=table.lower(), column=column)
    full_table.append(sqoop_str)
    
    path_file = '/home/scripts/bash/full/to_hdfs_{table}.sh'.format(table=table)
    f = open(path_file, 'w')
    f.write(str + '\n')
    
    print(sqoop_str)
    print('-' * 80)
