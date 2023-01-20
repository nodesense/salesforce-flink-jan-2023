$FLINK_HOME/bin/flink  savepoint  -m localhost:8282   43530bafb42c4ef5211e42ea65f5c8b3 hdfs://namenode:9000/savepoints/kafka-myapp-name/jan-20-2023-3-22-pm

$FLINK_HOME/bin/flink  stop  -m localhost:8282   43530bafb42c4ef5211e42ea65f5c8b3 --savepointPath hdfs://namenode:9000/savepoints/kafka-myapp-name/stop/jan-20-2023-3-33-pm


$FLINK_HOME/bin/flink  run -p 4 -m localhost:8081 -py /home/training/flink-dev/flink/DataTableAPI.py

 $FLINK_HOME/bin/flink run -s  hdfs://namenode:9000/savepoints/kafka-myapp-name/jan-20-2023-3-22-pm/savepoint-43530b-bfb8b7aa6f59  -m localhost:8282  -py   /home/training/fastdata-stack/flink/code/KafkaHelloWorld.py

 bin/flink stop --type [native/canonical] --savepointPath [:targetDirectory] :jobId


hdfs://namenode:9000/savepoints/kafka-myapp-name/jan-20-2023-3-22-pm/savepoint-43530b-bfb8b7aa6f59

https://github.com/nodesense/fastdata-stack/blob/main/flink/code/KafkaHelloWorld.py


https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/JDBC_Catalog.ipynb


hdfs://namenode:9000/data/1k-dataset.csv



hdfs dfs -ls hdfs://namenode:9000/
hdfs dfs -ls hdfs://namenode:9000/data


hdfs dfs -put /data  hdfs://namenode:9000/
hdfs dfs -ls hdfs://namenode:9000/data
hdfs dfs -cat hdfs://namenode:9000/data/1k-dataset.csv
--





$FLINK_HOME/bin/start-cluster.sh 

-- local cluster
$FLINK_HOME/bin/sql-client.sh -m localhost:8282

--docker ha proxy mapped to one of the active job manager 
$FLINK_HOME/bin/sql-client.sh -m localhost:8181

SET sql-client.execution.result-mode=TABLEAU;

CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = 'ecommerce',
    'username' = 'team',
    'password' = 'team1234',
    'base-url' = 'jdbc:mysql://localhost:3306'
);




--------



USE CATALOG my_catalog;

SHOW CATALOGS;

SHOW DATABASES;

SHOW TABLES;

SELECT * FROM products;

======


$FLINK_HOME/bin/sql-client.sh

SET sql-client.execution.result-mode=TABLEAU;

CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = 'ecommerce',
    'username' = 'team',
    'password' = 'team1234',
    'base-url' = 'jdbc:mysql://localhost:3306'
);

USE CATALOG my_catalog;

SHOW CATALOGS;

SHOW DATABASES;

SHOW TABLES;

SELECT * FROM person;

https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/C071_MySqlAccess.ipynb


https://github.com/nodesense/fastdata-stack/blob/main/flink/notes/mysql.md



pip install apache-flink==1.15.3
source /home/training/flink-dev/bin/activate


$FLINK_HOME/bin/flink  run -m localhost:8181 -py /home/training/fastdata-stack/flink/code/HelloWorld.py



c


http://65.109.180.163:8081


# to run this in flink cluster, use below command
# $FLINK_HOME/bin/flink  run -m localhost:8081 -py /home/training/flink-dev/flink/HelloWorld.py

from pyflink.table import EnvironmentSettings, TableEnvironment

env = EnvironmentSettings.in_batch_mode()

#   create table environment
table_env = TableEnvironment.create(env)

data = [ ("Joe", 28), ("Mary", 34), ("Venkat", 40) ] # 3 records
columns = ["name", "age"]
# create virtual table or view
employee_table = table_env.from_elements(data, columns)

employee_table.print_schema()

print("before")
 
print(table_env.list_tables())
table_env.register_table("employees2", employee_table)

print ("after register")
print(table_env.list_tables())

#--

from pyflink.table import DataTypes, CsvTableSource, StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

import datetime
from pyflink.table.expressions import col
from pyflink.table.window import Over, GroupWindow
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE
from pyflink.table.udf import udf

env_settings = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(stream_execution_environment=env_settings)
https://github.com/nodesense/fastdata-stack




wget  -P $FLINK_HOME/lib https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.15.3/flink-parquet-1.15.3.jar

wget  -P $FLINK_HOME/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/1.15.3/flink-sql-parquet-1.15.3.jar


https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/S005-CsvToParquet.ipynb


file:////home/training/flink-dev/data/ecommerce-clean.json


'file:////home/training/flink-dev/data/ecommerce-clean.csv'

https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/S007-CSVToJson.ipynb


https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/S003-EcommerceAnalytics.ipynb


/home/training/flink-dev

https://github.com/nodesense/cts-flink-2022/blob/main/data/ecommerce-clean.zip


https://github.com/nodesense/cts-flink-2022/blob/main/notebooks2/S002_TableCSVAPI.ipynb


https://github.com/nodesense/cts-flink-2022/tree/main/data


http://trainingsf-12:8888/lab?token=bcef6c74c934aedc75b5405d00644a661d57eda0b04e0ded
http://training20:8889/lab?token=aaa1635a9fbddd82c6a8ef0c4574216270b67a8cebad27c6
