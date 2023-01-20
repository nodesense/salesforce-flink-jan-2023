hdfs dfs -ls hdfs://namenode:9000/
hdfs dfs -ls hdfs://namenode:9000/data


hdfs dfs -put /data  hdfs://namenode:9000/
hdfs dfs -ls hdfs://namenode:9000/data
hdfs dfs -cat hdfs://namenode:9000/data/1k-dataset.csv
