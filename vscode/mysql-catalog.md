$FLINK_HOME/bin/start-cluster.sh 

-- local cluster
$FLINK_HOME/bin/sql-client.sh -m localhost:8282

--docker ha proxy mapped to one of the active job manager 
$FLINK_HOME/bin/sql-client.sh -m localhost:8181

SET sql-client.execution.result-mode=TABLEAU;

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

SELECT * FROM products;

