# $FLINK_HOME/bin/flink  run -m localhost:8081 -py /home/training/flink-dev/flink/DataTableAPI.py

# we can set parallelism in command line or in code

# $FLINK_HOME/bin/flink  run -p 4 -m localhost:8081 -py /home/training/flink-dev/flink/DataTableAPI.py

# $FLINK_HOME/bin/flink  run -m localhost:8181 -py /home/training/fastdata-stack/flink/code/DataTableAPI.py

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
import datetime
from pyflink.table.expressions import col
from pyflink.table.window import Over, GroupWindow
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE
from pyflink.table.udf import udf
# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
# 536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/01/2010 8:26,2.55,17850,United Kingdom
column_names = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']

column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE(), 
                DataTypes.STRING(), DataTypes.DOUBLE(),DataTypes.STRING(), DataTypes.STRING()]

source = CsvTableSource(
   'hdfs://namenode:9000/data/1k-dataset.csv',  
    column_names,
    column_types,
    ignore_first_line=True,
    quote_character='"',
    lenient=True
)

# source is data source -origin data is from
# table is flink api
# sink is target where data goes to after transformation
table_env.register_table_source('invoices', source)

# invoices is a table
# get table from source
invoices = table_env.from_path('invoices')


##############################
print('\nRegistered Tables List')
print(table_env.list_tables())

print('\nFinancial Trxs Schema')
invoices.print_schema()

# UDF - User Defined Function, python/scala/java
# UDF code is not optimized by flink, python udf shall run on Python VM ie python runtime
def convertDateFormat2(input):
    # format
    format = '%d/%m/%Y %H:%M'
  
    # convert from string format to datetime format
    dt = datetime.datetime.strptime(input, format)
    new_format = '%Y-%m-%d %H:%M:%S' 
    return dt.strftime(new_format)

# creating a python function as UDF, first parameter is date string, return type is date string
# Since function has input(s)/multiple args with data types, and the function return output with datatype
# the middle one, [DataTypes.STRING()] is for input arg(s), input arg is basically a string type
# last one  DataTypes.STRING() is RETURN type from the function , only one return value
convertDateFormat = udf(convertDateFormat2, [DataTypes.STRING()], DataTypes.STRING())

# convert data time string to SQL date time string format as new column CInvoiceDate
# drop Orignal InvoiceDate which is string 
# Cast CInvoiceDate which is SQL date time stirng to TIMESTAMP(3) 3 means precision and 
# create new Column InvoiceDate which TIMESTAMP type
# drop temp col CInvoiceDate
invoices2 = invoices.add_columns(convertDateFormat(col('InvoiceDate')).alias('CInvoiceDate'))\
                    .drop_columns(col('InvoiceDate'))\
                    .add_columns(col("CInvoiceDate").cast(DataTypes.TIMESTAMP(3)).alias("InvoiceDate"))\
                    .drop_columns(col('CInvoiceDate'))\
                    .add_columns( (col("Quantity") * col("UnitPrice")).alias("Amount"))

invoices2.print_schema()

table_env.register_table('invoices2', invoices2)
 

result = table_env.sql_query("""
SELECT InvoiceNo, sum(Quantity)  as Qty, sum(Amount) as TotalAmount, 
        count(InvoiceNo) as UniqueItems
        
FROM invoices2
GROUP BY InvoiceNo
""")

#result.fetch(5).execute().print()
result.print_schema()

# source table
table_env.register_table(  "analytics_results_source", result)

#result.execute().print()

#print(result.explain())

# format is json, json jar already present in lib directory, loaded into memory by default
sink_ddl_parquet=f"""
    create table `EComAnalyticsResults`(
                 InvoiceNo STRING,
                   
                   Qty DOUBLE,
                   TotalAmount DOUBLE,
                   UniqueItems BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
         'path' = 'hdfs://namenode:9000/data/ecommerce-analytics-results.csv'
    )
    """

table_env.execute_sql(sink_ddl_parquet)

table_env.execute_sql("INSERT INTO EComAnalyticsResults select * from analytics_results_source")
result.execute_insert("EComAnalyticsResults")