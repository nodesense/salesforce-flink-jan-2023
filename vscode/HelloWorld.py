# to run this in flink cluster, use below command
# shall run flink client application
# internally, flink client appplication shall submit job to job manager as DFG
# job manager shall use task manager to execute tasks
# $FLINK_HOME/bin/flink  run -m localhost:8081 -py /home/training/flink-dev/flink/HelloWorld.py

# $FLINK_HOME/bin/flink  run -m localhost:8181 -py /home/training/fastdata-stack/flink/code/HelloWorld.py

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

# parsing Table API/SQL Query is part of Flink client,  from this, it will DFG when we submit job
table2 = table_env.sql_query("SELECT name,  age from employees2")
table2.print_schema()

# DFG creation, optimization all happens within flink client
print(table2.explain()) 

# until now, we used meta operations, like data source, creating/registering tables, schema etc
# until what we done all run inside flink client, this doesn't submit any jobs/tasks to flink cluster
# job manager is not until now...

# now, the flink code, shall create an action, that will create DFG internally, and 
# submit DAG for execution as job, this means, Flink Manager must exeucte a job, dashboard must be updated


# SQL API works with temp view, temp table, table registered in table environment
# SQL is parsed, converted into AST, then generate Data Flow Graph
table3 = table_env.sql_query("SELECT name,age FROM employees2 WHERE age <= 30")
table3.print_schema()
# execute is an action, this will create DFG, submit the job/DFG to Flink job manager
# every job has unique job id, you can use this to terminate job, to create save point,..
# Job has been submitted with JobID 9cb25385772c5bc2bff95bd6574eb5fc
table3.execute().print() 
# to_pandas is action, this runs/executes the Query on Job/Task manager
# Job has been submitted with JobID 7fec1ae09e82e26094d9aade24b80ccb
table3.to_pandas() # last cell expression printed

# this create again new action even though we already have same code executed
table3.execute().print() 


