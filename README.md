# Firepad for copy paste don't paste passport

 https://demo.firepad.io/#RkjDdI8F4n

# Day  1 code reference 

  https://github.com/nodesense/cts-flink-2022/tree/main/notebooks2
  

# PyFlink for Local Development with Jupyter
 

```
 python -m venv flink-dev
 cd flink-dev
 source bin/activate
 pip install apache-flink==1.15.3
 pip install jupyterlab
 jupyter-lab --ip="0.0.0.0"
 ```

now check the jupyter link shown on the console, copy paste the same in your browser. 

# If you want to access jupyter from local machine/office machine


open new terminal in Linux

replace training with ip address 

type below command 
```
ifconfig 
```

copy ip from eth0, for example 123.234.32.12

then replace training in the url with the ip address copied 

copy the url from jupyter terminal

```
http://training:8888/lab?token=6e63304036b37d7e4be500463ff5e99fa989b157b4045ce1
```

```
http://123.234.32.12:8888/lab?token=6e63304036b37d7e4be500463ff5e99fa989b157b4045ce1
```

open the url with ip address on your chrome browser from office laptop 


### Hint to know site-packages location
system wide
```
python -m site

python -c 'import site; print(site.getsitepackages())'

```

python -m site --user-site


The flink lib path is here  [example] 

```
/home/krish/flink-dev/lib/python3.8/site-packages/pyflink/lib
```


# Setting up flink single node cluster [1 Job manager, 1 Task manager]

open terminal 

```
cd ~
```

```
wget  https://dlcdn.apache.org/flink/flink-1.15.3/flink-1.15.3-bin-scala_2.12.tgz

tar xf flink-1.15.3-bin-scala_2.12.tgz

sudo rm -rf /opt/flink-1.15.3

sudo mv flink-1.15.3 /opt

sudo chmod 777 -R /opt/flink-1.15.3
```

# update bash rc files

in terminal
```
code ~/.bashrc
```

paste below end of the file


```
export FLINK_HOME=/opt/flink-1.15.3
export CLASSPATH=$CLASSPATH:$FLINK_HOME/lib/*:.
export HADOOP_CLASSPATH=`hadoop classpath`

```


# Start Flink Cluster

```
$FLINK_HOME/bin/start-cluster.sh 
```

# Stop Flink Cluster

```
$FLINK_HOME/bin/stop-cluster.sh 
```
