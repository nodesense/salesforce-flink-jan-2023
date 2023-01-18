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
