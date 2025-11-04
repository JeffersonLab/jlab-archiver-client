# mya_getter

A python wrapper package on MYA utilities.

## Overview 
A light wrapper on common MYA utilities used at CEBAF.  These wrapped utilities are currently command line executables,
however, this software offers easy parallelization through the use of process pools.  This software also wraps some
limited functionality of the myquery web-API including mysampler.

Supported utilities are mySampler and myData.  These applications must be installed on your local system for
this package to be of much use.

## Usage

This software can be used in one of two ways, as a command line executable or as an importable package.

### Command Line Executable
First clone the repo.  Then set up a virtual environment named venv using the supplied requirements.txt file.  Now
running bin/mya_getter.bash should work.

```tcsh
git clone https://github.com/JeffersonLab/mya_getter.git
cd mya_getter
/usr/csite/pubtools/bin/python3.7 -m venv venv
source venv/bin/activate.csh
pip install -r requirements.txt
```

Example simple mySampler call.  This queries mySampler twice to take five samples of R123GMES, one second apart.  The
first query is at 2022-02-01 and the next query is ten seconds later.  Data is written to test.csv.  Since a process
pool is used, output in the file is chronologically ordered within a query, but not across queries as can be seen below.
```bash
 bin/mya_getter.bash mysampler -b 2022-02-01 -n 5 -i 1s -q 10 --num-queries 2 -o test.csv -p R123GMES

cat test.csv 
level_0,Date,R123GMES
query_0,2022-02-01_00:00:10,6.696
query_0,2022-02-01_00:00:11,6.696
query_0,2022-02-01_00:00:12,6.696
query_0,2022-02-01_00:00:13,6.696
query_0,2022-02-01_00:00:14,6.696
query_1,2022-02-01_00:00:00,6.696
query_1,2022-02-01_00:00:01,6.696
query_1,2022-02-01_00:00:02,6.696
query_1,2022-02-01_00:00:03,6.696
query_1,2022-02-01_00:00:04,6.696
```

### Importable package
You can install this repo directly into your code, then import it as mya_getter.  Use of a virtual environment is
recommended.

```bash
pip install git+https://github.com/JeffersonLab/mya_getter.git
```
Then run the following python code as an example.
```python
import mya_getter as mg
from datetime import datetime, timedelta
query = mg.MySamplerQuery(start=(datetime.now() - timedelta(hours=1)), interval='1m', num_samples='60', pvlist=['R123GMES'])
df = mg.mySampler(query)
print(df.head())
```

This code would output the following.
```
                  Date  R123GMES
0  2022-04-14_13:53:24         0
1  2022-04-14_13:54:24         0
2  2022-04-14_13:55:24         0
3  2022-04-14_13:56:24         0
4  2022-04-14_13:57:24         0

```

Multiple queries can be run in parallel.  Here is an example.
```python
import mya_getter as mg
from datetime import datetime, timedelta
start = datetime.now()
pvlist = ['R121GMES', 'R123GMES']
query1 = mg.MySamplerQuery(start=(start - timedelta(hours=1)), interval='1m', num_samples='3', pvlist=pvlist)
query2 = mg.MySamplerQuery(start=(start - timedelta(hours=2)), interval='1m', num_samples='3', pvlist=pvlist)

df = mg.do_parallel_queries(mg.mySampler, [query1, query2])
print(df.head())
```

This will output the following result.
```python
   level_0                 Date  R121GMES  R123GMES
0  query_0  2022-04-14_13:20:48         0         0
1  query_0  2022-04-14_13:21:48         0         0
2  query_0  2022-04-14_13:22:48         0         0
3  query_1  2022-04-14_14:20:48         0         0
4  query_1  2022-04-14_14:21:48         0         0
```
