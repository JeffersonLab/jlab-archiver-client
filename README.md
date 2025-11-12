#  jlab_archiver_client

A python wrapper package on MYA utilities.

## Overview
This package provides a light wrapper on the myquery web service and makes it convenient to make data accessible for
analysis in Python.  Data is presented in common Pandas data structures.

Currently only the interval and mysampler endpoints are supported.

## Usage

This software can be used as an importable package.

```bash
pip install git+https://github.com/JeffersonLab/jlab_archiver_client.git
```

Then run the following python code as an example.

```python
import jlab_archiver_client as jac
from datetime import datetime, timedelta

# Take samples starting one hour ago, at one minute intervals (60,000 ms), and take 60 samples.
query = jac.MySamplerQuery(start=(datetime.now() - timedelta(hours=1)), interval=60_000, num_samples=60,
                          pvlist=['R123GMES'])
mysampler = jac.MySampler(query)
mysampler.run()
data_df, disconnection_dict, metadata_dict = mysampler.data, mysampler.disconnects, mysampler.metadata

print(data_df.head())
```