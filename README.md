# Spackl
[![PyPI version](https://img.shields.io/pypi/v/spackl.svg)](https://pypi.org/project/spackl/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/spackl.svg)](https://pypi.org/project/spackl/) [![CircleCI](https://circleci.com/gh/aaronbiller/spackl/tree/master.svg?style=shield)](https://circleci.com/gh/aaronbiller/spackl/tree/master) [![Coverage Status](https://coveralls.io/repos/github/aaronbiller/spackl/badge.svg?branch=master)](https://coveralls.io/github/aaronbiller/spackl?branch=master)

Abstracting data source idiosyncrasies so you can stop reading Q&A forums and start reading your data.

## Installation
```
pip install spackl
```

## Usage Overview
#### Query Data
```python
from spackl import db, file

conf = db.Config()

pg = db.Postgres(**conf.default)
bq = db.BigQuery(**conf.bq_datalake)
csv = file.CSV('/path/to/file.csv')

# Same method for all sources
pg_results = pg.query('SELECT id FROM schema.some_table')
bq_results = bq.query('SELECT id FROM dataset.some_table')
csv_results = csv.query()
```

#### Access Query Results
_by index_
```python
results[0]
# (1234,)
```

_by attribute_
```python
results.id
# (1234, 1235, 1236)
```

_by key_
```python
results['id']
# (1234, 1235, 1236)
```

_index by index_
```python
results[0][0]
# 1234
```

_attribute by index_
```python
results.id[0]
# 1234
```

_key by index_
```python
results['id'][0]
# 1234
```

_index by attribute_
```python
results[0].id
# 1234
```

_index by key_
```python
results[0]['id']
# 1234
```

#### Other Data Formats
```python
# Pandas Dataframe
results.df()

# JSON String
results.json()

# List of tuples
results.list()

# Vertical dictionary
results.dict()
```
