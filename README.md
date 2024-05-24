# Athena Query Utility

## Overview

This Python script provides a utility for executing queries on Amazon Athena and retrieving the results. It includes caching mechanisms to store and retrieve query results locally, which can significantly speed up repeated queries. Additionally, it supports multi-threaded query execution and integration with Jupyter notebooks for interactive use.

## Features

- **Execute Athena Queries**: Run SQL queries on Amazon Athena and retrieve results.
- **Caching**: Save query results to local files to avoid redundant queries.
- **Multi-threading**: Execute queries in separate threads for non-blocking operations.
- **Jupyter Notebook Integration**: Display results directly within Jupyter notebooks.

## Requirements

- Python 3.x
- boto3
- pandas
- ipywidgets (for Jupyter notebook integration)

## Installation

Install the required Python packages using pip:

```sh
pip install boto3 pandas ipywidgets
```

## Usage

### Setting Up AWS Clients

Before running queries, you need to set up Athena and S3 clients using your AWS profile:

```python
from boto3 import Session
from botocore.config import Config

def get_athena_s3_clients(profile_name='your-aws-profile'):
    session = Session(profile_name=profile_name)
    athena = session.client('athena', config=Config(region_name='us-west-2', signature_version='v4'))
    s3 = session.client('s3', config=Config(region_name='us-west-2', signature_version='v4'))
    return athena, s3

athena_client, s3_client = get_athena_s3_clients(profile_name='your-aws-profile')
```

### Running a Query

Use the `athena_query` function to execute a query. Optionally, you can enable caching to store and retrieve query results locally.

```python
from functools import partial

query = partial(
    athena_query,
    database='your-database',
    athena=athena_client,
    s3=s3_client,
    workgroup='your-workgroup'
)

result = query("SELECT * FROM your_table LIMIT 10")
print(result)
```

### Using Cache

To use caching, simply run your query as shown above. The script will automatically handle caching based on the SQL query string.

### Running a Query in a Thread

To execute a query in a separate thread and display results in a Jupyter notebook, use the `thread_query` function:

```python
thread = thread_query(query="SELECT count(*) FROM your_table WHERE month=202201")
```

### Functions

- `save_cache(key, value, query_execution)`: Saves query results to a cache file.
- `get_from_cache(key)`: Retrieves query results from a cache file.
- `athena_query(query, database, athena, s3, workgroup=None, output_location=None, print_function=print, use_cache=None)`: Executes a query on Athena and returns the result.
- `thread_query(query)`: Executes a query in a separate thread for non-blocking operations in Jupyter notebooks.
- `get_athena_s3_clients(profile_name='your-aws-profile')`: Sets up and returns Athena and S3 clients using the specified AWS profile.

## Example

```python
# Example usage in a Jupyter notebook

# Initialize clients
athena_client, s3_client = get_athena_s3_clients(profile_name='your-aws-profile')

# Partial function with common parameters
query = partial(
    athena_query,
    database='your-database',
    athena=athena_client,
    s3=s3_client,
    workgroup='your-workgroup'
)

# Run a query
result = query("SELECT * FROM your_table LIMIT 10")
print(result)

# Run a threaded query in a Jupyter notebook
thread = thread_query(query="SELECT count(*) FROM your_table WHERE month=202201")
```

## Author

Michael Permana (mpermana@hotmail.com)
