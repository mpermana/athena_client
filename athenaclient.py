'''
Author: Michael Permana (mpermana@hotmail.com)
'''
from boto3 import client
from boto3 import Session
from botocore.config import Config
from datetime import datetime
from functools import partial
from hashlib import md5
from io import BytesIO
from IPython.display import display
from IPython.display import HTML
from ipywidgets import Output
from json import dumps
from time import sleep
from os.path import exists
from pandas import read_csv
from pandas import set_option
from pickle import load, dump
from pprint import pformat
from re import search
from threading import Thread

cache_path = '.cache'

# set jupyter notebooks
set_option('display.max_columns', None)
set_option('display.max_rows', 10000)
# set_option('display.float_format', '${:,.2f}'.format)

# code
def save_cache(key, value, query_execution):
    key_md5 = md5(key.encode()).hexdigest()
    with open(f'{cache_path}/{key_md5}.json', "w") as text_file:
        data = {
            'query_execution': query_execution,
            'sql': key
        }
        print(dumps(data, default=str, indent=4),
              file=text_file)
    with open(f'{cache_path}/{key_md5}.pickle', 'wb') as pickle_file:
        dump(value, pickle_file)

def get_from_cache(key):
    key_md5 = md5(key.encode()).hexdigest()
    filename = f'{cache_path}/{key_md5}.pickle'
    if exists(filename):
        with open(filename, 'rb') as pickle_file:
            return load(pickle_file)
    
def athena_query(
    query,
    database,
    athena,
    s3,
    workgroup=None,
    output_location=None,
    print_function=print,
    use_cache=None
):
    if use_cache is None:
        use_cache = 'select' in query.lower()
    try:
        cache_key = f'--{database}\n{query}'
        if use_cache:
            cache_result = get_from_cache(cache_key)
            if cache_result is not None:
                return cache_result
        ResultConfiguration = {}
        if output_location:
            ResultConfiguration['OutputLocation'] = output_location
        query_execution = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration=ResultConfiguration,
            WorkGroup=workgroup
        )
        start_time = datetime.now()
        query_execution_id = query_execution['QueryExecutionId']
        while True:
            query_execution = athena.get_query_execution(QueryExecutionId=query_execution_id).get('QueryExecution')
            state = query_execution.get('Status', {}).get('State')
            stop_time = datetime.now()
            print_function('Duration: %d' % (stop_time-start_time).total_seconds(), end='\r')
            if state in ['CANCELLED', 'FAILED']:
                raise Exception('%s' % query_execution.get('Status', {}).get('StateChangeReason'))
            if state == 'SUCCEEDED':                
                output_location = query_execution['ResultConfiguration']['OutputLocation']
                _, _, bucket, key = output_location.split('/', 3)
                obj = s3.get_object(Bucket=bucket, Key=key)
                print_function(f'Duration: {(stop_time-start_time).total_seconds()} aws s3 cp s3://{bucket}/{key}')
                if key.endswith('.csv'):
                    result = read_csv(BytesIO(obj['Body'].read()))
                else:
                    if query_execution['StatementType'] == 'DDL':
                        return state
                    content = obj['Body'].read()
                    index = content.find(b'#')
                    if index != -1:
                        content = content[:index]
                    return read_csv(BytesIO(content), sep='\t', header=None)
                save_cache(cache_key, result, query_execution)
                return result
            sleep(2)
    except KeyboardInterrupt:        
        result = athena.stop_query_execution(QueryExecutionId=query_execution_id)
        print_function(f'Query cancelled {query_execution_id} {result}')
    except Exception as e:
        pattern = r'line (\d+):(\d+):'
        match = search(pattern, str(e))
        if match:
            print('Query has error:')
            line_number = int(match.group(1))
            column_number = int(match.group(2))
            for i, line in enumerate(query.split('\n')):
                print(line)
                if i+1 == line_number:
                    print('-'*(column_number-1) + '^')
                    print(e)
        else:
            raise e

def thread_query(query="select count(*) from fact_all_adw_traffic_daily where month=202201"):
    print('Query:', query)
    result = {}
    def thread_function(query, out):
        df = athena_query(query, print_function=out.append_stdout)
        out.append_display_data(df)
        result['df'] = df

    out = Output()
    display(out)

    thread = Thread(
        target=thread_function,
        args=(query, out))
    thread.result = result
    thread.start()
    return thread

def get_athena_s3_clients(profile_name='mobilityware-platform-prod'):
    session = Session(profile_name=profile_name)
    athena = session.client('athena', config=Config(
        region_name='us-west-2',
        signature_version='v4'
    ))
    s3 = session.client('s3', config=Config(
        region_name='us-west-2',
        signature_version='v4'
    ))
    return athena, s3

athena_client, s3_client = get_athena_s3_clients(profile_name='profile-name')

query = partial(
    athena_query,
    database='database',
    athena=athena_client,
    s3=s3_client,
    workgroup='workgroup'
)
