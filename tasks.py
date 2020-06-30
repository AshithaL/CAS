import urllib.request as request
import json
import pandas as pd
from cerberus import Validator
from airflow.hooks.mysql_hook import MySqlHook

LOCAL_DIR = '/tmp/'


def validate_data():
    with request.urlopen('https://api.covid19india.org/data.json') as response:
        source = response.read()
        data = json.loads(source)
        statewise_dict = data['statewise']
        v = Validator()
        v.schema = {'active': {'required': True, 'type': 'string'},
                    'confirmed': {'required': True, 'type': 'string'},
                    'deaths': {'required': True, 'type': 'string'},
                    'recovered': {'required': True, 'type': 'string'},
                    'deltaconfirmed': {'required': True, 'type': 'string'},
                    'deltadeaths': {'required': True, 'type': 'string'},
                    'deltarecovered': {'required': True, 'type': 'string'},
                    'lastupdatedtime': {'required': True, 'type': 'string'},
                    'migratedother': {'required': True, 'type': 'string'},
                    'statecode': {'required': True, 'type': 'string'},
                    'statenotes': {'required': True, 'type': 'string'},
                    'state': {'required': True, 'type': 'string'},
                    }
        for item in statewise_dict:
            if not v.validate(item):
                print(v.errors)
                raise ValueError('API Data Not Valid')
        print('API Data is valid')

        df = pd.DataFrame(statewise_dict, columns=['active', 'confirmed', 'deaths', 'recovered', 'state'])
        df.to_csv(LOCAL_DIR + 'covidData_fetched.csv', index=False)


def create_table():
    request = "create table covid.corona(active varchar(50),confirmed varchar(50),deaths varchar(50),recovered varchar(50),state varchar(30));"
    mysql_hook = MySqlHook(mysql_conn_id="mysql", schema="covid")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    return sources

