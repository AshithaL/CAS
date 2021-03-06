import datetime
import subprocess
import logging
import os
from subprocess import Popen, PIPE
import requests
import yaml
import csv
import json

from src.utils.constants import YAMLPATH

stream = open(YAMLPATH, 'r')
data = yaml.load(stream)
db = data['db']
table_name = data['table_name']
username = data['username']
password = data['password']

date = datetime.datetime.today().strftime('%Y-%m-%d')
file_name = '/home/nineleaps/CAS/src/Data/data{}.csv'.format(
    datetime.datetime.today().strftime('%Y-%m-%d'))

logging.basicConfig(filename="/home/nineleaps/CAS/src/logFilecovid.txt",
                    filemode='a',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s-%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def fetch_covid_state_data():
    """
    this functtion is used to fetch the api data and store it in a csv file locally.
    :return:
    """
    try:
        req = requests.get('https://api.covidindiatracker.com/state_data.json')
        logging.info('Request was successful')
        url_data = req.text
        data = json.loads(url_data)
        covid_data = []
        for state in data:
            covid_data.append([date, state.get('state'), state.get('active')])
        with open(file_name, "w") as f:
            writer = csv.writer(f)
            writer.writerows(covid_data)
        logging.info('New csv created')
    except requests.exceptions.Timeout:
        logging.error("Connection time out")
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects")
    except requests.exceptions.RequestException as e:
        logging.critical('Critical error occurred during request')
        raise SystemExit(e)


def run_cmd(args_list):
    """
    This function is used to run linux commands
    :param args_list:
    :return:
    """
    try:
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        s_output, s_err = proc.communicate()
        s_return = proc.returncode
        logging.info('')
        return s_return, s_output, s_err
    except requests.exceptions.Timeout:
        logging.error("Connection time out")
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects")
    except requests.exceptions.RequestException as e:
        logging.critical('Critical error occurred during request')
        raise SystemExit(e)


def hdfs_job():
    """
    This function is used to dump the csv file to hadoop inside user folder
    :return:
    """
    try:
        cmd = ['hdfs', 'dfs', '-copyFromLocal', '/home/nineleaps/CAS/src/Data/data{}.csv'.format(date),
               '/user/covid_data_2020-07-04.csv']

        print(cmd)
        (ret, out, err) = run_cmd(cmd)
        print(ret, out, err)
        if ret == 0:
            logging.info('Successfuly dump data in hdfs.')
        else:
            logging.info('Error.')
    except requests.exceptions.Timeout:
        logging.error("Connection time out")
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects")
    except requests.exceptions.RequestException as e:
        logging.critical('Critical error occurred during request')
        raise SystemExit(e)


# Create Sqoop Job
def sqoop_job():
    """
    This function is used to dump the csv file data stored in hadoop to mysql table using sqoop job.
    :return:
    """

    try:
        os.system(
            "sqoop export   --table {}  --connect jdbc:mysql://localhost:3306/{}   --username {}   --password "
            "{}   --export-dir /user/covid_data_{}.csv   --columns dt,state,cases".format(table_name,
                                                                                          db,
                                                                                          username,
                                                                                          password, date))
        logging.info("sucessfully dumped in mysql")
    except requests.exceptions.Timeout:
        logging.error("Connection time out")
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects")
    except requests.exceptions.RequestException as e:
        logging.critical('Critical error occurred during request')
        raise SystemExit(e)



