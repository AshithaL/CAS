import csv
import datetime
import json
import logging
import os
from subprocess import Popen, PIPE

import requests
#
# date = datetime.datetime.today().strftime('%Y-%m-%d')
file_name = "/home/nineleaps/PycharmProjects/CAS/src/Data/data{}.csv".format(datetime.date)
# req = requests.get('https://api.covidindiatracker.com/state_data.json')
# url_data = req.text
# data = json.loads(url_data)
# covid_data = [['date', 'state', 'active_cases']]
# for state in data:
#     covid_data.append([date, state.get('state'), state.get('active')])
#     with open(file_name, "w") as f:
#         writer = csv.writer(f)
#         writer.writerows(covid_data)
#


def fetch_covid_state_data():
    try:
        req = requests.get('https://api.covidindiatracker.com/state_data.json')
        logging.info('Request was successful')
        url_data = req.text
        data = json.loads(url_data)
        covid_data = []
        for state in data:
            covid_data.append([datetime.date, state.get('state'), state.get('active')])
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


