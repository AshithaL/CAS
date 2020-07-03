import logging
import random
from src import connect as c

"""
This is POC which is created to understand how kibana analyse logs.  
"""
logging.basicConfig(filename="logFilecovid.txt",
                    filemode='a',
                    format='%(asctime)s %(levelname)s-%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
for i in range(0, 15):
    x = random.randint(0, 2)
    if (x == 0):
        logging.warning('Log Message')
    elif (x == 1):
        logging.critical('Log Message')
    else:
        logging.error('Log Message')
