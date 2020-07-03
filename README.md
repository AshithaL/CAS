# CAS
CAS(Covid Analysis Service) collects data and generates reports based on the covid data. The report contains information on COVID-19 cases such as cases per state and also analyzing the data. 

# Prerequisites

python3+

grafana

kibana

airflow

# steps to run

-In terminal
```
python3 src/connect.py
```
-This file will run all the functions required. Like to fetch the data from the API and store it in a csv file. Then dump the csv file to hadoop and from there dump the hadoop data to mysql database tables.

-start grafana server
```
sudo systemctl start grafana-server
```
- connect to mysql db
1. Select datasource option.
2. Serach for mysql and add it.
3. Enter the credentials and details like db name,table to connect with mysql db.
4. Visualize the data accordingly.

- Run the elastic search server 
```
sudo -i service elasticsearch start
```
-Run kibana server
```
sudo service kibana start
```


