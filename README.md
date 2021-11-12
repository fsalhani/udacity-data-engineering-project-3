# Data Warehouse

## Project Contents

* [dwh.cfg](dwh.cfg) - configuration file with information on data location and Redshift connection parameters (in the repository, I left a fake password and host to avoid outside connections)
* [sql_queries.py](sql_queries.py) - file created based on the project's template that contains the queries for drops, creates, copies and inserts
* [create_tables.py](create_tables.py) - file created based on the project's template that resets the tables in the Redshift data warehouse (always run before `etl.py`)
* [etl.py](etl.py) - file created based on the project's template that processes the data from S3 into staging tables and inserts into the analytical tables 
* [run_etl.sh](run_etl.sh) - simple shell script to execute `create_tables.py`, then `etl.py`
* [dashboards](dashboards) - examples of analysis that could be performed in the data provided with dashboards and queries

## Execution guide

Follow these steps to run the scripts for the project
1. Clone the repository
```shell
git clone https://github.com/fsalhani/udacity-data-engineering-project-3.git
```

2. Edit the config file [dwh.cfg](dwh.cfg) and enter the credentials for your Redshift DWH

3. To execute the scripts in this project, there are two options:
Run each python script in order;
```shell
chmod +x run_etl.sh
./run_etl.sh
```
or run the [run_etl.sh](run_etl.sh) script.
```shell
python3 create_tables.py
python3 etl.py
```
