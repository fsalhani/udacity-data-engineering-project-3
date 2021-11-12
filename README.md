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
Run each python script in the correct order;
```shell
python3 create_tables.py
python3 etl.py
```
or run the [run_etl.sh](run_etl.sh) script.
```shell
chmod +x run_etl.sh
./run_etl.sh
```

## Query reasoning

### Create tables

#### Staging tables
For the staging tables, there wasn't much to decide while creating the tables. I simply downloaded some samples from S3 and created the tables with the ordered fields. No constraints were applied, because, as those are staging tables, I wish to keep the data close to the raw data from the files.

#### Users table
The user table contains 5 pieces of information: user_id, first and last names, gender and level.
`user_id` was set as the `PRIMARY KEY` and both the `DISTKEY` and `SORTKEY` because it is the key that links this table to other tables.
Even though all the other informations are never NULL, I didn't force gender to be `NOT NULL` in case this information is removed from the user registration in the future.

#### Time table
The time table contains information on the time in which the songs were played.
`start_time` was set as the `PRIMARY KEY` and both the `DISTKEY` and `SORTKEY` because it is the key that links this table to other tables.
All information was set as `NOT NULL`, because the fields are just dimensions related to the time and should never be empty.

#### Songs table
`song_id` was set as the `PRIMARY KEY` and both the `DISTKEY` and `SORTKEY` because it is the key that links this table to other tables.
`song_id`, `title` and `artist_id` were set as `NOT NULL`, because they should always exist as a property of the song. `year` and `duration` were left as nullable in case any songs lacked those pieces of information.
Additionally, `artist_id` was set as a `FOREIGN KEY` referencing the `artists` table

#### Artists table
`artist_id` was set as the `PRIMARY KEY` and both the `DISTKEY` and `SORTKEY` because it is the key that links this table to other tables.
`artist_id` and `name` were set as `NOT NULL` because all artists are expected to have those. Information regarding location and coordinates is nullable because they could be missing.

#### Songplays table
The `songplays` table doesn't have a `PRIMARY KEY`, because Redshift does not require or enforces it. I chose `DISTSTYLE EVEN` to evenly distribute by data through the nodes and optimize parallel processing. I could have used `user_id`, `song_id` or `artist_id` as `DISTKEY`, but I preferred not to do it, because I would be optimizing for a specific kind of query.
I used a `COMPOUND SORTKEY` on time and user, because most queries in a database like this should include time windows.
I wish I could set all the `FOREIGN KEYS` to `NOT NULL`, but unfortunatelly, not all songs in the log_data have a match in the song_data. Therefore, only `user_id` and `start_time` were restricted not to have null values.

### Inserts

For the `artists`, `songs`, `time` and `users` tables, I avoided duplicates by doing two things:
* Using `DISTINCT` in the `SELECT` query from the staging tables
* Joining the data with the target table to ensure that id has not been inserted yet

For the `songplays` table, I created a query joining both staging tables to get all the information needed. As not all songs appeared in the song_data, I had to use a `LEFT JOIN` instead of an `INNER JOIN` to avoid information loss. This resulted in many entries without a `song_id` or `artist_id`.
