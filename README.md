# eutl_orm
The European Transaction Log (EUTL) is the backbone of the European Union Emissions Trading System (EUETS). 
It implements the transfer of emission allowances between parties active in the EUETS. The EUTL provides data on regulated 
installations, their emissions as well as transfers of allowances. Provided data 
extracted from the EUTL website this module provides a simple Object Relational Mapper (ORM) allowing easy access to the data. 
The data can be downloaded on <a href="https://euets.info">EUETS.INFO</a>.

This package is currently under development and backward compatibility is therefore not granted. 

## Installation 

The installation steps below have been written for **Linux/Ubuntu**.

Clone the repository onto your local system: 

`git clone https://github.com/RikerlWien/eutl_orm.git path/to/local/directory`

### PostgreSQL setup

The ORM relies on a PostgreSQL database. For that, you need to download and install PostgreSQL, for example as explained [here](https://www.tecmint.com/install-postgresql-and-pgadmin-in-ubuntu/).

```bash
# install PostgreSQL
$ sudo apt update
$ sudo apt install postgresql

# check status of PostgreSQL service
$ sudo systemctl is-active postgresql
$ sudo systemctl is-enabled postgresql
$ sudo systemctl status postgresql
$ sudo pg_isready
```

You need to create a user role and an empty database in which the ETS data will be stored. For that, you can use the command-line-based PostgreSQL database shell `psql`

```bash
$ sudo su - postgres
$ psql

# create user 
postgres=# CREATE USER eutl_admin WITH PASSWORD '1234';

# list roles (=users)
postgres=# \du

# create database
postgres=# CREATE DATABASE eutl_db OWNER eutl_admin;

List databases: 
postgres=# \l

# grant privileges on database to user
postgres=# GRANT ALL PRIVILEGES ON DATABASE eutl_db to eutl_admin;

# quit psql
postgres=# \q
```

You can chose whatever database, user name and password you like, but you must make sure you use them to access the database.

For that, create a python file `attic/connection_settings.py` with following content (adapt if you chose a different user name, database name or password):

````python
connectionSettings = dict(
    user="eutl_admin",
    host="localhost",
    db="eutl_db",
    passw="1234",
    port=5432
)
````

To setup the database, run the python script `scripts/setup_db.py`