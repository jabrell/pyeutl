# eutl_orm
The European Transaction Log (EUTL) is the backbone of the European Union Emissions Trading System (EUETS). 
It implements the transfer of emission allowances between parties active in the EUETS. The EUTL provides data on regulated 
installations, their emissions as well as transfers of allowances. The pre-processed data can be downloaded on <a href="https://euets.info">EUETS.INFO</a>.

Provided these data, this pyeutl package provides two different approaches to make these data operational:

1. An simple Object Relational Mapper (ORM) based on a postgres database.
2. Methods to directly access data provided in the zip-file available at <a href="https://euets.info">EUETS.INFO</a>.

This package is currently under development and backward compatibility is therefore not granted. 

# Installation 

Clone the repository and follow the steps given the in the different notebooks. Requirements.txt provides dependencies.

# Get started
Documentation is currently provided in a series of jupyter notebooks.

## Object Relational Mapper

1. orm_1_create_database.ipynb shows how to create the database provided the zip-file containing data extracted from <a href="https://euets.info">EUETS.INFO</a>.
2. orm_2_installations.ipynb shows how to analyze compliance and transaction behavior of an installation and associated accounts.

## Ziploader

1. zip_1_load_data.ipynb shows how to load installation, account, and transaction data.