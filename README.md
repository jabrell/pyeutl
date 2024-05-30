# pyeutl
The European Transaction Log (EUTL) is the backbone of the European Union Emissions Trading System (EUETS). 
It implements the transfer of emission allowances between parties active in the EUETS. The EUTL provides data on regulated 
installations, their emissions as well as transfers of allowances. The pre-processed data can be downloaded on [EUETS.INFO](https://euets.info).

Provided these data, this pyeutl package provides two different approaches to make these data operational:

1. An simple Object Relational Mapper (ORM) based on a postgres database.
2. Methods to directly access data provided in the zip-file available at [EUETS.INFO](https://euets.info).

This package is currently under development and backward compatibility is therefore not granted. 

# Installation 

Dependencies are managed using poetry. So the most easy way to install *pyeutl* is to use
pip in your python environment. The minimum python version has to 3.11.

```
pip install git+https://github.com/jabrell/pyeutl.git
```

# Get started
Documentation is currently provided in a series of jupyter notebooks.

## Object Relational Mapper

1. orm_1_create_database.ipynb shows how to create the database provided the zip-file containing data extracted from [EUETS.INFO](https://euets.info): [Setting up the database](https://nbviewer.org/github/jabrell/pyeutl/blob/dev202405/orm_1_create_database.ipynb)

2. orm_2_installations.ipynb shows how to analyze compliance and transaction behavior of an installation and associated accounts: [Analyzing installations](https://nbviewer.org/github/jabrell/pyeutl/blob/dev202405/orm_2_Installations.ipynb)
3. orm_3_registry.ipynb shows how to analyze the data on the registry level: [Analyzing countries](https://nbviewer.org/github/jabrell/pyeutl/blob/dev202405/orm_3_Registry.ipynb)

## Ziploader
1. zip_1_load_data.ipynb shows how to load installation, account, and transaction data: [Using the ziploader](https://nbviewer.org/github/jabrell/pyeutl/blob/dev202405/zip_1_load_data.ipynb)

# Versions
To access the 2022 version of the data please you have to use [v2022 version](https://github.com/jabrell/pyeutl/releases/tag/v2022)
