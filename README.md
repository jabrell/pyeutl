# eutl_orm
The European Transaction Log (EUTL) is the backbone of the European Union Emissions Trading System (EUETS). 
It implements the transfer of emission allowances between parties active in the EUETS. The EUTL provides data on regulated 
installations, their emissions as well as transfers of allowances. Provided data 
extracted from the EUTL website this module provides a simple Object Relational Mapper (ORM) allowing easy access to the data. 
The data can be downloaded on EUETS.INFO.

This package is currently under development and backward compatibility is therefore not granted. 

# Installation 

Clone the repository and follow the steps given the in the different notebooks. Requirements.txt provides dependencies.

# Get started
Documentation is currently provided in a series of jupyter notebooks.

1. 1_create_database.ipynb shows how to create the database provided the zip-file containing data extracted from EUETS.INFO.
2. 2_installations.ipynb shows how to analyze compliance and transaction behavior of an installation and associated accounts.

