# Analytics Engineering Challenge
This Readme file contains two main sections:
- The first part has a technical explanation of all the code and how to run it.
- The second part has explanations and assumptions made through the assesment.

**Disclaimer:**
  - As a professional I've lightly used the DBT framework, it is something I'm keen to continue learning. Nevertheless, due to the nature of the assesment and the time I would need to produce some quality work with DBT, I decided to go with a *Pythonestic* approach which I believe can highlight in a better way my skills.

## Technical Section
This repository  aims to:
1. Resolve the challenge of writting a script that creates the dim_user and user_metrics tables.
2. Produce an easy way to run and test the code for the reviewers convenience.

### Spin up a PG instance with Docker
Running the main.py will create an instance of a PG database which will retain all of our data for this assesment.

### Utility Module
- This module has the table schema for the creation of the original tables that were given on csv files.
  - Staging tables
  - Datawarehouse tables

- In addition, this module contains the queries used to create the final tables asked on the assesment that are named with the suffix 'sql_{table_name}' (Ej. sql_dim_user)

### Central Processing Module: data_insertion folder
*The file secrets.env is intentionally added for the challenge purposes, normally it should be on the .gitignore.*

1. data_processor
  - Purpose: Acts as an orchestrator that utilizes the utility module to produce the raw tables from the csv files given in an agnostic way.

2. data_insertion
  - Purpose: Creates all the tables to migrate with the suffix 'sql_*' on our PG database.

3. main
  - Purpose: Runs both of the processing modules to populate our empty PG instance with the provided tables and solution tables.

### Notebooks
You can find a jupyter notebook named *Queries* under the notebooks folder that contains a breakdown of the queries used through the assesment.
