# Simple ETL Using Airflow

This is a simple ETL using Airflow. First, we fetch data from API (extract). Then, we drop unused columns, convert to CSV, and validate (transform). Finally, we load the transformed data to database (load).

## Prerequisite


### Install Dependency (for local run)

``` bash

pip install -r requirements.txt
```

### Setup PostgreSQL

Install PostgreSQL & Create user and password



## Test Running the DAG (local run)

We create a DAG that has 3 Tasks:

* `extract_sales` - Extracts data and save it to csv format (extracted_sales_date.csv) adding the current date in the name.
* `transform_sales`  - Transforms data and save it to csv format (transformed_sales_date.csv) adding the current date in the name.
* `load_sales` - Loads the data in the postgres database using the SQLAlchemy



Finaaly, Check Airflow UI to see your new deployed DAG and enable it.

