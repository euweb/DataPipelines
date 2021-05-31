# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines.
The tool Apache Airflow should be used to load and process data residing in S3 and to transfer it to data warehouse Redshift in the AWS.

## DAGs

**udac_example_dag.py**

![DAG](https://github.com/euweb/DataPipelines/blob/main/dag.png?raw=true)

**subdag.py**

![Sub-DAG](https://github.com/euweb/DataPipelines/blob/main/subdag.png?raw=true)

## Configuration

### Airflow

#### Variables

`udac_example_dag.append`: Possible values are _True_ or _False_. If False the dimensions and facts tables will be emptied before inserting new rows

#### Connections

`aws_credentials`: AWS Credentials to Access Redshift Cluster and S3 Bucket

`redshift`: Connection to the Redshift Cluster

## Pre-Requests

  - AWS account
  - Up and running Redshift Cluster

## Running

1. clone this repository
2. run `docker-compose up`
3. go to http://localhost:8080/home
4. create _aws_credentials_ and _redshift_ connections (Admin->Connections)
5. activate an run DAG `udac_example_dag`
