

**Instruction of running the code**
eg: jupyter notebook
(1) Open a new notebook file test.ipynb.
#import the following module:
import create_cluster as cc
import create_tables as ct
import etl 

(2) fill out your infomation in dwh.cfg files:
[CLUSTER]
HOST=
DB_NAME=sparkify
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=

[AWS]
KEY=
SECRET=

(3)execute the following command:
# This will create a new cluster, with a iam role granted permission to access the cluster
cc.main()

(4)execute the following command:
# This will create the database tables, including staging/destination tables.
ct.main()

(5)execute the following command:
# This will populate the data to staging, and final tables.
etl.main()

