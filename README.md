Overview
========

This is an ETL project that extracts and collects customers' complaints data in telcoms company from sources like S3 and google sheets, processes this data and load it into a data warehouse for further analysis

Project structure
================
```
coretelecoms-etl/
├── .astro
├── dags/
│   ├── my_dag.py
│                             
├── include/                           
│   ├── load.py
│   ├── tasks.py
│   ├── transformation.py
│   └── dbt/
│       └── my_dbt_project/
│           └── models/
│               ├── marts/
│               │   ├── staging/
│               │
│               ├── dbt_project.yml
│               └── source.yml
├── Dockerfile                           
├── requirements.txt               
├── .env                     
└── airflow_settings.yml
```



Tools
================
S3: data lake
python: for data ingestion, and transformation.
dbt: for building the dimensional model
postgres: data warehouse

Requirements
================
You need to have astro cli on your device to run this project, To install it go to this <a href='https://www.astronomer.io/docs/astro/cli/install-cli' > Link </a>
