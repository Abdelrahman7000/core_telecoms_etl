FROM astrocrpublic.azurecr.io/runtime:3.1-5

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate


WORKDIR /usr/local/airflow 
COPY dags/ dags/
COPY include/ include/

ENV DBT_PROFILES_DIR=/usr/local/airflow/include/dbt/my_dbt_project