import pandas as pd
import boto3
import pyarrow.parquet as pq
import io
from datetime import datetime
from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
import logging

load_dotenv()

session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION'),
    )
s3 = session.client("s3")
PG_CONN= os.getenv("pg_conn_str")
BUCKET_NAME = os.getenv("DEST_BUCKET")



# def load_social_media(table_name='media_complaints',mode='incremental'):
    
#     response = s3.list_objects_v2(
#         Bucket=BUCKET_NAME,
#         Prefix='raw/social_media/'
#     )

#     if "Contents" not in response:
#         raise ValueError("No files found")

#     parquet_files = [
#         obj["Key"]
#         for obj in response["Contents"]
#         if obj["Key"].endswith(".parquet")
#     ]

#     if not parquet_files:
#         raise ValueError("No parquet files found")

#     if mode == "incremental":
#         today_str = datetime.utcnow().strftime("%Y-%m-%d")
#         files_to_load = [f for f in parquet_files if today_str in f]
#         if not files_to_load:
#             logging.info(f"No parquet files found for today ({today_str})")
#             return
#     elif mode == "full":
#         files_to_load = parquet_files
  
#     else:
#         raise ValueError("mode must be 'incremental', 'full', or 'static' ")

#     logging.info(f"Files to load ({len(files_to_load)}): {files_to_load}")

#     df_list = []

#     # 2. Download + read each parquet file
#     for key in files_to_load:
#         logging.info(f"Reading s3://{BUCKET_NAME}/{key}")

#         buffer = io.BytesIO()
#         s3.download_fileobj(BUCKET_NAME, key, buffer)
#         buffer.seek(0)  # Reset cursor

#         table = pq.ParquetFile(buffer).read()
#         df = table.to_pandas()
#         df_list.append(df)

#     # 3. Combine
#     combined_df = pd.concat(df_list, ignore_index=True)
#     logging.info("Total rows:", len(combined_df))

#      # 5️⃣ Write to Postgres using SQLAlchemy
#     engine = create_engine(PG_CONN)

#     if mode == "incremental":
#         # append only
#         combined_df.to_sql(table_name, engine, if_exists="append", index=False)
#     else:
#         # full load: create or replace
#         combined_df.to_sql(table_name, engine, if_exists="replace", index=False)

#     logging.info(f"Data loaded to Postgres table {table_name} successfully.")

def load_data_to_pg(
    table_name,
    mode: str = "incremental",
    source_prefix="raw/web_forms/",
):

    # List parquet files in S3 under the given prefix
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=source_prefix
    )

    if "Contents" not in response:
        raise ValueError("No files found")

    # List of parquet files
    parquet_files = [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        raise ValueError("No parquet files found")

    # Determine files to load based on mode
    if mode == "incremental":
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        files_to_load = [f for f in parquet_files if today_str in f]
        if not files_to_load:
            logging.info(f"No parquet files found for today ({today_str})")
            return
    elif mode == "full":
        files_to_load = parquet_files
    
    elif mode=='static':
        files_to_load=[f for f in parquet_files if table_name in f]
        
    else:
        raise ValueError("mode must be 'incremental', 'full', or 'static' ")

    logging.info(f"Files to load ({len(files_to_load)}): {files_to_load}")

    df_list = []

    # Download and read each parquet file
    for key in files_to_load:
        logging.info(f"Reading s3://{BUCKET_NAME}/{key}")

        buffer = io.BytesIO()
        s3.download_fileobj(BUCKET_NAME, key, buffer)
        buffer.seek(0)

        table = pq.ParquetFile(buffer).read()
        df = table.to_pandas()
        df_list.append(df)

    # union all dataframes
    combined_df = pd.concat(df_list, ignore_index=True)
    logging.info("Total rows:", len(combined_df))

     # create sqlalchemy engine and write to Postgres
    engine = create_engine(PG_CONN)

    if mode == "incremental":
        # append new data
        combined_df.to_sql(table_name, engine, if_exists="append", index=False)
    else:
        # full load for new data
        combined_df.to_sql(table_name, engine, if_exists="replace", index=False)

    logging.info(f"Data loaded to Postgres table {table_name} successfully.")


    
