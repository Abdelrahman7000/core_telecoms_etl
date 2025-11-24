import os
from dotenv import load_dotenv
import boto3
import requests
import logging
import io
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import pandas as pd
import psycopg2

load_dotenv()



# Using a session to manage AWS credentials
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)
# Create S3 and SSM clients from the session
s3 = session.client("s3")
ssm = session.client("ssm")

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET")
DEST_BUCKET = os.getenv("DEST_BUCKET")


def get_pg_credentials():
    '''
    Retrieve PostgreSQL credentials from AWS SSM Parameter Store.
    Returns:
    dict: A dictionary containing the database host, username, password, name, and port.
    '''
    params = ["/coretelecomms/database/db_host","/coretelecomms/database/db_username",
               "/coretelecomms/database/db_password", "/coretelecomms/database/db_name",
               "/coretelecomms/database/db_port"]

    secrets = {}
    for p in params:
        response = ssm.get_parameter(Name=p, WithDecryption=True)
        secrets[p] = response["Parameter"]["Value"]

    return secrets

def write_parquet_to_s3(df, bucket, key):
    '''
    Write a Pandas DataFrame as a Parquet file to S3.

    Args:
    df (pd.DataFrame): The DataFrame to write.
    bucket (str): The S3 bucket name.
    key (str): The S3 object key.
    '''
    table = pa.Table.from_pandas(df)
    out_buffer = io.BytesIO()
    pq.write_table(table, out_buffer)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=out_buffer.getvalue()
    )
    logging.info(f"Wrote parquet → s3://{bucket}/{key}")


def s3_exists(bucket, key):
    '''
    Check if an object exists in S3.
    Args:
    bucket (str): The S3 bucket name.
    key (str): The S3 object key.
    '''
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logging.info(f"Found existing object → s3://{bucket}/{key}")
        return True
    except:
        logging.info(f"Object does not exist → s3://{bucket}/{key}")
        return False

def ingest_website_forms():
    '''
    Ingest web form request tables from Postgres, normalize schema, and write as Parquet to S3.'''

    # Get Postgres creds from SSM
    creds = get_pg_credentials()

    conn = psycopg2.connect(
        host=creds['/coretelecomms/database/db_host'],
        user= creds['/coretelecomms/database/db_username'],
        password= creds['/coretelecomms/database/db_password'],
        dbname= creds['/coretelecomms/database/db_name'],
        port=creds['/coretelecomms/database/db_port']
    )

    cursor = conn.cursor()

    # Find all tables that follow the naming pattern
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'customer_complaints'
          AND table_name LIKE 'web_form_request_%';
    """)

    tables = [row[0] for row in cursor.fetchall()]

    for tbl in tables:
        _, _, _, year, month, day = tbl.split("_")
        date_str = f"{year}-{month}-{day}"

        #s3_target_key = f"raw/web_forms/ingest_date={date_str}/data.parquet"
        s3_target_key = f"raw/web_forms/Web_form_request_{date_str}.parquet"

        # Check if the S3 partition already exists → skip if yes
        if s3_exists(DEST_BUCKET, s3_target_key):
            logging.info(f"Skipping — already processed: {tbl}")
            continue

        logging.info(f"Processing table: {tbl}")

        # Extract full table from Postgres into DataFrame
        df = pd.read_sql(
            f"SELECT * FROM customer_complaints.{tbl};",
            conn
        )

        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Add some metadata columns
        df["ingestion_timestamp"] = datetime.utcnow()
        df["source_system"] = "website_forms_pg"
        df["source_table"] = tbl

        # Write to S3 raw zone
        write_parquet_to_s3(df, DEST_BUCKET, s3_target_key)

        logging.info(f"Ingested {tbl} → {s3_target_key}")

    conn.close()


def staging_static_data(filename, source="s3"):
    '''
    Load static data into S3 from either another S3 bucket or a public URL.
    Args:
    filename (str): The base name of the file.
    source (str): "s3" to load from another S3 bucket, otherwise loads from URL.
    '''
    URL = (
        "https://docs.google.com/spreadsheets/d/"
        "1bQTdflSmSTkbMm2HLmLkKDcJlwv1S2CnbOxzbS6QcnQ/export?format=csv&gid=0"
    )

    dest_key = f"raw/{filename}.csv"

    try:
        if source == "s3":
            src_key = f"customers/{filename}.csv"
            s3.copy_object(
                Bucket=DEST_BUCKET,
                CopySource={"Bucket": SOURCE_BUCKET, "Key": src_key},
                Key=dest_key,
            )
        else:
            response = requests.get(URL)
            response.raise_for_status()

            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=dest_key,
                Body=response.content,
            )

        logging.info("Data loaded successfully")
    except Exception as e:
        logging.error(f"Error while loading to S3: {e}")



def ingest_data(prefix, extension, target_prefix, source_system, read_func):
    """
    Ingest data files from S3, normalize schema, and write as Parquet to S3.
    Args:
    prefix (str): S3 prefix to list source files.
    extension (str): File extension to filter source files.
    target_prefix (str): S3 prefix for the output Parquet files.
    source_system (str): Identifier for the source system.
    read_func (function): Function to read raw file data into a Pandas DataFrame.
    """

    response = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])

    for obj in objects:
        key = obj["Key"]
        if not key.endswith(extension):
            continue

        filename = key.split("/")[-1]
        date_part = filename.split("_")[-1].replace(extension, "")

        target_key = f"{target_prefix}_{date_part}.parquet"

        # skip if already processed
        if s3_exists(DEST_BUCKET, target_key):
            logging.info(f"Skipping — already processed: {filename}")
            continue

        logging.info(f"Processing: {filename}")

        # Read raw file
        raw_obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)
        df = read_func(raw_obj["Body"].read())

        # Normalize schema
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df["ingestion_timestamp"] = datetime.utcnow()
        df["source_system"] = source_system
        df["file_name"] = filename

        write_parquet_to_s3(df, DEST_BUCKET, target_key)
        logging.info(f"Ingest complete → {target_key}")



def ingest_call_center_logs():
    '''
    Ingest call center log CSV files from S3, normalize schema, and write as Parquet to S3.
    '''
    ingest_data(
        prefix="call logs/",
        extension=".csv",
        target_prefix="raw/call_center_logs/call_logs_day",
        source_system="call_center_logs",
        read_func=lambda data: pd.read_csv(io.BytesIO(data)),
    )


def ingest_social_media_data():
    '''
    Ingest social media JSON files from S3, normalize schema, and write as Parquet to S3.
    '''
    ingest_data(
        prefix="social_medias/",
        extension=".json",
        target_prefix="raw/social_media/media_complaint_day",
        source_system="media_complaints",
        read_func=lambda data: pd.read_json(io.BytesIO(data)),
    )
