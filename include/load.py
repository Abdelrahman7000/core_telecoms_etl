def load_data_to_pg(
    table_name,
    mode: str = "incremental",
    source_prefix="raw/web_forms/",
    transform_fn=None,
):
    import boto3
    import pyarrow.parquet as pq
    import pyarrow as pa
    import io
    import os
    import pandas as pd
    import logging
    from datetime import datetime
    from sqlalchemy import create_engine
    from airflow.models import Variable

    # boto session
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    s3 = session.client("s3")

    BUCKET_NAME = os.getenv("DEST_BUCKET")
    PG_CONN = Variable.get("pg_conn_str")
    engine = create_engine(PG_CONN)

    # list parquet files
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=source_prefix)
    parquet_files = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if mode == "incremental":
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        files_to_load = [f for f in parquet_files if today_str in f]
    elif mode == "full":
        files_to_load = parquet_files
    elif mode == "static":
        files_to_load = [f for f in parquet_files if table_name in f]
    else:
        raise ValueError("Invalid mode")

    logging.info(f"Files to load: {files_to_load}")

    # LOAD IN CHUNKS (row groups)
    for key in files_to_load:
        logging.info(f"Streaming file: s3://{BUCKET_NAME}/{key}")

        buffer = io.BytesIO()
        s3.download_fileobj(BUCKET_NAME, key, buffer)
        buffer.seek(0)

        parquet_file = pq.ParquetFile(buffer)
        total_row_groups = parquet_file.num_row_groups

        logging.info(f"{key} has {total_row_groups} row groups")

        # Stream row group by row group
        for rg in range(total_row_groups):
            table = parquet_file.read_row_group(rg)
            df_chunk = table.to_pandas()

            # apply optional transform
            if transform_fn:
                df_chunk = transform_fn(df_chunk)

            # load chunk into postgres
            df_chunk.to_sql(
                table_name,
                engine,
                if_exists="append",       # always append chunk-by-chunk
                index=False
            )

            logging.info(f"Loaded chunk {rg+1}/{total_row_groups} from {key}")

    logging.info(f"Finished loading table: {table_name}")

    
