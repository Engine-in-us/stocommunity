from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.fetch_tasks import fetch_data
from modules.process_tasks import process_data
from modules.s3_tasks import upload_to_s3
from modules.redshift_tasks import create_redshift_table, upload_to_redshift
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

# API별 설정
api_settings = [
    {
        "task_type": "volume",
        "endpoint": "uapi/domestic-stock/v1/quotations/volume-rank",
        "tr_id": "FHPST01710000",
        "params": {"FID_COND_MRKT_DIV_CODE": "J"},
        "columns": ["순위", "종목코드", "종목명", "현재가", "거래량", "등락률"],
        "transformations": {
            "현재가": lambda x: int(x.replace(",", "")),
            "거래량": lambda x: int(x.replace(",", "")),
            "등락률": lambda x: float(x.strip("%")) / 100,
        },
        "redshift_table": "volume_table",
        "create_table_sql": """
            CREATE TABLE IF NOT EXISTS volume_table (
                rank INT,
                stock_code VARCHAR(10),
                stock_name VARCHAR(40),
                price INT,
                volume INT,
                change_rate FLOAT
            );
        """,
    },
    {
        "task_type": "market_cap",
        "endpoint": "uapi/domestic-stock/v1/quotations/market-cap",
        "tr_id": "FHPST01710100",
        "params": {"FID_COND_MRKT_DIV_CODE": "J"},
        "columns": ["순위", "종목코드", "종목명", "시가총액", "현재가"],
        "transformations": {
            "시가총액": lambda x: int(x.replace(",", "")),
            "현재가": lambda x: int(x.replace(",", "")),
        },
        "redshift_table": "market_cap_table",
        "create_table_sql": """
            CREATE TABLE IF NOT EXISTS market_cap_table (
                rank INT,
                stock_code VARCHAR(10),
                stock_name VARCHAR(40),
                market_cap INT,
                price INT
            );
        """,
    },
    {
        "task_type": "change_rate",
        "endpoint": "uapi/domestic-stock/v1/quotations/change-rate",
        "tr_id": "FHPST01710200",
        "params": {"FID_COND_MRKT_DIV_CODE": "J"},
        "columns": ["순위", "종목코드", "종목명", "등락률", "거래량"],
        "transformations": {
            "등락률": lambda x: float(x.strip("%")) / 100,
            "거래량": lambda x: int(x.replace(",", "")),
        },
        "redshift_table": "change_rate_table",
        "create_table_sql": """
            CREATE TABLE IF NOT EXISTS change_rate_table (
                rank INT,
                stock_code VARCHAR(10),
                stock_name VARCHAR(40),
                change_rate FLOAT,
                volume INT
            );
        """,
    },
]

# DAG 생성
for api in api_settings:
    with DAG(
        dag_id=f"{api['task_type']}_top_!0",
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False,
    ) as dag:

        # Fetch task
        fetch_data_task = PythonOperator(
            task_id=f"fetch_{api['task_type']}_data",
            python_callable=fetch_data,
            op_kwargs={
                "task_type": api["task_type"],
                "endpoint": api["endpoint"],
                "tr_id": api["tr_id"],
                "params": api["params"],
                "top_n": 10,
            },
        )

        # Upload raw data to S3
        upload_raw_to_s3_task = PythonOperator(
            task_id=f"upload_raw_{api['task_type']}_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={
                "task_type": api["task_type"],
                "file_format": "csv",
                "file_name_prefix": f"{api['task_type']}_raw",
                "bucket_path": f"raw_data/{api['task_type']}",
            },
        )

        # Process data
        process_data_task = PythonOperator(
            task_id=f"process_{api['task_type']}_data",
            python_callable=process_data,
            op_kwargs={
                "task_type": api["task_type"],
                "columns": api["columns"],
                "transformations": api["transformations"],
            },
        )

        # Upload transformed data to S3
        upload_transformed_to_s3_task = PythonOperator(
            task_id=f"upload_transformed_{api['task_type']}_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={
                "task_type": api["task_type"],
                "file_format": "parquet",
                "file_name_prefix": f"{api['task_type']}_transformed",
                "bucket_path": f"transformed_data/{api['task_type']}",
            },
        )

        # Create Redshift table
        create_table_task = PythonOperator(
            task_id=f"create_{api['task_type']}_table",
            python_callable=create_redshift_table,
            op_kwargs={
                "table_name": api["redshift_table"],
                "create_table_sql": api["create_table_sql"],
            },
        )

        # Upload to Redshift
        upload_to_redshift_task = PythonOperator(
            task_id=f"upload_{api['task_type']}_to_redshift",
            python_callable=upload_to_redshift,
            op_kwargs={
                "task_type": api["task_type"],
                "table_name": api["redshift_table"],
                "aws_access_key": "your_access_key",
                "aws_secret_key": "your_secret_key",
            },
        )

        # DAG dependencies
        fetch_data_task >> upload_raw_to_s3_task >> process_data_task >> upload_transformed_to_s3_task >> create_table_task >> upload_to_redshift_task
