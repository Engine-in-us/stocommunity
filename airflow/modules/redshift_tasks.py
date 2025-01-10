from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_redshift_table(**kwargs):
    table_name = kwargs["table_name"]
    create_table_sql = kwargs["create_table_sql"]

    # Redshift 연결
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Redshift 테이블 {table_name} 생성 완료.")

def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    s3_path = kwargs['ti'].xcom_pull(key=f"{task_type}_s3_path", task_ids=f"upload_{task_type}_to_s3")

    aws_access_key = kwargs["aws_access_key"]
    aws_secret_key = kwargs["aws_secret_key"]

    copy_sql = f"""
    COPY {table_name}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{aws_access_key}'
    SECRET_ACCESS_KEY '{aws_secret_key}'
    FORMAT AS PARQUET;
    """

    # Redshift COPY 실행
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"{task_type} 데이터를 Redshift 테이블 {table_name}에 적재 완료.")
