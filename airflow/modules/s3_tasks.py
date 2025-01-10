import pandas as pd
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    file_format = kwargs["file_format"]  # csv 또는 parquet
    file_name_prefix = kwargs["file_name_prefix"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 데이터 가져오기
    data_key = f"{task_type}_processed_data" if file_format == "parquet" else f"{task_type}_data"
    data = kwargs['ti'].xcom_pull(key=data_key, task_ids=f"{task_type}_task")
    if not data:
        raise Exception(f"{task_type} 데이터 S3 업로드 실패: 가져온 데이터가 없습니다.")

    # DataFrame 생성
    df = pd.DataFrame(data)

    # S3 업로드
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_client = s3_hook.get_conn()

    buffer = BytesIO()
    if file_format == "csv":
        df.to_csv(buffer, index=False)
    elif file_format == "parquet":
        df.to_parquet(buffer, index=False)

    buffer.seek(0)
    file_name = f"{file_name_prefix}_{datetime.now().strftime('%Y%m%d')}.{file_format}"
    s3_client.upload_fileobj(buffer, "team6-s3", f"{bucket_path}/{file_name}")

    print(f"{task_type} 데이터를 S3의 {bucket_path}/{file_name}에 업로드 완료.")

    # S3 경로를 XCom에 저장
    kwargs['ti'].xcom_push(key=f"{task_type}_s3_path", value=f"s3://team6-s3/{bucket_path}/{file_name}")
