from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# API 및 S3, Redshift 설정 가져오기
def get_connections(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port,
        "extra": conn.extra_dejson,
    }

# 데이터를 가져오는 함수
def fetch_data(**kwargs):
    task_type = kwargs["task_type"]
    api_conn = get_connections("koreainvestment_api")

    endpoint = f"{api_conn['host']}/{kwargs['endpoint']}"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {api_conn['extra']['access_token']}",
        "appkey": api_conn["extra"]["app_key"],
        "appsecret": api_conn["extra"]["app_secret"],
        "tr_id": kwargs["tr_id"],
    }
    params = kwargs["params"]

    response = requests.get(endpoint, headers=headers, params=params)
    print(f"API 응답 상태 코드: {response.status_code}")
    print(f"API 응답 데이터: {response.text}")

    # 응답 데이터 구조 확인
    try:
        data = response.json().get("output", [])
        print(f"가져온 데이터: {data}")
    except Exception as e:
        raise Exception(f"API 응답 파싱 실패: {e}")

    # 필요한 컬럼만 필터링
    columns = kwargs["columns"]
    filtered_data = [{col: item.get(col, None) for col in columns} for item in data]

    # DataFrame으로 변환
    df = pd.DataFrame(filtered_data)

    if "종목코드" in df.columns:
        df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

    # 상위 10개 데이터만 가져오기
    top_10_data = df.head(10)

    # CSV 파일로 저장
    file_path = f"/tmp/raw_{task_type}_data_{datetime.now().strftime('%y%m%d')}.csv"
    pd.DataFrame(top_10_data).to_csv(file_path, index=False, encoding='utf-8-sig')

    # XCom에 파일 경로 저장
    kwargs['ti'].xcom_push(key=f"{task_type}_csv_path", value=file_path)
    print(f"{task_type} 데이터를 CSV로 저장했습니다: {file_path}")

# 데이터를 S3에 저장 (CSV 파일 업로드)
def upload_raw_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    # S3로 업로드
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=csv_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(csv_path)}",
        replace=True
    )
    print(f"{task_type} 데이터를 S3의 {bucket_path}/{os.path.basename(csv_path)}에 저장 완료.")

# 데이터를 처리하는 함수
def process_data(**kwargs):
    task_type = kwargs["task_type"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    # 데이터 로드 및 컬럼 확인
    df = pd.read_csv(csv_path)

    # 컬럼 이름 매핑 및 스키마 정의
    schema_mapping = {
        "stock_volume_top10": {
            "columns": {
                "data_rank": "순위",
                "mksc_shrn_iscd": "종목코드",
                "hts_kor_isnm": "종목명",
                "stck_prpr": "현재가",
                "acml_vol": "거래량",
                "prdy_ctrt": "등락율",
            },
            "dtypes": {
                "순위": "int32",
                "종목코드": "string",
                "종목명": "string",
                "현재가": "int32",
                "거래량": "int64",
                "등락율": "float64",
            },
        },
        "market_cap_top10": {
            "columns": {
                "data_rank": "순위",
                "mksc_shrn_iscd": "종목코드",
                "hts_kor_isnm": "종목명",
                "stck_prpr": "현재가",
                "stck_avls": "시가총액(억)",
                "mrkt_whol_avls_rlim": "시장비중(%)",
            },
            "dtypes": {
                "순위": "int32",
                "종목코드": "string",
                "종목명": "string",
                "현재가": "int32",
                "시가총액(억)": "int64",
                "시장비중(%)": "float64",
            },
        },
        "price_change_top10": {
            "columns": {
                "data_rank": "순위",
                "stck_shrn_iscd": "종목코드",
                "hts_kor_isnm": "종목명",
                "stck_prpr": "현재가",
                "prdy_ctrt": "전일대비율",
                "acml_vol": "누적거래량",
            },
            "dtypes": {
                "순위": "int32",
                "종목코드": "string",
                "종목명": "string",
                "현재가": "int32",
                "전일대비율": "float64",
                "누적거래량": "int64",
            },
        },
    }

    # 매핑 정보 가져오기
    if task_type not in schema_mapping:
        raise ValueError(f"'{task_type}'에 대한 스키마 매핑이 정의되지 않았습니다.")

    task_schema = schema_mapping[task_type]
    column_mapping = task_schema["columns"]
    dtypes = task_schema["dtypes"]

    # 컬럼 이름 변경
    df.rename(columns=column_mapping, inplace=True)

    # 데이터 타입 변환
    for col, dtype in dtypes.items():
        if col in df.columns:
            # 컬럼명이 '종목코드' 또는 '종목명'인 경우 문자열로 변환
            if col == "종목코드":
                df[col] = df[col].astype(str).str.zfill(6)  # 문자열로 변환하고 앞에 0 채우기
            elif col == "종목명":
                df[col] = df[col].astype(str)  # 문자열로 변환
            else:
                # 나머지 데이터는 숫자로 변환
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(dtype)
        else:
            raise KeyError(f"'{col}' 컬럼이 데이터프레임에 없습니다.")
            
    # 처리된 데이터 저장 (Parquet 포맷)
    processed_path = f"/tmp/transformed_{task_type}_data_{datetime.now().strftime('%y%m%d')}.parquet"
    df.to_parquet(processed_path, index=False)
    kwargs['ti'].xcom_push(key=f"{task_type}_processed_path", value=processed_path)
    print(f"{task_type} 처리된 데이터가 저장되었습니다: {processed_path}")

# 처리된 데이터를 S3에 업로드 (Parquet 파일 업로드)
def upload_transformed_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 처리된 Parquet 파일 경로 가져오기
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")
    if not processed_path or not os.path.exists(processed_path):
        raise FileNotFoundError(f"처리된 Parquet 파일 경로를 찾을 수 없습니다: {processed_path}")

    # S3로 업로드
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(processed_path)}",
        replace=True
    )
    print(f"{task_type} 처리된 데이터를 S3의 {bucket_path}/{os.path.basename(processed_path)}에 저장 완료.")

# Redshift 테이블 생성 함수
def create_redshift_table(**kwargs):
    table_name = kwargs["table_name"]
    columns_sql = kwargs["columns_sql"]
    redshift_conn = get_connections("redshift_conn")

    # Redshift 연결
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 테이블 생성 SQL
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} ({columns_sql});
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Redshift 테이블 {table_name}이 생성되었습니다.")

# Redshift에 데이터 적재 (COPY 명령)
def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")

    if not processed_path:
        raise Exception(f"{task_type} 데이터 Redshift 업로드 실패: 처리된 데이터 경로가 없습니다.")

    # AWS Connection 사용
    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login  # AWS Access Key ID
    secret_key = aws_conn.password

    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # COPY 명령 실행
    copy_sql = f"""
    COPY {table_name}
    FROM 's3://team6-s3/transformed_data/{os.path.basename(processed_path)}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS PARQUET;
    """
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{task_type} 데이터를 Redshift 테이블 {table_name}에 적재 완료.")

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="stock_ranking_data_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    endpoints = [
        {
            "task_type": "stock_volume_top10",
            "endpoint": "uapi/domestic-stock/v1/quotations/volume-rank",
            "tr_id": "FHPST01710000",
            "params": {"FID_COND_MRKT_DIV_CODE": "J",  # 시장 구분 코드 (J: 주식, ETF, ETN)
                        "FID_COND_SCR_DIV_CODE": "20171",  # 조건 화면 분류 코드
                        "FID_INPUT_ISCD": "0000",  # 전체 종목 대상
                        "FID_DIV_CLS_CODE": "0",  # 전체 분류
                        "FID_BLNG_CLS_CODE": "0",  # 평균 거래량 기준
                        "FID_TRGT_CLS_CODE": "111111111",  # 대상 구분 코드
                        "FID_TRGT_EXLS_CLS_CODE": "0000000000",  # 제외 대상 없음
                        "FID_INPUT_PRICE_1": "0",  # 가격 제한 없음
                        "FID_INPUT_PRICE_2": "0",
                        "FID_VOL_CNT": "0",  # 거래량 제한 없음
                        "FID_INPUT_DATE_1": "0"},
            "columns": ["data_rank", "mksc_shrn_iscd", "hts_kor_isnm", "stck_prpr", "acml_vol", "prdy_ctrt"],
            "columns_sql": "\"순위\" INT4, \"종목코드\" VARCHAR(10), \"종목명\" VARCHAR(50), \"현재가\" INT4, \"거래량\" INT8, \"등락율\" FLOAT8"
        },
        {
            "task_type": "market_cap_top10",
            "endpoint": "uapi/domestic-stock/v1/ranking/market-cap",
            "tr_id": "FHPST01740000",
            "params": {"fid_cond_mrkt_div_code": "J",  # 주식 시장
                        "fid_cond_scr_div_code": "20174",  # 조건 화면 분류 코드
                        "fid_div_cls_code": "0",  # 전체 분류
                        "fid_input_iscd": "0000",  # 전체 종목 대상
                        "fid_trgt_cls_code": "0",  # 전체 대상
                        "fid_trgt_exls_cls_code": "0",  # 제외 대상 없음
                        "fid_input_price_1": "",
                        "fid_input_price_2": "",
                        "fid_vol_cnt": ""},
            "columns": ["data_rank", "mksc_shrn_iscd", "hts_kor_isnm", "stck_prpr", "stck_avls", "mrkt_whol_avls_rlim"],
            "columns_sql": "\"순위\" INT4, \"종목코드\" VARCHAR(10), \"종목명\" VARCHAR(50), \"현재가\" INT4, \"시가총액(억)\" INT8, \"시장비중(%)\" FLOAT8"
        },
        {
            "task_type": "price_change_top10",
            "endpoint": "uapi/domestic-stock/v1/ranking/fluctuation",
            "tr_id": "FHPST01700000",
            "params": {"fid_cond_mrkt_div_code": "J",  # 전체 시장
                        "fid_cond_scr_div_code": "20170",
                        "fid_input_iscd": "0000",  # 전체 종목
                        "fid_rank_sort_cls_code": "0",  # 0: 상승률 순, 1: 하락률 순
                        "fid_input_cnt_1": "0",
                        "fid_prc_cls_code": "1",  # 종가 대비 설정
                        "fid_input_price_1": "",
                        "fid_input_price_2": "",
                        "fid_vol_cnt": "",
                        "fid_trgt_cls_code": "0",
                        "fid_trgt_exls_cls_code": "0",
                        "fid_div_cls_code": "0",
                        "fid_rsfl_rate1": "",
                        "fid_rsfl_rate2": ""},
            "columns": ["data_rank", "stck_shrn_iscd", "hts_kor_isnm", "stck_prpr", "prdy_ctrt", "acml_vol"],
            "columns_sql": "\"순위\" INT4, \"종목코드\" VARCHAR(10), \"종목명\" VARCHAR(50), \"현재가\" INT4, \"전일대비율\" FLOAT8, \"누적거래량\" INT8"
        }
    ]

    for endpoint in endpoints:
        fetch_data_task = PythonOperator(
            task_id=f"fetch_{endpoint['task_type']}_data",
            python_callable=fetch_data,
            op_kwargs=endpoint,
        )

        upload_raw_data_to_s3_task = PythonOperator(
            task_id=f"upload_raw_{endpoint['task_type']}_data_to_s3",
            python_callable=upload_raw_to_s3,
            op_kwargs={
                "task_type": endpoint["task_type"],
                "bucket_path": "raw_data",
            },
        )

        process_data_task = PythonOperator(
            task_id=f"process_{endpoint['task_type']}_data",
            python_callable=process_data,
            op_kwargs=endpoint,
        )

        upload_transformed_data_to_s3_task = PythonOperator(
            task_id=f"upload_transformed_{endpoint['task_type']}_data_to_s3",
            python_callable=upload_transformed_to_s3,
            op_kwargs={
                "task_type": endpoint["task_type"],
                "bucket_path": "transformed_data",
            },
        )

        create_table_task = PythonOperator(
            task_id=f"create_{endpoint['task_type']}_table",
            python_callable=create_redshift_table,
            op_kwargs={
                "table_name": f"transformed_{endpoint['task_type']}",
                "columns_sql": endpoint["columns_sql"]
            },
        )

        upload_to_redshift_task = PythonOperator(
            task_id=f"upload_{endpoint['task_type']}_data_to_redshift",
            python_callable=upload_to_redshift,
            op_kwargs={
                "task_type": endpoint["task_type"],
                "table_name": f"transformed_{endpoint['task_type']}",
            },
        )

        fetch_data_task >> upload_raw_data_to_s3_task >> process_data_task >> upload_transformed_data_to_s3_task >> create_table_task >> upload_to_redshift_task
