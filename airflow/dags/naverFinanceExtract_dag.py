from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import time, boto3, os, logging
import pandas as pd



# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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

def crawl_stock_data(**kwargs):
    # Selenium 드라이버 설정
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
        driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=options)  # ChromeDriver 경로
        driver.get("https://example.com")
        driver.quit()
    except Exception as e:
        print(e)
        raise


    today = datetime.now().strftime('%Y.%m.%d')
    url = 'https://m.stock.naver.com/investment/research/company'
    
    text_list = []
    cols = ["date", "stockCode", "stockName", "investmentOpinion", "targetPrice", "currentPrice", "title", "text", "url"]
    
    try : 
        driver.get(url)
        time.sleep(2)  # 페이지 로딩 대기
        i = 0
        while True:
            i += 1
            time.sleep(2)
            search_page = driver.find_elements(By.CLASS_NAME, "ResearchList_item__I6Z7_")[i]

            news_date = driver.find_elements(By.CLASS_NAME, 'ResearchList_description___nHPv')[1].text.strip(".")
            stock_name = search_page.text.split("\n")[0]

            if news_date not in search_page.text:
                break
            else:
                try:
                    search_page.click()
                except:
                    driver.execute_script(f"window.scrollBy(0, 110);")
                    search_page.click()

            time.sleep(2)

            news_url = driver.current_url

            stock_code = int(driver.find_element(By.CLASS_NAME, 'HeaderResearch_tag__qwHzD').text.replace(stock_name, ""))

            investment_opinion = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_text__BFWiw').text

            try:
                target_price_text = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price___VI3M').text.split("\n")[-1]
                target_price = int(target_price_text.replace(",", "").replace("원", ""))
            except:
                target_price = None

            try:
                current_price_text = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price_today__zpk_T').text.split("\n")[-1]
                current_price = int(current_price_text.replace(",", "").replace("원", ""))
            except:
                current_price = None

            title = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[0]
            
            text = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[-1]

            if text_list[-1] == [news_date, stock_code, stock_name, investment_opinion, target_price, current_price, title, text, news_url] :
                break
            else :
                text_list.append([news_date, stock_code, stock_name, investment_opinion, target_price, current_price, title, text, news_url])

            driver.back()
            time.sleep(2)
            driver.execute_script(f"window.scrollBy(0, 110);")

    except Exception as e:
        print("에러 발생:", e)

    finally:
        driver.quit()
        if text_list :
            # 데이터프레임 생성 및 저장
            df = pd.DataFrame(text_list, columns=cols)
            file_path_news = 'naverNews.csv'
            df.to_csv(file_path_news, index=False)
            print("데이터가  저장되었습니다.")
        else :
            print("데이터가 없어 파일을 생성하지 않았습니다.")

    return file_path_news

def crawl_kospi_kosdaq_data(**kwargs):
    # Selenium 드라이버 설정
    # Selenium 드라이버 설정
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
        driver = webdriver.Chrome(service=Service('/usr/local/bin/chromedriver'), options=options)  # ChromeDriver 경로
        driver.get("https://example.com")
        driver.quit()
    except Exception as e:
        print(e)
        raise 

    url_kospi = 'https://m.stock.naver.com/domestic/index/KOSPI/total'
    url_kosdaq = 'https://m.stock.naver.com/domestic/index/KOSDAQ/total'

    data = []

    try:
        # 코스피 데이터 크롤링
        driver.get(url_kospi)
        time.sleep(3)

        kospi_index = driver.find_element(By.CLASS_NAME, 'GraphMain_price__GT8dV').text
        kospi_rate = driver.find_elements(By.CLASS_NAME, 'VGap_gap__LQYpL')[1].text

        # 코스닥 데이터 크롤링
        driver.get(url_kosdaq)
        time.sleep(3)

        kosdaq_index = driver.find_element(By.CLASS_NAME, 'GraphMain_price__GT8dV').text
        kosdaq_rate = driver.find_elements(By.CLASS_NAME, 'VGap_gap__LQYpL')[1].text

        # 데이터 저장
        data.append({
            'indexName': "kospi_index",
            'value': kospi_index
        })
        data.append({
            'indexName': 'kospi_rate',
            'value': kospi_rate
        })
        data.append({
            'indexName': 'kosdaq_index',
            'value': kosdaq_index
        })
        data.append({
            'indexName': 'kosdaq_rate',
            'value': kosdaq_rate
        })

        # DataFrame 생성
        df = pd.DataFrame(data)
        file_path_kospi_kosdaq = 'kospi_kosdaq_data.csv'
        df.to_csv(file_path_kospi_kosdaq, index=False, encoding='utf-8-sig')
        print(f"CSV 파일 {file_path_kospi_kosdaq} 생성 완료.")

    except Exception as e:
        print("에러 발생:", e)

    finally:
        driver.quit()

    return file_path_kospi_kosdaq

def upload_to_s3(file_path, bucket_name, object_key):
    # GitHub Secrets에서 AWS 자격 증명을 환경 변수로 설정
    aws_conn = get_connections("aws_conn")
    aws_access_key = aws_conn.login #os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = aws_conn.password #os.getenv('AWS_SECRET_ACCESS_KEY')

    # S3 클라이언트 생성
    s3_client = boto3.client(
        's3',
        aws_access_key_id= "${{ secrets.AWS_ACCESS_KEY_ID}}",
        aws_secret_access_key= "${{ secrets.AWS_SECRET_ACCESS_KEY}}"
    )

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        print(f"파일이 S3에 업로드되었습니다: s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"S3 업로드 실패: {e}")

def s3_upload_task_func(**kwargs):
    # 크롤링 작업의 출력 파일 경로 가져오기
    ti = kwargs['ti']
    stock_file_path = ti.xcom_pull(task_ids='crawl_stock_data_task')
    kospi_kosdaq_file_path = ti.xcom_pull(task_ids='crawl_kospi_kosdaq_data_task')

    # S3 업로드 정보
    bucket_name = 'team6-s3'
    stock_object_key = 'raw_data/naverFinance/naverFinanceNews.csv'
    kospi_kosdaq_object_key = 'raw_data/naverFinance/kospiKosdaqData.csv'

    # 파일 업로드
    upload_to_s3(stock_file_path, bucket_name, stock_object_key)
    upload_to_s3(kospi_kosdaq_file_path, bucket_name, kospi_kosdaq_object_key)

# Redshift 테이블 생성 함수
def create_redshift_tables(**kwargs):
    # Redshift 연결 설정
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 테이블 생성 SQL (naverNews 테이블)
    create_naver_news_table_sql = """
    CREATE TABLE IF NOT EXISTS naverNews (
        date DATE,
        stockCode INT,
        stockName VARCHAR(40),
        investmentOpinion VARCHAR(20),
        targetPrice INT,
        currentPrice INT,
        title VARCHAR(255),
        text VARCHAR(MAX),
        url VARCHAR(255)
    );
    """

    # 테이블 생성 SQL (kospiKosdaqData 테이블)
    create_kospi_kosdaq_table_sql = """
    CREATE TABLE IF NOT EXISTS kospiKosdaqData (
        indexName VARCHAR(20),
        value VARCHAR(40)
    );
    """

    # 테이블 생성 실행
    cursor.execute(create_naver_news_table_sql)
    cursor.execute(create_kospi_kosdaq_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print("Redshift 테이블 naverNews 및 kospiKosdaqData가 성공적으로 생성되었습니다.")

# Redshift에 데이터 적재 함수
def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    s3_path = kwargs['ti'].xcom_pull(key=f"{task_type}_s3_path", task_ids=f"upload_to_s3_task")

    if not s3_path:
        raise Exception(f"{task_type} 데이터 Redshift 업로드 실패: S3 경로가 없습니다.")

    # AWS 연결 정보 가져오기
    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login  # AWS Access Key ID
    secret_key = aws_conn.password

    # Redshift 연결 설정
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # COPY 명령 실행 (S3 데이터를 Redshift로 로드)
    copy_sql = f"""
    COPY {table_name}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    CSV DELIMITER ',' IGNOREHEADER 1;
    """
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"{task_type} 데이터를 Redshift 테이블 {table_name}에 적재 완료.")

# DAG 정의
dag = DAG(
    'crawl_and_upload_stock_data_dag',
    default_args=default_args,
    description='DAG for crawling stock data and uploading to Redshift',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# PythonOperator로 크롤링 작업 실행
crawl_stock_task = PythonOperator(
    task_id='crawl_stock_data_task',
    python_callable=crawl_stock_data,
    provide_context=True,
    dag=dag,
)

crawl_kospi_kosdaq_task = PythonOperator(
    task_id='crawl_kospi_kosdaq_data_task',
    python_callable=crawl_kospi_kosdaq_data,
    provide_context=True,
    dag=dag,
)

s3_upload_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=s3_upload_task_func,
    provide_context=True,
    dag=dag,
)

create_redshift_tables_task = PythonOperator(
    task_id='create_redshift_tables_task',
    python_callable=create_redshift_tables,
    provide_context=True,
    dag=dag,
)

upload_naver_news_to_redshift_task = PythonOperator(
    task_id='upload_naver_news_to_redshift_task',
    python_callable=upload_to_redshift,
    op_kwargs={'task_type': 'stock', 'table_name': 'naverNews'},
    provide_context=True,
    dag=dag,
)

upload_kospi_kosdaq_to_redshift_task = PythonOperator(
    task_id='upload_kospi_kosdaq_to_redshift_task',
    python_callable=upload_to_redshift,
    op_kwargs={'task_type': 'kospi_kosdaq', 'table_name': 'kospiKosdaqData'},
    provide_context=True,
    dag=dag,
)

# Task 순서 설정
crawl_stock_task >> crawl_kospi_kosdaq_task >> s3_upload_task >> create_redshift_tables_task >> [upload_naver_news_to_redshift_task, upload_kospi_kosdaq_to_redshift_task]