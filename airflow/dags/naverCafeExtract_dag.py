from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pyperclip
import pyautogui
import pandas as pd
import json
import time
import os
from datetime import datetime
import platform


# .env 파일 로드 (Airflow Variable 사용)
def load_env(**kwargs):
    creds = BaseHook.get_connection('naver_env')  # Airflow Connection에서 불러오기
    os.environ['NAVER_ID'] = creds.login
    os.environ['NAVER_PW'] = creds.password


# 크롬 드라이버 초기화
def init_driver():
    options = Options()
    options.add_experimental_option("detach", True)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver


def paste_clipboard():
    if platform.system() == "Darwin":  # macOS
        pyautogui.hotkey("command", "v")
    else:  # Windows or Linux
        pyautogui.hotkey("ctrl", "v")


# 네이버 로그인
def login(driver):
    driver.get("https://nid.naver.com/nidlogin.login")
    time.sleep(2)

    pyperclip.copy(os.getenv('NAVER_ID'))
    paste_clipboard()
    time.sleep(1)

    pyperclip.copy(os.getenv('NAVER_PW'))
    paste_clipboard()
    time.sleep(1)

    driver.find_element(By.CSS_SELECTOR, "#log\\.login").click()
    time.sleep(3)


# 게시글 크롤링
def crawl_posts():
    driver = init_driver()
    login(driver)

    url = "https://cafe.naver.com/geobuk2/ArticleList.nhn?search.clubid=26251287&search.menuid=98&search.boardtype=L"
    driver.get(url)
    driver.switch_to.frame("cafe_main")

    posts = []
    for i in range(5):  # 테스트용으로 5개 크롤링
        try:
            title = driver.find_element(By.CLASS_NAME, "title_text").text
            content = driver.find_element(By.CLASS_NAME, 'se-main-container').text
            posts.append({'title': title, 'content': content})

            next_btn = driver.find_element(By.CSS_SELECTOR, "a.BaseButton.btn_next")
            next_btn.click()
            time.sleep(2)
        except Exception as e:
            print(f"크롤링 실패: {e}")
            break

    driver.quit()
    return posts


# 키워드 매칭
def match_keywords(**kwargs):
    ti = kwargs['ti']
    posts = ti.xcom_pull(task_ids='crawl_posts')

    # alias_dict 불러오기
    with open('/opt/airflow/data/alias_dict.json', 'r', encoding='utf-8') as f:
        alias_dict = json.load(f)

    matched_results = []
    for post in posts:
        text = post['title'] + ' ' + post['content']
        matches = [key for key, aliases in alias_dict.items() if any(alias in text for alias in aliases)]
        matched_results.append({'title': post['title'], 'matches': matches})

    df = pd.DataFrame(matched_results)
    csv_path = f"/tmp/naver_cafe_matching_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_path, index=False)

    # XCom에 파일 경로 전달
    ti.xcom_push(key='csv_path', value=csv_path)


# S3 업로드
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='match_keywords', key='csv_path')

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename=csv_path,
        bucket_name='team6-s3',
        replace=True,
        key=f"Test/navercafe/{os.path.basename(csv_path)}"
    )


# DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'naver_cafe_crawling_dag',
    default_args=default_args,
    description='네이버 카페 게시글 크롤링 및 주식 키워드 매칭 DAG',
    schedule_interval=None,  # 최초 1회 실행
    catchup=False,
)

# Task 정의
load_env_task = PythonOperator(
    task_id='load_env',
    python_callable=load_env,
    dag=dag,
)

crawl_task = PythonOperator(
    task_id='crawl_posts',
    python_callable=crawl_posts,
    dag=dag,
)

match_task = PythonOperator(
    task_id='match_keywords',
    python_callable=match_keywords,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# Task 실행 순서
load_env_task >> crawl_task >> match_task >> upload_task