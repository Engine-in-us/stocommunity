from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import boto3
import json
import time
import os
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup


# Setting DAG 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'community_crawling_dag',
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    description='주식 커뮤니티(Clien, FMKorea) 게시글 크롤링 DAG',
    schedule_interval='@daily',
    catchup=False
)        

# Initialize WebDriver
def init_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # service = Service(ChromeDriverManager().install())
    # driver = webdriver.Chrome(service=service, options=options)
    driver = webdriver.Remote('remote_chromedriver:4444/wd/hub', options=options)
    driver.implicitly_wait(5)
    
    return driver

# Crawling FMkorea community posts
def get_fmkorea_posts(base_url):
    driver = init_driver()
    posts = []
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_start = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
    date_end = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    page = 1
    stop_crawling = False
    
    while not stop_crawling:
        url = f"{base_url}&page={page}"
        driver.get(url)
        driver.implicitly_wait(60) 
        
        articles = driver.find_elements(By.CSS_SELECTOR, '.bd_lst.bd_tb_lst.bd_tb .title a')
        
        if not articles:
            print("[FMKorea] No more articles found.")
            break
        
        for article in articles:
            if article.get_attribute('title') == "댓글":
                continue
                
            parent_tr = article.find_element(By.XPATH, "./../..")  # article의 부모 tr 태그를 찾기
            post_element = parent_tr.find_element(By.CSS_SELECTOR, ".time")
            post_str = post_element.text.strip()
                
            try:
                if ":" in post_str: # HH:MM 형태 시간 계산
                    post_time = datetime.strptime(post_str, "%H:%M").time()
                    post_date = datetime.combine(today.date(), post_time)
                    if post_date > today:  # HH:MM이 오늘 이후라면 어제로 조정
                        post_date = datetime.combine(yesterday.date(), post_time)
                else:
                    post_date = datetime.strptime(post_str, "%Y.%m.%d %H:%M")
            except Exception as e:
                print(f"[FMKorea] Date parsing failed for {post_str}: {e}")
                continue
                
            # 첫번째 어제자 게시물을 찾으면, 본문 크롤링만 진행
            if date_start <= post_date <= date_end:
                print("[FMKorea] Found first yesterday post, starting detailed crawls...")
                article.click()
                time.sleep(2)
                
                while True:
                    title = driver.find_element(By.CSS_SELECTOR, 'h1.np_18px').text.strip()
                    url = driver.current_url
                    time_element = driver.find_element(By.CSS_SELECTOR, '.date')
                    time_str = time_element.text.strip()
                    date = datetime.strptime(time_str, "%Y.%m.%d %H:%M")
                    
                    if date < date_start:
                        stop_crawling = True
                        break
                    
                    content_element = driver.find_element(By.CSS_SELECTOR, '.rd_body article')
                    content = content_element.text.strip() if content_element else "No content"
                    
                    posts.append({
                        'title': title,
                        'date': date.strftime("%Y-%m-%d %H:%M"),
                        'text': content,
                        'link': url
                    })
                    
                    # 다음 게시물로 계속 넘어가기 (버튼 클릭 event)
                    next_button = driver.find_element(By.CSS_SELECTOR, 'div.prev_next_btns span.btn_pack.next.blockfmcopy')
                    driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    next_button.click()
                    time.sleep(0.5)
                
                if stop_crawling:
                    print("[FMKorea] Crawling stopped. All posts from yesterday have been fetched.")
                    break
        page += 1
    
    driver.quit()
    return posts

# Crawling Clien community posts
def get_clien_posts(base_url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    today = datetime.now()
    date_start = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
    date_end = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    posts = []
    page = 0
    
    while True:
        url = f"{base_url}?&od=T31&category=0&po={page}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break
        
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.select('div.list_item.symph_row')
        
        if not articles:
            print("[Clien] No more articles found.")
            break
        
        for article in articles:
            title_element = article.select_one('span.subject_fixed')
            date_element = article.select_one('span.timestamp')
            link_element = article.select_one('a.list_subject')
            
            if not title_element or not date_element or not link_element:
                continue
            
            title = title_element.get_text(strip=True)
            link = f"https://www.clien.net{link_element['href']}"
            date_str = date_element.get_text(strip=True)
            post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

            if post_date < date_start:
                print("[Clien] No more posts from yesterday.")
                return posts

            if date_start <= post_date <= date_end:
                # Crawling post content
                post_response = requests.get(link, headers=headers)
                if post_response.status_code != 200:
                    print(f"[Clien] Failed to fetch post: {link}")
                    continue
                
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                content_element = post_soup.select_one('div.post_article')
                content = content_element.get_text(strip=True) if content_element else "No content"
                
                posts.append({
                    'title': title,
                    'date': post_date.strftime("%Y-%m-%d %H:%M"),
                    'text': content,
                    'link' : link
                })
        page += 1
    return posts

# Process of crawling and Save data
def crawl_and_save_data(**kwargs):
    ti = kwargs['ti']
    clien_url = "https://www.clien.net/service/board/cm_stock"
    fm_url = "https://www.fmkorea.com/index.php?mid=stock&category=2997203870"
    
    clien_posts = get_clien_posts(clien_url)
    fm_posts = get_fmkorea_posts(fm_url)
    
    crawling_df = pd.DataFrame(clien_posts + fm_posts)
    csv_path = f"/tmp/community_crawling_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    crawling_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print("[Done] Create community crawling csv file.")
    ti.xcom_push(key='csv_path', value=csv_path)
    
    
# Upload to S3
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='crawl_and_save_data', key='csv_path')

    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_file(
        filename=csv_path,
        bucket_name='team6-s3',
        replace=True,
        key=f"raw_data/{os.path.basename(csv_path)}"
    )

crawl_and_save_task = PythonOperator(
    task_id='crawl_and_save_data',
    python_callable=crawl_and_save_data,
    dag=dag
)

upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

crawl_and_save_task >> upload_s3_task