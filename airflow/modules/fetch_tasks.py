import requests

def fetch_data(**kwargs):
    from modules.connections import get_connections

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
    data = response.json().get("output", [])

    # 상위 10개 데이터만 가져오기
    top_10_data = data[:10]

    # XCom에 저장
    kwargs['ti'].xcom_push(key=f"{task_type}_data", value=top_10_data)
