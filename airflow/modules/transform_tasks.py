import pandas as pd

def process_data(**kwargs):
    task_type = kwargs["task_type"]
    columns = kwargs["columns"]  # 컬럼 이름 매핑
    transformations = kwargs.get("transformations", {})  # 변환 로직 정의

    # XCom에서 데이터 가져오기
    data = kwargs['ti'].xcom_pull(key=f"{task_type}_data", task_ids=f"fetch_{task_type}_data")
    if not data:
        raise Exception(f"{task_type} 데이터가 존재하지 않습니다.")

    # DataFrame 생성
    df = pd.DataFrame(data, columns=columns)

    # 변환 작업 수행 (컬럼별로 커스터마이징)
    for col, transform in transformations.items():
        if col in df.columns:
            df[col] = df[col].apply(transform)

    # XCom에 처리된 데이터 저장
    kwargs['ti'].xcom_push(key=f"{task_type}_processed_data", value=df.to_dict(orient="records"))
