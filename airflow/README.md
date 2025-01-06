# [Celery Executor](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html)
- resource issue로 인하여 worker를 개별 instance로 분리
- celery executor 기반의 airflow 구성


# Prerequisites
- Install docker, docker compose
- master node에는 scheduler, webserver, trigger, flower, redis
- worker node는 필요에 따라 scale out 할 것 (default 3로 구성)
- metadata db는 rds로 구성

# docker-compose.yaml in master node
- [docker-compose.yaml](./docker-compose.yaml)에 worker를 제외하고 설정(=`master node`)
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__SQL_ALCHEMY_CONN`, `AIRFLOW__CELERY__RESULT_BACKEND`에는 사전 구성한 RDS instance 정보로 설정
- 변경 사항이 있을 시 아래와 같이 각 컨테이너 적용
```angular2html
$ sudo docker compose up --build --force-recreate -d airflow-init
$ sudo docker compose up --build --force-recreate -d 
$ sudo docker compose up --build --force-recreate -d flower
```

# Dockerfile in worker node
[Dockerfile](./Dockerfile)에서 Worker를 구성할 dockerfile을 만들어 실행
## build
```angular2html
$ VERSION=0.0.1
$ cd /home/ec2-user/airflow_worker
$ docker build . -t airflow_docker:${VERSION}
```

## deploy
host는 worker node에 따라 달리 설정 (ex. worker1, worker2...)
```angular2html
$ VERSION=0.0.1
$ host=worker1
$ docker run -d -it --restart=always --name worker1 -p 8080:8080 \
-v /home/ec2-user/airflow_worker/dags:/opt/airflow/dags \
-v /home/ec2-user/airflow_worker/plugins:/opt/airflow/plugins \
-v /home/ec2-user/airflow_worker/log:/opt/airflow/log \
-v /etc/localtime:/etc/localtime:ro -e TZ=Asia/Seoul \
airflow_docker:${VERSION} \
airflow celery worker -H ${host} -q queue1
```