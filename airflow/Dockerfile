FROM ubuntu:22.04

# env
ENV AIRFLOW_HOME="/opt/airflow"
ENV AIRFLOW__WORKER__NAME="worker_node"

ENV AIRFLOW__MASTER__IP="ec2-43-200-116-182.ap-northeast-2.compute.amazonaws.com"
ENV AIRFLOW__REDIS__HOST=${AIRFLOW__MASTER__IP}:6379
ENV AIRFLOW__POSTGRESQL__HOST=${AIRFLOW__MASTER__IP}:5432
ENV AIRFLOW__CORE__EXECUTOR="CeleryExecutor"

ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://postgres:postgres@team6-postgres-for-airflow.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com:5432/airflow"
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://postgres:postgres@team6-postgres-for-airflow.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com:5432/airflow"
ENV AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://postgres:postgres@team6-postgres-for-airflow.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com:5432/airflow"
ENV AIRFLOW__CELERY__BROKER_URL="redis://:@${AIRFLOW__REDIS__HOST}/0"
ENV AIRFLOW__LOGGING__BASE_LOG_FOLDER="${AIRFLOW_HOME}/log"
ENV AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY="${AIRFLOW_HOME}/log/scheduler"
ENV AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION="${AIRFLOW_HOME}/log/dag_processor_manager/dag_processor_manager.log"
ENV AIRFLOW__CORE__HOSTNAME_CALLABLE="airflow.utils.net.get_host_ip_address"
ENV AIRFLOW__CORE__FERNET_KEY="yCd2SFMy6F0YHrF62N3XhA0gYe0cOTRqUXssh6FjyCk="
ENV AIRFLOW__WEBSERVER__SECRET_KEY="b'M\x9e\xe3\x9d\xf4\x1c:B\x82\xbb\xea\x0fl\x86\xfe|'"
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="true"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="false"
ENV AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth"
ENV AIRFLOW__FLOWER__AUTHENTICATED_API="true"
ENV _PIP_ADDITIONAL_REQUIREMENTS="pandas requests s3fs boto3 webdriver_manager"

ARG DEBIAN_FRONTEND=noninteractive

# python3.9 설치
RUN apt-get update  \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get install python3.9 python3.9-distutils python3-pip -y \
    && python3.9 -m pip install --upgrade pip \
    && python3.9 -m pip install cffi wheel \
    && apt-get install -y wget \
    && apt-get install -y gnupg2 \
    && apt-get install -y vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/*

# install airflow
# https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.9.txt
RUN python3.9 -m pip install importlib-metadata==6.8.0
RUN python3.9 -m pip install apache-airflow[celery]==2.7.3
RUN python3.9 -m pip install psycopg2-binary==2.9.9
RUN python3.9 -m pip install Redis==4.6.0
RUN python3.9 -m pip install Flask-Session==0.5.0
RUN python3.9 -m pip install pandas==2.1.2
RUN python3.9 -m pip install requests==2.31.0
RUN python3.9 -m pip install s3fs
RUN python3.9 -m pip install boto3==1.28.64
RUN python3.9 -m pip install selenium
RUN python3.9 -m pip install pyperclip
RUN python3.9 -m pip install webdriver_manager
RUN pip install --ignore-installed apache-airflow-providers-amazon
RUN pip install apache-airflow-providers-postgres
RUN airflow db check && echo "DB connection successful!" || echo "DB connection failed!"
RUN echo "DB Connection String: $AIRFLOW__CORE__SQL_ALCHEMY_CONN"
RUN printenv | grep AIRFLOW
RUN airflow db init
RUN mkdir ${AIRFLOW_HOME}/dags
RUN mkdir ${AIRFLOW_HOME}/plugins
RUN mkdir ${AIRFLOW_HOME}/data

# Google Chrome 설치
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' && \
    apt-get update && \
    apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* \
    && ls -l /usr/bin/google-chrome \
    && /usr/bin/google-chrome --version