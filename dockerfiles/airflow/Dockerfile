FROM apache/airflow:2.5.1-python3.9
USER root
RUN apt-get update && apt-get -y install libpq-dev gcc 
USER airflow
RUN pip install psycopg2
RUN pip install scikit-learn  
RUN pip install -U bentoml>=1.0.15
RUN pip install mlflow boto3 pymysql 
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
