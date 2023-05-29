FROM apache/airflow:2.5.1
USER root
RUN apt-get update 
USER airflow
RUN pip install scikit-learn  
RUN pip install -U bentoml>=1.0.15
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt