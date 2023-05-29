
import os
import sys
import warnings
from os.path import dirname, abspath
from datetime import datetime, timedelta
from os.path import dirname, abspath
os.system('pip3 install sklearn mlflow')
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, get_current_context

import pandas as pd
import numpy as np

import sklearn
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn
import mlflow.pyfunc

model_uri = 'runs:/0bc1297e92044188b0c43085a37bb9fb/model'
port=3000
tutorial_dir_path = dirname(dirname(abspath(__file__)))

bento_path = os.path.join(tutorial_dir_path, "bentoml")

def importmlflow():
    import mlflow
    import bentoml
    mlflow.set_experiment("Test_model")

   # registered_model = mlflow.register_model(
      #      model_uri, "Classifier")
  # Load model as a PyFuncModel.
    #loaded_model = mlflow.pyfunc.load_model(model_uri)
  #  mlflow.pyfunc.load_model("models:/classification/Production")
    bentoml.mlflow.import_model(
             'comment-classifier',model_uri,
            signatures={"predict":{"batchable":True}}
        )
    print(dirname(dirname(abspath(__file__))))
    return True






with DAG(
    'test_bentoml',
    description="Pipeline for training and deploying a classifier of toxic comments",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["tutorial"]
) as dag:
    import os
    import mlflow
   # mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("projet")
    mlflow.sklearn.autolog(silent=True, log_models=False)
    os.environ["AWS_ACCESS_KEY_ID"] = "mlflow_access"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mlflow_secret"
    os.environ["AWS_REGION"]="us-east-1"

   # os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'

    # We start by versioning our data and code to make sure our results can be traced back to the data and code that generated it.
    # We assume the latest data has been loaded to the .csv files under the data folder.
    # Our data does not change, but in a practical scenario, the data could have changed since last run.


    t2= ShortCircuitOperator(
        task_id="bento_model",
        python_callable=importmlflow
    )
    t3=BashOperator(
    task_id='bento_version',
    bash_command="bentoml --version")

    t5=BashOperator(
    task_id='bento_models',
    bash_command="bentoml models list")

    t6=BashOperator(
    task_id='bento_serve',
    bash_command="bentoml serve service:svc --port 3000"
    )

t2 >> t3 >> t5 >>  t6
