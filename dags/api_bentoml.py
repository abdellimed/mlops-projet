
import os
import sys
import warnings
from os.path import dirname, abspath
from datetime import datetime, timedelta
from os.path import dirname, abspath
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, get_current_context,PythonOperator

import pandas as pd
import numpy as np

import sklearn
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import subprocess

model_uri = 'runs:/936d7390adc0432884958e35e3e0abd9/model'
port=3000
tutorial_dir_path = dirname(dirname(abspath(__file__)))

bento_path = os.path.join(tutorial_dir_path, "bentoml")

def importmlflow():
    import mlflow
    import bentoml
    

   # registered_model = mlflow.register_model(
      #      model_uri, "Classifier")
  # Load model as a PyFuncModel.
    #loaded_model = mlflow.pyfunc.load_model(model_uri)
  #  mlflow.pyfunc.load_model("models:/classification/Production")
    bentoml.mlflow.import_model(
             'comment-classifier',model_uri,
            signatures={"predict":{"batchable":True}}
        )
    return True

    

# Fonction pour installer les dépendances
def install_dependencies():
    import subprocess
    packages = ['numpy', 'typing', 'pandas','pydantic']
    for package in packages:
        subprocess.check_call(['pip', 'install', package])

# Appel de la fonction pour installer les dépendances



def create_service_file():
    import os
    import subprocess
    import logging


    content = '''
import numpy as np
import bentoml
import typing
import pandas as pd
from pydantic import BaseModel
from bentoml.io import JSON
from bentoml.io import NumpyNdarray

runner = bentoml.mlflow.get("comment-classifier:latest").to_runner()

svc = bentoml.Service('comment-classifier', runners=[runner])

class Features(BaseModel):
    comment: str

input_spec = JSON(pydantic_model=Features)
@svc.api(input=input_spec, output=JSON())
def predict(input_text: Features):
    input_df = pd.DataFrame([input_text.dict()])
    return {'res':runner.predict.run(input_df)[0]}
    '''

    # Ouvrir le fichier en mode écriture
    with open("service.py", "w") as file:
        # Écrire le contenu dans le fichier
        file.write(content)
    
    #os.system('ls')
   # command = ['ls']
    command = ['bentoml', 'serve', 'service:svc','--port','3200']
    #command = ['cat','service.py']
    try:
        output = subprocess.check_output(command, text=True, stderr=subprocess.STDOUT)
        logging.info(f"Command output: {output}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Command execution failed with error code {e.returncode}. Output: {e.output}")

    

    


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
   # mlflow.set_experiment("projet")
   # mlflow.sklearn.autolog(silent=True, log_models=False)
    mlflow.set_experiment("mlops1")
    os.environ["AWS_ACCESS_KEY_ID"] = "mlflow_access"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mlflow_secret"
    os.environ["AWS_REGION"]="us-east-1"

   # os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'

    # We start by versioning our data and code to make sure our results can be traced back to the data and code that generated it.
    # We assume the latest data has been loaded to the .csv files under the data folder.
    # Our data does not change, but in a practical scenario, the data could have changed since last run.
    
    t0= PythonOperator(
        task_id="install",
        python_callable=install_dependencies
    )


    t1= PythonOperator(
        task_id="create_service_file",
        python_callable=create_service_file
    )

  

    t3= ShortCircuitOperator(
        task_id="import_mlflowmodel",
        python_callable=importmlflow
     )

    t5=BashOperator(
    task_id='bento_models',
    bash_command="bentoml models list")

 
t0 >> t3 >> t5 >>  t1
