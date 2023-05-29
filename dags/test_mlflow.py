
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

model_uri = 'runs:/bbaac885032a4f09a18ef6188a54cbfa/model'

def importmlflow():
    import mlflow
    import bentoml
    mlflow.set_experiment("Test_model")

   # registered_model = mlflow.register_model(
      #      model_uri, "Classifier")
    logged_model = 'runs:/bbaac885032a4f09a18ef6188a54cbfa/model'

# Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)
  #  mlflow.pyfunc.load_model("models:/classification/Production")
    bentoml.mlflow.import_model(
             'modelsk',logged_model,
            signatures={"predict":{"batchable":True}}
        )
   
    return True

def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


tutorial_dir_path = dirname(dirname(abspath(__file__)))

def _train_model():
    import bentoml
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    # Read the wine-quality csv file (make sure you're running this from the root of MLflow!)
    #wine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wine-quality.csv")
    #wine_path= os.path.join(tutorial_dir_path, "data/wine-quality.csv")
    data = pd.read_csv("/data/wine-quality.csv")

    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    alpha =  0.5
    l1_ratio = 0.5

    with mlflow.start_run() as run:
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        predicted_qualities = lr.predict(test_x)

        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, "model")


        bento_model = bentoml.sklearn.save_model('kneighbors', lr)
       # registered=mlflow.register_model("runs:/{}/model".format(run.info.run_id),'modelregistred')
     #   bentoml.mlflow.import_model(
      #      'modelsk',registered.source,
       #     signatures={"predict":{"batchable":True}}
        #)
      
    return True



with DAG(
    'test_mlflow',
    description="Pipeline for training and deploying a classifier of toxic comments",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["tutorial"]
) as dag:
    import os
    import mlflow
   # mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("Test_model")
    mlflow.sklearn.autolog(silent=True, log_models=False)
    os.environ["AWS_ACCESS_KEY_ID"] = "mlflow_access"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "mlflow_secret"
    os.environ["AWS_REGION"]="us-east-1"

   # os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'

    # We start by versioning our data and code to make sure our results can be traced back to the data and code that generated it.
    # We assume the latest data has been loaded to the .csv files under the data folder.
    # Our data does not change, but in a practical scenario, the data could have changed since last run.

    t4 = ShortCircuitOperator(
        task_id="train_model",
        python_callable=_train_model
    )
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

t4 >> t2 >> t3 >> t5
