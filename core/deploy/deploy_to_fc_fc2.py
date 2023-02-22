import logging

import pandas as pd
import os
import sys
import warnings

from mlflow.deployments import BaseDeploymentClient
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE

from pathlib import Path
from mlflow.tracking.artifact_utils import _download_artifact_from_uri

from core.utils import generate_client
from core.info import list_models as ziya_list_models


logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')


def target_help():
    # TODO: Improve
    help_string = (
        "TODO")
    return help_string


def run_local(name, model_uri, flavor=None, config=None):
    # TODO: implement
    raise MlflowException("mlflow-fc-serve does not currently "
                          "support run_local.")


class FCServeDeploy(BaseDeploymentClient):
    def __init__(self, uri):
        super().__init__(uri)

        try:
            self.service_name = generate_client.service_name
            self.client = generate_client.generate_fc_client()
        except Exception as e:
            print(e)
            sys.exit(1)

    def help(self):
        return target_help()

    # TODO 1.create_function 函数参数待优化。 2. index.py 文件需容错
    def create_function(self, func_name, model_uri, environmentVariables={'PYTHONUSERBASE': '/home/share/python'}):
        fc_model_list = ziya_list_models.list_fc_models()
        if func_name in fc_model_list["fc_models"]:
            raise Exception(f"model {func_name} already exists in FC")

        model_path = None
        index_path = Path(model_uri).parent

        path = Path(_download_artifact_from_uri(model_uri))
        for file in path.iterdir():
            if file.suffix == '.pkl':
                file_list = file.parts
                model_path = f'./{file_list[-2]}/{file_list[-1]}'
            else:
                pass

        if not model_path:
            raise Exception("模型文件不存在，请确认！")

        template_py = """
import pandas as pd
import joblib


def handler(event, context):

    model = joblib.load('model_path')
    
    df = pd.read_json(event.decode())
    result = model.predict(None, df)
    result = pd.DataFrame(result).to_json(orient='records')
    return result

"""

        template_py = template_py.replace('model_path', model_path)
        file_list = os.listdir(model_uri)
        if "MLmodel" not in file_list:
            # 说明不是MLflow 格式模型， 模板predict 方法只有一个参数df
            template_py = template_py.replace('None,', '')

        with open(f"{index_path}/index.py", 'w') as fp:
            fp.write(template_py)

        try:
            result = self.client.create_function(self.service_name, func_name, 'python3', 'index.handler',
                                                 codeDir=index_path,
                                                 instanceType="c1",
                                                 timeout=86400,
                                                 memorySize=32768,
                                                 instanceConcurrency=1,
                                                 environmentVariables=environmentVariables)
            return result.data
        except Exception as e:
            raise Exception(e)

    def create_deployment(self, func_name, model_uri, flavor=None, config=None):
        if flavor is not None and flavor != "python_function":
            raise MlflowException(
                message=(
                    f"Flavor {flavor} specified, but only the python_function "
                    f"flavor is supported by mlflow-fc-serve."),
                error_code=INVALID_PARAMETER_VALUE)

        self.create_function(func_name, model_uri)
        return {"name": func_name, "config": config, "flavor": "python_function"}

    def delete_deployment(self, func_name):
        result = self.client.delete_function(self.service_name, func_name)
        logger.info("Deleted model with name: {}".format(func_name))
        return result

    def update_deployment(self, name, model_uri=None, flavor=None,
                          config=None):
        if model_uri is None:
            pass
        else:
            self.delete_deployment(name)
            self.create_deployment(name, model_uri, flavor, config)

        return {"name": name, "config": config, "flavor": "python_function"}

    def list_deployments(self, **kwargs):

        def get_func_name(func_dict):
            return func_dict["functionName"]

        func_list = self.client.list_functions(self.service_name, limit=100)
        result_list = map(get_func_name, func_list.data['functions'])
        return list(result_list)

    def get_deployment(self, func_name):
        try:
            return {"name": func_name, "config": self.client.get_function(self.service_name, func_name).data}
        except KeyError:
            raise MlflowException(f"No deployment with name {func_name} found")

    def predict(self, deployment_name, df):
        data = df.to_json(orient='records')
        df_byte = self.client.invoke_function(self.service_name, deployment_name, payload=data.encode())
        df = pd.read_json(df_byte.data.decode())
        return df

