"""
该模块是根据mlflow格式对 aliyun_server.py 模块的外层封装
主要class 类 FCServeDeploy 只在deploy 时候使用：需要模型路径
"""

import logging

import pandas as pd
import os
import sys
import warnings
import zipfile

from mlflow.deployments import BaseDeploymentClient
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE

from pathlib import Path
from mlflow.tracking.artifact_utils import _download_artifact_from_uri

from core.utils import generate_client
from core.info import list_models as ziya_list_models
from core.deploy import aliyun_serve

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')


def target_help():
    help_string = (
        "TODO")
    return help_string


def run_local(name, model_uri, flavor=None, config=None):
    raise MlflowException("mlflow-fc-serve does not currently "
                          "support run_local.")


class FCServeDeploy(BaseDeploymentClient):
    def __init__(self, uri):
        super().__init__(uri)
        self.uri = uri

        try:
            self.service_name = generate_client.service_name
            # self.client = generate_client.generate_fc_client()
            self.fc = aliyun_serve.ALiYunFCServe()
        except Exception as e:
            print(e)
            sys.exit(1)

    def help(self):
        return target_help()

    def zip_dir(self, dir_path, out_path):
        """
        压缩指定文件夹
        :param dir_path: 目标文件夹路径
        :param out_path: 压缩文件保存路径
        :return: 无
        """
        zip_ = zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED)
        for path, dir_names, file_names in os.walk(dir_path):
            # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
            f_path = path.replace(dir_path, "")

            for filename in file_names:
                zip_.write(os.path.join(path, filename), os.path.join(f_path, filename))

        zip_.close()

    def create_function(self, func_name):
        fc_model_list = ziya_list_models.list_fc_models()
        if func_name in fc_model_list["fc_models"]:
            raise Exception(f"model {func_name} already exists in FC")

        model_uri = self.uri

        path = Path(_download_artifact_from_uri(model_uri))
        local_model_name = path.parts[-1]

        # 1. 根据模型文件创建 index.py
        template_py = """
from flask import Flask
from flask import request
import mlflow
import pandas as pd

app = Flask(__name__)


@app.route('/invoke', methods=['POST'])
def main():
    model = mlflow.pyfunc.load_model('../model_path')
    request_data = request.stream.read().decode().rstrip('\\r')

    df = pd.read_json(request_data)
    print("input data shape is ...")
    print(df.shape)
    result = model.predict(df)
    result = pd.DataFrame(result).to_json(orient="records")
    return result


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)

    """
        template_py = template_py.replace('model_path', local_model_name)

        # 1. 在模型文件夹中创建index.py 文件
        with open(f"{model_uri}/app.py", 'w') as fp:
            fp.write(template_py)

        # 2. 将模型文件与index.py 打包压缩成OSS code
        input_path = model_uri
        output_path = f"{Path(model_uri).parent}/{func_name}.zip"

        self.zip_dir(input_path, output_path)

        # 4. 将压缩文件上传至OSS
        my_oss = generate_client.generate_oss_client()
        oss_file_name = f"code/{func_name}.zip"
        my_oss.put_object_from_file(oss_file_name, output_path)

        # 5. 根据新版sdk 创建函数

        bucket_name = generate_client.bucket_name
        service_name = generate_client.service_name

        self.fc.create_func(bucket_name, oss_file_name, service_name, func_name)

        # 6. 删除模型文件中生成的indxe.py 以及 压缩包
        os.remove(f"{model_uri}/app.py")
        os.remove(output_path)

    def create_deployment(self, func_name, model_uri, flavor=None, config=None):
        if flavor is not None and flavor != "python_function":
            raise MlflowException(
                message=(
                    f"Flavor {flavor} specified, but only the python_function "
                    f"flavor is supported by mlflow-fc-serve."),
                error_code=INVALID_PARAMETER_VALUE)

        # self.create_function(func_name, model_uri)
        # 使用新版sdk部署函数
        self.create_function(func_name)

        return {"name": func_name, "config": config, "flavor": "python_function"}

    def delete_deployment(self, func_name):
        result = self.fc.delete_func(self.service_name, func_name)
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

        # def get_func_name(func_dict):
        #     return func_dict["functionName"]
        #
        # func_list = self.client.list_functions(self.service_name, limit=100)
        # result_list = map(get_func_name, func_list.data['functions'])
        # return list(result_list)
        func_list = self.fc.list_func(self.service_name)
        return func_list

    def get_deployment(self, func_name):
        try:
            return {"name": func_name, "config": self.fc.get_func(self.service_name, func_name)}
        except KeyError:
            raise MlflowException(f"No deployment with name {func_name} found")

    def predict(self, deployment_name, df: pd.DataFrame):

        df_byte = self.fc.invoke_func(self.service_name, deployment_name, df)

        df = pd.read_json(df_byte)
        return df




