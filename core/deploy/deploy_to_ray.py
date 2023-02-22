import pandas as pd
import ray
import sys
import os
from ray import serve
from starlette.requests import Request
import json
import joblib
import mlflow
import logging
from inspect import signature


@serve.deployment
class ModelDeployment:

    def __init__(self, model_uri):
        model = self.load_model_file(model_uri)

        self.model = model

    def load_model_file(self, model_path):
        """
        模型统一使用mlflow方式加载
        :param model_path:
        :return:
        """
        try:
            model = mlflow.pyfunc.load_model(model_path)
            return model
        except Exception as e:
            print(e)
        raise Exception("模型文件不存在！")

    def _predict(self, df):

        predict_df = self.model.predict(df)

        return pd.DataFrame(predict_df)

    async def predict(self, df):
        result = self._predict(df)
        try:
            return result.to_json(orient="records")
        except:
            return result

    async def _process_request_data(self, request: Request) -> pd.DataFrame:
        body = await request.body()
        if isinstance(body, pd.DataFrame):
            return body
        return pd.read_json(json.loads(body))

    async def __call__(self, request: Request):
        df = await self._process_request_data(request)
        result = self._predict(df)
        try:
            return result.to_json(orient="records")
        except:
            return result


class RayServeDeploy:

    def __init__(self):

        try:
            # 设置终端显示情况，显示error级别错误, TODO 未能准确捕捉错误，暂时显示所有信息
            # ray.init(address="auto", logging_level=logging.FATAL, log_to_driver=False)
            ray.init(address="auto", ignore_reinit_error=True)
        except Exception as e:

            raise Exception("当前ray未配置")

    def help(self):
        pass

    def create_deployment(self, name, model_uri):

        try:
            ModelDeployment.options(name=name).deploy(model_uri)
        except Exception as e:
            print(e)
            sys.exit(1)
        return {"name": name}

    def list_deployments(self, **kwargs):
        return [{"name": name, "info": info} for name, info in serve.list_deployments().items()]

    def delete_deployment(self, name):
        if any(name == d["name"] for d in self.list_deployments()):
            serve.get_deployment(name).delete()

    def get_deployment(self, name):
        try:
            return {"name": name, "info": serve.list_deployments()[name]}
        except KeyError:
            raise Exception(f"No deployment with name {name} found")

    def predict(self, deployment_name, df):
        handle = serve.get_deployment(deployment_name).get_handle()
        predictions_json = ray.get(handle.predict.remote(df))
        return pd.read_json(predictions_json)


# 模块实现单例模式，防止重复调用启动ray
ray_serve = RayServeDeploy()
