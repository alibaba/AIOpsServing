import pandas as pd
import requests

import sys
from core.utils import generate_client
from core.utils.oss_util import OssUtil
from core.deploy import aliyun_serve
from core.deploy import deploy_to_docker


def get_func_name(func_dict):
    return func_dict["functionName"]


def _run_model(model_name_version, backend, df):
    backend = backend.lower()
    fc_service = generate_client.service_name
    if backend == "ray":
        try:
            client = generate_client.generate_ray_client()
        except Exception as e:
            print(e)
            sys.exit(1)

        model_dict = client.list_deployments()
        model_list = [instance["name"] for instance in model_dict]
        if model_name_version not in model_list:
            raise Exception(f"model {model_name_version} not exists in {backend}")

        result_df = client.predict(model_name_version, df)
        result_df = pd.DataFrame(result_df)
        return result_df

    elif backend == "fc":
        try:
            client = generate_client.generate_aliyun_client()
        except Exception as e:
            print(e)
            sys.exit(1)

        fc_client = aliyun_serve.ALiYunFCServe()
        service_name = generate_client.service_name

        model_list = fc_client.list_func(service_name)

        if model_name_version not in model_list:
            raise Exception(f"model {model_name_version} not exists in {backend}")

        result_df = fc_client.invoke_func(fc_service, model_name_version, df)

        result_df = pd.read_json(result_df)
        return result_df

    elif backend == "docker":
        docker_client = deploy_to_docker.DockerServeDeploy()
        docker_list = docker_client.get_container_list()
        if model_name_version not in docker_list:
            raise Exception(f"model {model_name_version} not exists in {backend}")

        result_df = docker_client.predict_by_docker(model_name_version, df)

        result_df = pd.read_json(result_df)
        return result_df

    else:
        raise Exception("指定后端错误，目前可选择计算后端[fc/ray/odps]")


def _run_model_odps(model_name_version, backend, table_name, field_tuple, option=None):
    backend = backend.lower()

    if backend != "odps":
        raise Exception("指定计算后端错误")
    try:
        client = generate_client.generate_odps_client()
    except Exception as e:
        print(e)
        sys.exit(1)

    sql_str = """
    select {},{}({}) as predict from {};
    """
    if not option:
        tmp_sql = sql_str.format(model_name_version, field_tuple, table_name)
    else:
        tmp_sql = f""" select {model_name_version}({field_tuple}) as predict from {table_name} where {option}"""
    print("generate sql ---------> \n", tmp_sql)
    instance = client.execute_sql(tmp_sql)

    with instance.open_reader(tunnel=True) as reader:

        pd_df = reader.to_result_frame()

    tmp_df = pd.DataFrame(pd_df)
    tmp_df["predict"] = tmp_df["predict"].astype(int)

    return tmp_df['predict']


def run_main_backend(model_name_version, backend, file_type, file_name):
    if file_type == "local":
        df = pd.read_json(file_name)
    elif file_type == "oss":
        o = OssUtil()
        df = o.get_df_from_oss(file_name)
    else:
        return "指定计算后端错误"

    if backend == "fc" or backend == "ray" or backend == "docker":
        try:
            result_df = _run_model(model_name_version, backend, df)
            return result_df
        except Exception as e:
            return e
    else:
        return "指定计算后端错误"


def run_with_server(model_name_version, file_type, file_name):
    # 使用ziya server api 接口调用模型
    if file_type == "local":
        df = pd.read_json(file_name)
    elif file_type == "oss":
        o = OssUtil()
        df = o.get_df_from_oss(file_name)
    else:
        return "指定计算后端错误"

    url = "http://ziya.alibaba-inc.com/model/{}".format(model_name_version)
    data = df.to_json(orient="records")
    response = requests.post(url, data)
    result = response.content
    return result
