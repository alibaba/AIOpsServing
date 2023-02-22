# 所有的评测均调用内部接口
import time
import hug
import pandas as pd
import sys

from core.utils import generate_client
from core.deploy import aliyun_serve
from core.deploy.deploy_to_docker import DockerServeDeploy
from sklearn.metrics import recall_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import f1_score

from odps import options

options.sql.settings = {'odps.sql.python.version': 'cp37'}
options.tunnel.limit_instance_tunnel = True


def timer(function):
    """
    装饰器函数timer
    :param function:想要计时的函数
    :return: 模型结果，计算总耗时
    """

    def wrapper(*args, **kwargs):
        time_start = time.time()
        res = function(*args, **kwargs)
        cost_time = time.time() - time_start
        # print("【%s】运行时间：【%s】秒" % (function.__name__, cost_time))
        return res, cost_time

    return wrapper


# 1. 模型在指定计算后端不存在
# 2. 模型调用超时
# 3. 模型不属于二分类场景（计算混淆矩阵）
# 4. 模型因为数据的原因调用出错（数据字段格式，数据shape）


# 获取 FC 存在的模型名称
def get_func_name(func_dict):
    return func_dict["functionName"]


def check_model_exist(computing_backend, model_name_version):
    # 判断模型在指定计算后端是否存在
    computing_backend = computing_backend.lower()
    if computing_backend == "fc":
        try:
            client = generate_client.generate_aliyun_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        service_name = generate_client.service_name
        fc = aliyun_serve.ALiYunFCServe()
        model_list = fc.list_func(service_name)

        if model_name_version in model_list:
            return fc
        else:
            raise Exception(f"当前指定模型{model_name_version}未部署在后端{computing_backend}上，请确认模型名称或重新部署后重试！")
    elif computing_backend == "ray":
        try:
            client = generate_client.generate_ray_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        model_dict = client.list_deployments()
        model_list = [instance["name"] for instance in model_dict]

        if model_name_version in model_list:
            return client
        else:
            raise Exception(f"当前指定模型{model_name_version}未部署在后端{computing_backend}上，请确认模型名称或重新部署后重试！")

    elif computing_backend == "docker":
        try:
            client = generate_client.generate_docker_client()
        except Exception as e:
            print(e)
            sys.exit(1)

        docker_client = DockerServeDeploy()
        docker_list = docker_client.get_container_list()
        if model_name_version in docker_list:
            return docker_client
        else:
            raise Exception(f"当前指定模型{model_name_version}未部署在后端{computing_backend}上，请确认模型名称或重新部署后重试！")

    elif computing_backend == "odps":
        try:
            client = generate_client.generate_odps_client()
        except Exception as e:
            print(e)
            sys.exit(1)

        odps_funcs = client.list_functions()
        model_list = [func.name for func in odps_funcs]
        if model_name_version in model_list:
            return client
        else:
            raise Exception(f"当前指定模型{model_name_version}未部署在后端{computing_backend}上，请确认模型名称或重新部署后重试！")

    else:
        raise Exception(f"请输入正确后端，目前仅支持[ray、fc、odps]")


@timer
def predict_with_fc(client, model_name_version, df):
    """
    通过内部接口调用FC
    :param client: FC client
    :param model_name_version: 模型名称以及版本号
    :param df: 模型输入数据
    :return: 模型预测结果
    """
    data = df.to_json(orient='records')
    service_name = generate_client.service_name
    try:
        df_byte = client.invoke_func(service_name, model_name_version, df)
        result = pd.read_json(df_byte)
        return result
    except Exception as e:
        raise Exception(e)


@timer
def predict_with_ray(client, model_name_version, df):
    """
    通过内部接口调用 RAY
    :param client: RAY client
    :param model_name_version: 模型名称以及版本号
    :param df: 模型输入数据
    :return: 模型预测结果
    """
    try:
        result = client.predict(model_name_version, df)
        result = pd.DataFrame(result)
        return result
    except Exception as e:
        raise Exception(e)

@timer
def predict_with_docker(client, model_name_version, df):
    """
    :param client:
    :param model_name_version:
    :param df:
    :return:
    """
    try:
        result = client.predict_by_docker(model_name_version, df)
        result = pd.read_json(result)
        return result
    except Exception as e:
        raise Exception(e)


@timer
def predict_with_odps(client, model_name_version, table_name, field_tuple, y_true, option=None, limit=1000):
    # 涉及表的处理
    sql_str = """
    select {},{}({}) as predict from {};
    """
    if not option:
        tmp_sql = sql_str.format(y_true, model_name_version, field_tuple, table_name)
    else:
        tmp_sql = f""" select {y_true} {model_name_version}({field_tuple}) as predict from {table_name} where {option}"""
    print("generate sql ---------> \n", tmp_sql)
    instance = client.execute_sql(tmp_sql)

    with instance.open_reader(tunnel=True) as reader:

        pd_df = reader.to_result_frame()

    tmp_df = pd.DataFrame(pd_df)
    tmp_df["predict"] = tmp_df["predict"].astype(int)

    # 修改数据类型
    if tmp_df[y_true][0] == "false" or tmp_df[y_true][0] == "true":
        tmp_df["new"] = tmp_df[y_true].apply(lambda x: 0 if x == "false" else 1)
        return tmp_df["new"], tmp_df["predict"]
    return tmp_df[y_true], tmp_df['predict']


def calculate(y_true, y_predict):
    # 计算指标主函数，TODO 目前只有二分类的计算方式

    acc = accuracy_score(y_true, y_predict)
    precision = precision_score(y_true, y_predict)
    recall = recall_score(y_true, y_predict)
    f1 = f1_score(y_true, y_predict)

    return acc, precision, recall, f1


def get_benchmark_result(computing_backend, client, model_name_version, df, y_true):
    if computing_backend == "fc":
        try:
            y_predict, time_cost = predict_with_fc(client, model_name_version, df)
        except Exception as e:
            print(e)
            sys.exit(1)
    elif computing_backend == "ray":
        try:
            y_predict, time_cost = predict_with_ray(client, model_name_version, df)
        except Exception as e:
            print(e)
            sys.exit(1)
    elif computing_backend == "docker":
        try:
            y_predict, time_cost = predict_with_docker(client, model_name_version, df)
        except Exception as e:
            print(e)
            sys.exit(1)

    else:
        raise Exception("请确认指定计算后端是否正确")
    print(f"model exist in {computing_backend}, Start calculation...")
    # y_true 可能为空，为空时只返回 time_cost
    if y_true:
        try:
            y_true = df[y_true]

            acc, precision, recall, f1 = calculate(y_true, y_predict)
            return acc, precision, recall, f1, time_cost, y_predict
        except Exception as e:
            print(e)
            sys.exit(1)
    else:
        # y_true 为空
        return '', '', '', '', time_cost, ''


@hug.cli()
def main(computing_backend: hug.types.text, model_name_version: hug.types.text, df_file_path: hug.types.text,
         y_true: hug.types.text, verbose=False):
    # 针对以FC，RAY为计算资源的模型计算benchmark
    if isinstance(df_file_path, pd.DataFrame):
        df = df_file_path
    elif df_file_path.endswith("json"):
        df = pd.read_json(df_file_path)
    else:
        raise Exception("本地文件请使用json文件，OSS文件请确定格式是否正确")
    df.columns = df.columns.astype(str)
    print("input data shape is ", df.shape)
    try:
        client = check_model_exist(computing_backend, model_name_version)
    except Exception as e:
        return e
    acc, precision, recall, f1, time_cost, y_predict = get_benchmark_result(computing_backend, client, model_name_version, df,
                                                                 y_true)
    result_dict = {"accuracy": acc, "precision": precision, "recall": recall, "f1": f1, "time_cost": time_cost}

    # 统一模型输出格式
    y_predict = pd.DataFrame(y_predict)
    y_predict = y_predict.iloc[:,0].values.tolist()

    return result_dict, y_predict


@hug.cli()
def main_odps(computing_backend, model_name_version, table_name, field_tuple, label, verbose=False):
    # 针对以ODPS为计算资源的模型计算benchmark
    # TODO 字段默认为表中所有字段
    if computing_backend != "odps":
        print("error")
        return
    try:
        client = check_model_exist(computing_backend, model_name_version)
    except Exception as e:
        return e
    (y_true, y_predict), time_cost = predict_with_odps(client, model_name_version, table_name, field_tuple, label)
    acc, precision, recall, f1 = calculate(y_true, y_predict)
    result_dict = {"accuracy": acc, "precision": precision, "recall": recall, "f1": f1, "time_cost": time_cost}

    return result_dict, y_predict


