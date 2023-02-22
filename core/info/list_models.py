import os
import sys
import json
from core.utils import generate_client
from core.deploy.aliyun_serve import ALiYunFCServe
from core.deploy.deploy_to_docker import DockerServeDeploy


# 1.列出模型库中存在的模型文件（oss）
def list_oss_models():
    try:
        bucket = generate_client.generate_oss_client()
    except Exception as e:
        return {"oss_models": []}

    model_prefix = "model"
    model_object_list = bucket.list_objects(prefix=model_prefix).object_list
    model_list = ["_".join(os.path.split(i.key)[0:1]) for i in model_object_list]
    model_set = set(model_list)

    # oss model name e.g. model/AnomalyDetection_acoe/v1/python_model.pkl
    result_list = ["_".join(i.split("/")[1:3]) for i in model_set]

    return {"oss_models": result_list}


def list_fc_models():
    # generate_client.check_conf_exists()
    # try:
    #     fc_client = generate_client.generate_fc_client()
    # except Exception as e:
    #     # print(e)
    #     return {"fc_models": []}
    #
    fc_service_name = generate_client.service_name
    # funcs = fc_client.list_functions(fc_service_name, limit=100).data["functions"]
    #
    # funcs = [i['functionName'] for i in funcs]
    # fc_model_info = {"fc_models": funcs}

    fc = ALiYunFCServe()

    funcs = fc.list_func(fc_service_name)
    fc_model_info = {"fc_models": funcs}
    return fc_model_info


def list_ray_models():
    try:
        ray_client = generate_client.generate_ray_client()
    except Exception as e:
        return {"ray_models": []}

    ray_model = ray_client.list_deployments()

    ray_funcs = [i["name"] for i in ray_model]
    ray_model_info = {"ray_models": ray_funcs}
    return ray_model_info


def list_odps_models():
    try:
        odps_client = generate_client.generate_odps_client()
    except Exception as e:
        return {"odps_models": []}

    odps_funcs = [i.name for i in odps_client.list_functions()]
    odps_model_info = {"odps_models": odps_funcs}

    return odps_model_info


def list_docker_models():
    try:
        docker_client = DockerServeDeploy()
        docker_list = docker_client.get_container_list()
    except Exception as e:
        return {"docker_models": []}
    docker_model_info = {"docker_models": docker_list}
    return docker_model_info


# 2. 列出正在运行中的模型（odps、ray、fc）

def list_ziya_models(backend=None):
    if backend is None:
        fc_model_info = list_fc_models()
        ray_model_info = list_ray_models()
        odps_model_info = list_odps_models()
        docker_model_info = list_docker_models()
        oss_model_info = list_oss_models()

        result_info = {}

        result_info.update(fc_model_info)
        result_info.update(ray_model_info)
        result_info.update(odps_model_info)
        result_info.update(docker_model_info)
        result_info.update(oss_model_info)
        if result_info:
            return json.dumps(result_info)
        else:
            # 当前所有 config 配置均出错
            return "当前未配置ziya.conf配置项"
    elif backend:
        backend = backend.lower()

        if backend == "ray":
            ray_model_info = list_ray_models()

            return json.dumps(ray_model_info)

        elif backend == "fc":
            fc_model_info = list_fc_models()

            return json.dumps(fc_model_info)

        elif backend == "odps":
            odps_model_info = list_odps_models()

            return json.dumps(odps_model_info)

        elif backend == "docker":
            docker_model_info = list_docker_models()

            return json.dumps(docker_model_info)

        elif backend == "oss":
            oss_model_info = list_oss_models()

            return json.dumps(oss_model_info)

        else:
            return "指定后端错误，目前可选择计算后端[fc/ray/odps/oss]"
