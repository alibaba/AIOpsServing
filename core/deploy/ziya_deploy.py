import hug
import sys

from core.utils import generate_client
from core.info import list_models as ziya_list_models
from core.deploy.deploy_to_fc_openapi import FCServeDeploy
from core.deploy.deploy_to_odps import ODPSServeDeploy
from core.deploy.deploy_to_docker import DockerServeDeploy


@hug.cli(doc="deploy model to aliyun FC")
def deploy_model_to_fc(model_name_version: hug.types.text, model_path: hug.types.text, verbose=False):
    try:
        fc_client = FCServeDeploy(model_path)
        deploy_detail = fc_client.create_deployment(model_name_version, model_path)

        if verbose:
            print(deploy_detail)

    except Exception as e:

        return f"""{model_name_version} 部署失败, 错误原因:{e}"""
    print(f"Successfully deployed model {model_name_version} to FC!")
    fc_service_name = generate_client.service_name
    out_put_string = f"""
    1、模型调用url： http://ziya.alibaba-inc.com/model/{model_name_version} 
    2、参考文档：https://yuque.antfin-inc.com/renlei.lf/ziya/tazg2g#Bi9fe
    3、私有接口：fc_client.invoke("{fc_service_name}", "{model_name_version}", payload=data.decode())

        """
    return out_put_string


@hug.cli(doc="deploy model to RAY, Model path must be absolute!")
def deploy_model_to_ray(model_name_version: hug.types.text, model_path: hug.types.text, verbose=False):
    ray_model_dict = ziya_list_models.list_ray_models()
    if model_name_version in ray_model_dict["ray_models"]:
        return f"model {model_name_version} already exists in RAY"

    try:
        plugin = generate_client.generate_ray_client()
    except Exception as e:
        print(e)
        sys.exit(1)
    try:
        result = plugin.create_deployment(name=model_name_version, model_uri=model_path)
        print(f"Successfully deployed model {model_name_version} to RAY!")
        if verbose:
            print(result)
    except Exception as e:
        print(e)
        sys.exit(1)

    out_put_string = f"""
1、模型调用url： http://ziya.alibaba-inc.com/model/{model_name_version} 
2、参考文档：https://yuque.antfin-inc.com/renlei.lf/ziya/tazg2g#Bi9fe
3、私有接口：ray_client.predict("{model_name_version}", df)

    """
    return out_put_string


@hug.cli(doc="deploy model to docker")
def deploy_model_to_docker(model_name_version: hug.types.text, model_path: hug.types.text, verbose=False):
    docker_model_list = ziya_list_models.list_docker_models()["docker_models"]
    if model_name_version in docker_model_list:
        return f"model {model_name_version} already exists in docker"

    try:
        docker_client = DockerServeDeploy()
        result = docker_client.create_deployment(model_name_version)
        print(f"Successfully deployed model {model_name_version} to docker!")
        if verbose:
            print(result)
    except Exception as e:
        print(e)
        sys.exit(1)

    out_put_string = f"""
1、模型调用url： http://ziya.alibaba-inc.com/model/{model_name_version}
2、参考文档：https://yuque.antfin-inc.com/renlei.lf/ziya/tazg2g#Bi9fe
3、私有接口：docker_client.predict_by_docker("{model_name_version}", df)

    """
    return out_put_string


# TODO  1.extra_resource_list 还需进一步处理，2.根据模型文件自动生成py_file
@hug.cli(doc="deploy model to aliyun ODPS")
def deploy_model_to_odps(model_dir: hug.types.text, pkg_dir: hug.types.text, odps_py_name: hug.types.text,
                         py_file_path: hug.types.text, class_type: hug.types.text, odps_func_name: hug.types.text,
                         extra_resource_list: hug.types.text, verbose=False):
    odps_deploy = ODPSServeDeploy()
    odps_deploy.main(model_dir, pkg_dir, odps_py_name, py_file_path, class_type, odps_func_name, extra_resource_list)
    out_put_string = f"""
odps 调用sql: e.g. select sum(id ,value) from table a where time=xxx;
已部署模型同sum 函数一样使用
    """
    return out_put_string
