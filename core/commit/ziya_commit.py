import os
import json
import re

from core.utils import oss_util
from core.info import list_models


def commit_model_to_oss(model_dir, describe=None):
    my_oss = oss_util.OssUtil()
    if not model_dir or not os.path.exists(model_dir):
        return "请确认模型输入路径是否正确"

    if model_dir.endswith("/"):
        model_dir = model_dir.rstrip("/")

    model_name_version = os.path.split(model_dir)[-1]

    regex = re.compile(r"_v\d+")
    result = regex.findall(model_name_version)
    if not result:
        # 列表为空，说明模型名称不符合规范，直接返回
        return f"model name {model_name_version} does not conform to the specification, You can refer to model_name_v1"

    # 名称为模型名称加版本号 e.g.  Model_a_v1

    model_name, model_version = my_oss.split_model_name_version(model_name_version)

    # 1、模型版本管理 V1
    # # 到oss模型库中查询总的现有模型列表
    # oss_total_model_list = list_models.list_oss_models()["oss_models"]
    #
    # # 获取同类型模型列表
    # same_model_list = [i for i in oss_total_model_list if model_name in i]
    #
    # if len(same_model_list) == 0:
    #     # 不存在同类型模型，模型初始版本为 V1
    #     model_name = model_name + "_v1"
    # else:
    #     # 获取最大版本号模型，+1
    #     version_list = [int(i.split("v")[-1]) for i in same_model_list]
    #     version_value = str(max(version_list)+1)
    #     model_name = model_name + f"_v{version_value}"

    # 2、 不使用模型版本管理，检查模型名称是否符合规范，必须带版本号，不是最新版本提示，不覆盖

    # 到oss模型库中查询总的现有模型列表
    oss_total_model_list = list_models.list_oss_models()["oss_models"]
    same_model_list = [i for i in oss_total_model_list if model_name in i]

    # 对比上传模型版本与oss模型库中最新版本，如果大于最新版本上传，否则返回，提示
    version_list = [float(i.split("_v")[-1]) for i in same_model_list]
    if not version_list:
        # 说明该模型第一次提交至OSS
        version_list = [0]
    oss_max_version = max(version_list)

    if float(model_version.split("v")[-1]) <= oss_max_version:
        return f"模型库中模型{model_name}最新版本为{oss_max_version},当前模型版本为{model_version}, 请确认模型版本号重新上传"

    model_file_list = os.listdir(model_dir)
    # 模型在oss中的模型名称 为model/model_name/version/file    e.g. model/AnomalyDetection_acoe/v1/python_model.pkl
    oss_name = "model/{}/{}/{}"

    if describe:
        str_describe = json.dumps(describe)
        headers = {"x-oss-meta-des": str_describe}
    else:
        headers = None

    for model_file_name in model_file_list:
        model_file_path = os.path.join(model_dir, model_file_name)
        tmp_oss_name = oss_name.format(model_name, model_version, model_file_name)
        my_oss.bucket.put_object_from_file(tmp_oss_name, model_file_path, headers=headers)
    return f"Model [{model_name_version}] uploaded successfully"
