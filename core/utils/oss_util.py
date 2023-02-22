import oss2
import os
import shutil
import logging
from oss2.models import BucketVersioningConfig
from io import BytesIO
import pickle
import json
import sys
import pandas as pd
from core.utils import generate_client


class OssUtil:

    def __init__(self):
        try:
            bucket = generate_client.generate_oss_client()
        except Exception as e:
            raise Exception(e)

        self.bucket = bucket

    def version_control(self, flag):
        """
        设置版本控制
        :return:
        """
        config = BucketVersioningConfig()

        if flag:
            config.status = oss2.BUCKET_VERSIONING_ENABLE
        else:
            config.status = oss2.BUCKET_VERSIONING_SUSPEND

        # 设置bucket版本控制状态。
        result = self.bucket.put_bucket_versioning(config)
        # 查看http返回码。
        print('http response code:', result.status)

    def put_checking_object(self, oss_file_name, content, headers=None):
        # 统一管理上传文件
        self.bucket.put_object(oss_file_name, content, headers=headers)

    def log(self):
        """
        开启log日志
        :return:
        """
        log_file_path = 'log.log'
        oss2.set_file_logger(log_file_path, 'oss2', logging.INFO)

    # 根据标签化的数据集名称获取oss中真实数据集
    def get_real_data_name_by_tag(self, tag_name):
        # 定义真实oss 数据集名称，规则 datasets/[oss_name]/data.data
        return f"datasets/{tag_name}/data.data"

    def get_real_model_name_by_tag(self, tag_name):
        model_name, model_version = self.split_model_name_version(tag_name)
        return f"model/{model_name}/{model_version}/"

    def put_datasets_to_oss(self, oss_name, df, describe=None):
        # 直接将df 转为json 存入oss，其中 oss_name 为标签化的数据集
        oss_name = self.get_real_data_name_by_tag(oss_name)
        json_df = df.to_json(orient='records')

        if describe:
            str_describe = json.dumps(describe)
            headers = {"x-oss-meta-des": str_describe}
        else:
            headers = None

        self.put_checking_object(oss_name, json_df, headers=headers)

    def get_df_from_oss(self, oss_name):
        # 根据标签化的数据集名称，读取真实数据集
        oss_name = self.get_real_data_name_by_tag(oss_name)
        flag = self.check_file_exists(oss_name)
        if not flag:
            # 数据集不存在oss中
            raise Exception(f"数据集{oss_name}在oss中不存在，请确认数据集信息")

        try:
            str_df = self.bucket.get_object(oss_name).read().decode()
            json_df = json.loads(str_df)
            return pd.json_normalize(json_df)
        except Exception as e:
            raise Exception(e)

    def check_file_exists(self, oss_key):
        # oss_key 可能是文件，可能是oss目录
        flag = False

        if self.bucket.object_exists(oss_key):
            # 判断指定路径文件在oss中是否存在
            flag = True
        if self.bucket.list_objects(oss_key).object_list:
            # 判断给定路径下是否存在文件
            flag = True

        return flag

    def download_model_from_oss(self, model_save_path, model_name_version):

        oss_model_path = self.get_real_model_name_by_tag(model_name_version)

        flag = self.check_file_exists(oss_model_path)

        if not flag:
            # 说明在oss中，即使存在模型文件夹，模型文件夹为空，模型文件也不存在
            raise Exception(f"model {model_name_version} not exists in OSS, please confirm!")

        # 模型文件夹所在路径
        model_dir_path = os.path.join(model_save_path, model_name_version)

        # 如果模型保存文件夹不存在，则创建模型文件夹
        if not os.path.exists(model_dir_path):
            os.makedirs(model_dir_path)

        model_file_path = os.path.join(model_dir_path, "MLmodel")

        # 如果模型文件已存在，则不需要重新下载
        if os.path.exists(model_file_path):
            # print(f"模型{model_name_version}已存在路径{model_save_path}")
            pass

        else:
            model_name, model_version = self.split_model_name_version(model_name_version)
            model_prefix = f"model/{model_name}/{model_version}"

            for b in oss2.ObjectIterator(self.bucket, prefix=model_prefix):
                local_file_name = os.path.split(b.key)[-1]

                local_file_path = os.path.join(model_dir_path, local_file_name)
                self.bucket.get_object_to_file(b.key, local_file_path)

    # 直接上传文件
    def upload_file_to_oss(self, data_path, describe=None):
        if data_path.endswith("/"):
            data_path = data_path.rstrip("/")

        oss_tmp_name = os.path.split(data_path)[-1]
        oss_name = "datasets/{}".format(oss_tmp_name)

        if describe:
            str_describe = json.dumps(describe)
            headers = {"x-oss-meta-des": str_describe}
        else:
            headers = None

        self.bucket.put_object_from_file(oss_name, data_path, headers=headers)

    # 直接下载文件
    def download_file_from_oss(self, file_save_path, file_name):
        real_file_name = self.get_real_data_name_by_tag(file_name)
        data_save_dir = os.path.join(file_save_path, file_name)
        if not os.path.exists(data_save_dir):
            os.makedirs(data_save_dir)

        file_list = os.listdir(data_save_dir)
        # 不为空，则说明文件已存在
        if file_list:
            pass
        else:
            # 下载文件
            # 获取文件描述，存为conf.json
            describe = self.get_datasets_describe(file_name)

            if describe is None:
                # 文件上传时未指定describe
                pass
            else:
                cfg_path = os.path.join(data_save_dir, "MLData")
                with open(cfg_path, "w") as fp:
                    fp.write(json.dumps(describe))

            local_file_name = os.path.join(data_save_dir, "data.data")
            self.bucket.get_object_to_file(real_file_name, local_file_name)

    def download_single_model(self, oss_file_name):
        """
        从oss中下载单个文件
        :param oss_file_name:  模型名称
        :return: 模型
        """
        file_bytes = BytesIO(self.bucket.get_object(oss_file_name).read())
        model = pickle.load(file_bytes)
        return model

    def list_data_sets(self):
        data_prefix = "datasets"
        file_list = self.bucket.list_objects(prefix=data_prefix).object_list
        # 数据集标签化处理，目前只显示数据集的名称, i.key = datasets/user_data/data.data
        data_list = [i.key.split("/")[-2] for i in file_list if len(i.key.split("/")) > 2]
        return {"datasets": data_list}

    # 读取模型文件描述
    def split_model_name_version(self, model_name_version):
        model_name = "_".join(model_name_version.split("_")[:-1])
        model_version = model_name_version.split("_")[-1]
        return model_name, model_version

    def get_model_describe(self, model_name_version):

        model_prefix = self.get_real_model_name_by_tag(model_name_version)

        flag = self.check_file_exists(model_prefix)
        if not flag:
            raise Exception(f"model {model_name_version} not exists in OSS, please confirm!")

        # 只获取oss模型文件夹下一个文件的元信息（文件夹下所有元信息都一致）
        for b in oss2.ObjectIterator(self.bucket, prefix=model_prefix):

            model_meta_info = self.bucket.head_object(b.key)
            try:
                describe = model_meta_info.headers["x-oss-meta-des"]
                return json.loads(describe)
            except Exception as e:
                #  该模型在上传时未上传模型描述文件
                raise Exception(e)

    # 读取数据集描述
    def get_datasets_describe(self, datasets_name):
        # # 根据标签化的数据集名称，读取真实数据集
        key = self.get_real_data_name_by_tag(datasets_name)

        model_meta_info = self.bucket.head_object(key)
        try:
            describe = model_meta_info.headers["x-oss-meta-des"]
            return json.loads(describe)
        except:
            # print("The datasets has no description information")
            return None

    def _get_datasets_label(self, datasets_name):
        describe = self.get_datasets_describe(datasets_name)
        if describe:
            label = describe[0]["label"]
            return label
        else:
            return None

    # 更新模型文件描述
    def update_model_describe(self, model_name_version, describe):
        # 判断模型是否存在
        oss_model_path = self.get_real_model_name_by_tag(model_name_version)

        flag = self.check_file_exists(oss_model_path)
        if not flag:
            raise Exception(f"model {model_name_version} not exists in OSS, please confirm!")

        str_des = json.dumps(describe)
        headers = {"x-oss-meta-des": str_des}

        # 更新oss模型文件夹下所有模型相关文件元信息

        for b in oss2.ObjectIterator(self.bucket, prefix=oss_model_path):
            self.bucket.update_object_meta(b.key, headers=headers)

    # 更新数据集描述
    def update_datasets_describe(self, datasets_name, describe):
        # 判断数据集是否存在
        data_list = self.list_data_sets()["datasets"]
        if datasets_name not in data_list:
            print(f"{datasets_name} not in oss, please confirm")
            return

        str_des = json.dumps(describe)
        headers = {"x-oss-meta-des": str_des}
        datasets_prefix = f"datasets/{datasets_name}"

        for b in oss2.ObjectIterator(self.bucket, prefix=datasets_prefix):
            self.bucket.update_object_meta(b.key, headers=headers)
