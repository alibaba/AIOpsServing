import time
import json

from core.utils import generate_client
from core.info import list_models as ziya_list_models
from core.utils import oss_util
# from core.info import cache_data_sqlite as cache_data
from core.info import cache_data_oss


class RunStatus:
    def __init__(self):
        cache = cache_data_oss.CacheOss()
        self.cache = cache
        try:
            self.my_oss = oss_util.OssUtil()
        except Exception as e:
            raise Exception(e)

    def get_backend_status(self):
        # 获取 status 中 backend 对应的描述信息
        try:
            fc_client = generate_client.generate_aliyun_client()
            fc_usable = "True"
            # 返回值是字典{"fc_models": [model1, model2]}
            fc_model_list = ziya_list_models.list_fc_models()["fc_models"]
            fc_config = ""
            # 增加显示FC 可用资源字段
            free = str(50-len(fc_model_list))
        except:
            fc_usable = "False"
            fc_model_list = []
            fc_config = ""
            free = ''

        fc_backend_dict = {"fc": {"usable": fc_usable, "models": fc_model_list, "config": fc_config, "free": free}}

        try:
            ray_client = generate_client.generate_ray_client()
            ray_usable = "True"
            ray_model_list = ziya_list_models.list_ray_models()["ray_models"]
            ray_config = ""
        except:
            ray_usable = "False"
            ray_model_list = []
            ray_config = ""
        ray_backend_dict = {"ray": {"usable": ray_usable, "models": ray_model_list, "config": ray_config}}

        try:
            odps_client = generate_client.generate_odps_client()
            odps_usable = "True"
            odps_model_list = ziya_list_models.list_odps_models()["odps_models"]
            odps_config = ""
        except:
            odps_usable = "False"
            odps_model_list = []
            odps_config = ""
        odps_backend_dict = {"odps": {"usable": odps_usable, "models": odps_model_list, "config": odps_config}}

        backend_list = [fc_backend_dict, ray_backend_dict, odps_backend_dict]

        backend_dict = {"backends": backend_list}

        return backend_dict

    def get_model_info(self):
        # 获取status 中 models 对应的描述信息
        # try:
        model_list = ziya_list_models.list_oss_models()["oss_models"]
        models_info = []

        for model_name in model_list:
            try:
                model_describe_json = self.my_oss.get_model_describe(model_name)
                # model_describe_json = json.loads(model_des)
            except:
                model_describe_json = ""
            models_info.append({model_name: model_describe_json})

        model_dict = {"models": models_info}
        return model_dict

    def get_datasets_info(self):
        # 获取 status 中 datasets 对应的描述信息
        try:
            oss_client = generate_client.generate_oss_client()
            data_list = []
            datasets = self.my_oss.list_data_sets()["datasets"]
            for dataset in datasets:
                data_meta_info = self.my_oss.get_datasets_describe(dataset)
                if data_meta_info is not None:
                    data_list.append({dataset: data_meta_info})

        except:
            data_list = []
        data_dict = {"datasets": data_list}
        return data_dict

    def get_status(self):
        # 实时获取状态信息
        status_result = {}
        backend_list = self.get_backend_status()
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        status_result["backends"] = [backend_list]
        status_result["update_time"] = now_time
        status_result["errors"] = ""

        return status_result

    def get_model_and_datasets_info(self, update=False):
        cache_file_name = 'cache/info.cache'

        # 可能出现的情况：1、缓存文件不存在，2、文件存在，指定内容不存在
        if not update:
            # 从缓存中获取缓存信息，结果为df，取最后一条记录
            if self.my_oss.bucket.object_exists("cache/info.cache"):
                # 如果存在，则说明以创建缓存文件，直接读取内容
                result_df = self.cache._read_cache(cache_file_name)
                # 从缓存中获取到缓存信息
                if not result_df.empty:
                    # df不为空说明缓存文件中存在缓存内容
                    result = result_df.tail(1).to_json(orient="records")
                    return json.dumps(json.loads(result), indent=4)
                else:

                    print("未查到缓存信息，正在实时获取模型相关信息..")
            else:
                # 缓存中没有相关信息，实时运行
                print("未查到缓存信息，正在实时获取模型相关信息..")

        # 实时获取model info 相关信息

        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        result_dict = {}
        backends_info = self.get_backend_status()
        models = self.get_model_info()
        datasets = self.get_datasets_info()
        result_dict.update(backends_info)
        result_dict.update(models)
        result_dict.update(datasets)
        result_dict.update({"update_time": now_time})

        # 结果存入缓存中
        content = json.dumps(result_dict)
        self.cache._write_cache(cache_file_name, content + "\n")

        # 主要为了展示的数据更加符合规范
        return json.dumps(result_dict, indent=4)
