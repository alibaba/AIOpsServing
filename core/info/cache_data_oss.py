"""
主要将bemchmark、info等接口结果信息缓存至oss中
1、oss文件追加写，
2、不同种类缓存结果分别存入对应的文件，一一对应

是否缓存最新一条记录就可以？
info 信息缓存文件: info接口结果缓存                             content                                       cache/info.cache
all_backend: 指定模型，指定数据，所有计算后端 benchmark 结果缓存  time,model_name,datasets,backend(*),content    cache/all_backend.cache
all_datasets: 指定模型，指定后端，所有数据集 benchmark 结果缓存   time,model_name,datasets(*), backend,content   cache/all_datasets.cache
all_model: 指定数据集，指定后端,所有模型 benchmark 结果缓存     time,model_name(*),datasets,backend,content     cache/all_model.cache
single: 指定模型，指定数据集，指定后端 benchmark 结果缓存        time,model_name,datasets, backend,content      cache/single_cache.cache

缓存信息内容:

"""
from core.utils.generate_client import generate_oss_client
import pandas as pd
import json
import sys


class CacheOss:
    # TODO  oss 支持过期时间

    def __init__(self):
        pass

    def get_bucket(self):
        try:
            bucket = generate_oss_client()
            return bucket
        except Exception as e:
            print(e)
            sys.exit(1)

    def _read_cache(self, cache_name):
        # 从oss中读取缓存数据，同时给予时间戳信息提示
        # 流式读取缓存内容，将缓存结果转为df，根据条件查询
        bucket = self.get_bucket()

        object_stream = bucket.get_object(cache_name)
        # 结果为分行字符串，将结果解析为df
        result = object_stream.read().decode()
        result = result.split("\n")
        result = [json.loads(i) for i in result if i != ""]
        result_df = pd.DataFrame(result)
        return result_df

    def _write_cache(self, cache_name, content):
        # 将结果缓存至oss
        # 判断缓存文件是否存在
        bucket = self.get_bucket()
        if bucket.object_exists(cache_name):
            # 缓存文件存在，获取position位置
            position = bucket.head_object(cache_name).content_length
            # 将结果追加
            bucket.append_object(cache_name, position, content)
        else:
            # 当前缓存文件不存在，创建新的缓存文件，结果追加
            result = bucket.append_object(cache_name, 0, content)

        return


