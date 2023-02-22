import json
import time

from core.benchmark import ziya_benchmark
from core.info import list_models as ziya_list_models
from core.utils import oss_util
from core.info import cache_data_oss, run_status


class Benchmark:
    def __init__(self):
        cache = cache_data_oss.CacheOss()
        self.cache = cache

    def get_update_time(self):
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return now_time

    def get_oss_models(self):
        oss_models = ziya_list_models.list_oss_models()
        return oss_models["oss_models"]

    def get_running_model_by_backend(self, backend):
        # 根据后端名称获取正在运行中的模型信息
        backend = backend.lower()
        if backend == "ray":
            model_list = ziya_list_models.list_ray_models()["ray_models"]
        elif backend == "fc":
            model_list = ziya_list_models.list_fc_models()["fc_models"]
        elif backend == "odps":
            model_list = ziya_list_models.list_odps_models()["odps_models"]

        elif backend == "docker":
            model_list = ziya_list_models.list_docker_models()["docker_models"]

        else:
            model_list = []
        return model_list

    def new_benchmark(self, model_name, datasets, backend, update):
        """
        1. 出于性能考虑，建立必要的缓存机制，当指定--update条件时重新计算并更新缓存
        2. --model 或 --dataset 或 --backend 的缺省值为 *，即所有
        3. --model * 对应ziya status中所有models
        4. --dataset * 对应ziya status中所有datasets，不允许本地dataset
        5. --backend * 对应ziya status中所有backends，限于现有部署情况，不自动触发部署, 暂不处理odps 后端
        :param model_name:
        :param datasets: 标签化后的数据集
        :param backend:
        :return:
        """

        # TODO odps 暂不做  benchmark
        if backend == "odps":
            return f"计算后端[odps]暂不支持，目前仅支持[RAY/FC]"

        my_oss = oss_util.OssUtil()

        # if y_true is None:
        #     return "请输入真实值对应的列名（label字段名称）"

        model_flag = "*" if model_name is None or model_name.endswith("*") else model_name
        arg_list = [model_flag, datasets, backend]
        if len(set(arg_list)) < 3:
            return "必须指定[model_name, datasets, backend]中的至少两个参数"

        if model_name.split("_")[-1] == "*":

            # benchmark 缓存文件
            model_cache_name = "cache/all_model/benchmark.cache"

            # 模型预测结果缓存文件
            model_predict_name = "cache/all_model/predict.cache"

            # 根据模型名称，获取oss所有版本的模型，与指定后端已部署的模型取交集，做benchmark

            # 可能出现的情况：1、缓存文件不存在，2、文件存在，指定内容不存在
            if not update:
                # 不是实时更新，从缓存中获取数据
                # 1、判断缓存文件是否存在
                if my_oss.bucket.object_exists(model_cache_name):
                    # 缓存文件已存在，根据条件读取内容
                    model_df = self.cache._read_cache(model_cache_name)
                    query_str = f"model_name=='*'&datasets=='{datasets}'&backend=='{backend}'"
                    result_df = model_df.query(query_str)
                    if not result_df.empty:
                        # 查到缓存结果,返回最后一条数据
                        result = result_df.tail(1).to_json(orient="records")
                        return json.dumps(json.loads(result), indent=4)
                    else:
                        print("未查到缓存信息，正在针对所有模型做实时benchmark..")
                # 缓存文件不存在
                else:
                    print("未查到缓存文件，正在针对所有模型做实时benchmark..")

            tmp_model_name = model_name.split("*")[0]
            # 从oss 读取数据集，数据集不存在，则抛出异常
            try:
                df = my_oss.get_df_from_oss(datasets)
            except Exception as e:
                return e

            # 读取数据集的同时获取描述文件中的label 列
            y_true = my_oss._get_datasets_label(datasets)
            # if y_true is None:
            #     # 说明该数据集在上传时未添加描述文件，
            #     return f"该数据集[{datasets}]未添加描述文件，请重新上传或者更新描述信息"

            oss_list = self.get_oss_models()
            oss_model_list = [i for i in oss_list if tmp_model_name in i]
            backend_with_model = self.get_running_model_by_backend(backend)
            use_model = [model for model in oss_model_list if model in backend_with_model]
            if not use_model:
                # oss 所有版本模型均未在指定后端部署
                return f"当前所有版本模型均未在指定后端部署"

            result_dict = {}
            predict_dict = {}
            for model in use_model:
                result, y_predict = ziya_benchmark.main(backend, model, df, y_true)
                result_dict[model] = result
                predict_dict[model] = y_predict
            update_time = self.get_update_time()
            final_dict = {"model_name": "*", "datasets": datasets, "backend": backend, "result": result_dict,
                          "update_time": update_time}

            final_predict_dict = {"model_name": "*", "datasets": datasets, "backend": backend, "predict": predict_dict,
                                  "update_time": update_time}
            # benchmark结果写入缓存中
            content = json.dumps(final_dict)
            self.cache._write_cache(model_cache_name, content + "\n")

            # 模型预测结果写入缓存中
            predict_content = json.dumps(final_predict_dict)
            self.cache._write_cache(model_predict_name, predict_content + "\n")

            # 返回benchmark 结果
            return json.dumps(final_dict, indent=4)

        elif datasets == "*":
            # 使用指定模型版本对应的所有数据集,对应关系在模型上传时指定

            # # 需判断指定后端是否存在指定模型名称
            # run_s = run_status.RunStatus()
            # backend_list = run_s.get_backend_status()['backends']
            #
            # model_list = [i[backend] for i in backend_list if backend in i.keys()]
            # if model_name not in model_list:
            #     return f"当前指定模型{model_name}未部署在后端{backend}上，请确认模型名称或重新部署后重试！"

            # benchmark 结果缓存文件
            datasets_cache_name = "cache/all_datasets/benchmark.cache"

            # 模型预测结果缓存文件
            datasets_predict_cache_name = "cache/all_datasets/predict.cache"

            if not update:
                # 不是实时更新，从缓存中获取数据
                # 检查缓存文件是否存在
                if my_oss.bucket.object_exists(datasets_cache_name):
                    # 缓存文件存在，查询缓存内容
                    datasets_df = self.cache._read_cache(datasets_cache_name)

                    query_str = f"model_name=='{model_name}'&datasets=='*'&backend=='{backend}'"
                    result_df = datasets_df.query(query_str)
                    if not result_df.empty:
                        # 不为空，查询到缓存，直接返回最后一条数据
                        result = result_df.tail(1).to_json(orient="records")
                        return json.dumps(json.loads(result), indent=4)
                    else:
                        print("未查到缓存信息，正在针对所有数据做实时benchmark..")

                else:
                    print("未查到缓存文件，正在针对所有数据做实时benchmark..")
            try:
                model_describe_json = my_oss.get_model_describe(model_name)
            except Exception as e:
                return e

            datasets = model_describe_json["datasets"]

            if not datasets:
                # 在oss中没有该模型对应的数据集
                return f"该模型在上传时未指定对应数据集，无法做benchmark，请更新模型描述信息或者重新上传模型"

            result_dict = {}
            predict_dict = {}
            # 同一模型对应的数据集label应该是一致的, 取第一个数据集的label
            y_true = my_oss._get_datasets_label(datasets[0])
            # if y_true is None:
            #     # 说明该数据集在上传时未添加描述文件，
            #     return f"该数据集[{datasets}]未添加描述文件，请重新上传或者更新描述信息"

            for data_name in datasets:
                # 某一个数据集可能不存在，输出错误信息，跳过
                try:
                    data = my_oss.get_df_from_oss(data_name)
                except Exception as e:
                    print(e)
                    continue
                result, y_predict = ziya_benchmark.main(backend, model_name, data, y_true)
                result_dict[data_name] = result
                predict_dict[data_name] = y_predict
            update_time = self.get_update_time()
            # benchmark结果写入缓存中
            final_dict = {"model_name": model_name, "datasets": "*", "backend": backend, "result": result_dict,
                          "update_time": update_time}
            content = json.dumps(final_dict)
            self.cache._write_cache(datasets_cache_name, content + "\n")

            # 模型预测结果写入缓存中
            final_predict_dict = {"model_name": model_name, "datasets": "*", "backend": backend,
                                  "predict": predict_dict,
                                  "update_time": update_time}

            predict_content = json.dumps(final_predict_dict)
            self.cache._write_cache(datasets_predict_cache_name, predict_content + "\n")

            # 返回benchmark 结果
            return json.dumps(final_dict, indent=4)

        elif backend == "*":
            # 使用指定版本模型、指定数据集在所有已部署后端做benchmark
            # 此处从所有后端实时查询，不使用status中的模型对应后端信息

            # benchmark 结果缓存文件
            backend_cache_name = "cache/all_backend/benchmark.cache"

            # 模型预测结果缓存文件
            backend_cache_predict_name = "cache/all_backend/predict.cache"

            if not update:
                # 不是实时更新，从缓存中获取数据
                # 检查缓存文件是否存在：
                if my_oss.bucket.object_exists(backend_cache_name):
                    # 文件存在，读取缓存内容
                    backend_df = self.cache._read_cache(backend_cache_name)
                    query_str = f"model_name=='{model_name}'&datasets=='{datasets}'&backend=='*'"
                    result_df = backend_df.query(query_str)
                    if not result_df.empty:
                        # 不为空，返回缓存结果
                        result = result_df.tail(1).to_json(orient="records")
                        return json.dumps(json.loads(result), indent=4)
                    else:
                        print("未查到缓存信息，正在针对所有后端做实时benchmark..")

                else:
                    print("未查到缓存文件，正在针对所有后端做实时benchmark..")

            # 从oss中获取数据集
            try:
                data = my_oss.get_df_from_oss(datasets)
            except Exception as e:
                return e
            # 从数据集描述文件中获取label列信息
            y_true = my_oss._get_datasets_label(datasets)

            # if y_true is None:
            #     # 说明该数据集在上传时未添加描述文件，
            #     return f"该数据集[{datasets}]未添加描述文件，请重新上传或者更新描述信息"

            fc_model_list = ziya_list_models.list_fc_models()["fc_models"]
            ray_model_list = ziya_list_models.list_ray_models()["ray_models"]
            docker_model_list = ziya_list_models.list_docker_models()["docker_models"]

            result_dict = {}
            predict_dict = {}
            # 不同计算后端只保留一个模型预测结果
            y_predict = ''
            if model_name in fc_model_list:
                result, y_predict = ziya_benchmark.main("fc", model_name, data, y_true)
                result_dict["fc"] = result
            if model_name in ray_model_list:
                result, y_predict = ziya_benchmark.main("ray", model_name, data, y_true)
                result_dict["ray"] = result
            if model_name in docker_model_list:
                result, y_predict = ziya_benchmark.main("docker", model_name, data, y_true)
                result_dict["docker"] = result

            update_time = self.get_update_time()

            # benchmark 结果写入缓存中
            final_dict = {"model_name": model_name, "datasets": datasets, "backend": "*", "result": result_dict,
                          "update_time": update_time}

            content = json.dumps(final_dict)
            self.cache._write_cache(backend_cache_name, content + "\n")

            # 模型预测结果写入缓存中
            final_predict_dict = {"model_name": model_name, "datasets": datasets, "backend": "*", "predict": y_predict,
                                  "update_time": update_time}

            predict_content = json.dumps(final_predict_dict)
            self.cache._write_cache(backend_cache_predict_name, predict_content + "\n")

            # 返回benchmark 结果
            return json.dumps(final_dict, indent=4)

        # 指定计算后端、指定数据集、指定模型版本
        elif "*" not in model_name and datasets != "*" and backend != "*":

            # benchmark 结果缓存文件
            cache_file_name = "cache/single_cache/benchmark.cache"
            # 模型预测结果缓存文件
            cache_file_predict_name = "cache/single_cache/predict.cache"
            if not update:
                # 不是实时更新，从缓存中读取数据
                # 检查缓存文件是否存在
                if my_oss.bucket.object_exists(cache_file_name):
                    # 文件存在，读取缓存内容
                    single_df = self.cache._read_cache(cache_file_name)
                    query_str = f"model_name=='{model_name}'&datasets=='{datasets}'&backend=='{backend}'"
                    result_df = single_df.query(query_str)
                    if not result_df.empty:
                        # 不为空，直接返回缓存结果
                        result = result_df.tail(1).to_json(orient="records")
                        return json.dumps(json.loads(result), indent=4)
                    else:
                        print("未查到缓存信息，根据指定模型、后端、数据集做实时benchmark..")
                else:
                    print("未查到缓存文件，根据指定模型、后端、数据集做实时benchmark..")

            # 从oss 读取数据集
            try:
                df = my_oss.get_df_from_oss(datasets)
            except Exception as e:
                return e

            # 读取数据集的同时获取描述文件中的label 列
            y_true = my_oss._get_datasets_label(datasets)
            # if y_true is None:
            #     # 说明该数据集在上传时未添加描述文件，
            #     return f"该数据集[{datasets}]未添加描述文件，请重新上传或者更新描述信息"
            result, y_predict = ziya_benchmark.main(backend, model_name, df, y_true)
            update_time = self.get_update_time()

            # benchmark 结果写入缓存中

            final_dict = {"model_name": model_name, "datasets": datasets, "backend": backend, "result": result,
                          "update_time": update_time}
            content = json.dumps(final_dict)
            self.cache._write_cache(cache_file_name, content + "\n")

            # 模型预测结果写入缓存中
            final_predict_dict = {"model_name": model_name, "datasets": datasets, "backend": backend,
                                  "predict": y_predict,
                                  "update_time": update_time}

            predict_content = json.dumps(final_predict_dict)
            self.cache._write_cache(cache_file_predict_name, predict_content + "\n")

            # 返回benchmark 结果
            return json.dumps(final_dict, indent=4)

        else:
            return "输入错误， 目前仅支持【model_name, datasets, backend】中存在一个缺失值，且其余参数输入正确"
