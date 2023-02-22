"""
该模块主要使用阿里云ak对FC服务中的函数进行创建、查询、删除等单一操作
"""
import pandas as pd

from alibabacloud_fc_open20210406 import models as fc__open_20210406_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient

from core.utils import generate_client


class ALiYunFCServe:
    def __init__(self):
        # TODO 引用使用单例模式
        self.client = generate_client.generate_aliyun_client()

    def create_func(self, bucket_name, object_name, service_name, func_name):
        runtime = util_models.RuntimeOptions()

        code = fc__open_20210406_models.Code(
            oss_bucket_name=bucket_name,
            oss_object_name=object_name
        )

        # 通过自定义环境，启动服务脚本，python3 为python 解释器的名称
        custom_config = fc__open_20210406_models.CustomRuntimeConfig(
            command=['python3', 'app.py']
        )
        # TODO 具体参数还需修改或者添加
        create_request = fc__open_20210406_models.CreateFunctionRequest(
            code=code,
            custom_runtime_config=custom_config,
            function_name=func_name,
            handler="index.handler",
            runtime="custom",
            timeout=86400,
            memory_size=32768,
            instance_type='c1',
            environment_variables={
                'PYTHONUSERBASE': '/home/share/python3.7.10/bin',
                'PATH': '/home/share/python3.7.10/bin',
                'LD_LIBRARY_PATH': '/home/share/python3.7.10/lib',
                'LIBRARY_PATH': '/home/share/python3.7.10/lib',
            },

        )
        try:
            create_header = fc__open_20210406_models.CreateFunctionHeaders()
            result = self.client.create_function_with_options(service_name, create_request, create_header, runtime)
            return result
        except Exception as e:
            raise Exception(e)

    def list_func(self, service_name):
        list_functions_headers = fc__open_20210406_models.ListFunctionsHeaders()
        # TODO limit需自动化去除, 但目前一个服务最大限制 50个函数
        list_functions_request = fc__open_20210406_models.ListFunctionsRequest(limit=100)
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            result = self.client.list_functions_with_options(service_name, list_functions_request,
                                                             list_functions_headers, runtime)
            funcs = result.body.functions

            funcs = [i.function_name for i in funcs]
            return funcs

        except Exception as error:
            # 如有需要，请打印 error
            UtilClient.assert_as_string(error)

    def delete_func(self, service_name, func_name):
        pass

    def get_func(self, service_name, func_name):
        get_function_headers = fc__open_20210406_models.GetFunctionHeaders()
        get_function_request = fc__open_20210406_models.GetFunctionRequest()
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            result = self.client.get_function_with_options(service_name, func_name, get_function_request,
                                                           get_function_headers, runtime)
            return result
        except Exception as error:
            # 如有需要，请打印 error
            UtilClient.assert_as_string(error)

        pass

    def update_func(self):
        pass

    def invoke_func(self, service_name, func_name, df: pd.DataFrame):
        """

        :param service_name: 服务名称
        :param func_name: 函数名称
        :param df: 输入数据 pandas.DataFrame
        :return: 函数运行返回值 str 类型
        """
        invoke_function_headers = fc__open_20210406_models.InvokeFunctionHeaders()
        invoke_function_request = fc__open_20210406_models.InvokeFunctionRequest(
            body=df.to_json(orient="records").encode())
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            result = self.client.invoke_function_with_options(service_name, func_name, invoke_function_request,
                                                              invoke_function_headers, runtime)
            return result.body.decode()
        except Exception as error:
            # 如有需要，请打印 error
            UtilClient.assert_as_string(error)
