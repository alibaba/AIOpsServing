# 该模块主要是为了方便用户将自己的模型封装成符合子牙平台规范要求的模型


def pack_model(model_name_version):
    template_str = """
import mlflow.pyfunc
import sys
import re
import os


# 使用说明：
# 1、编写__init__方法（可选）,编写predict主要预测函数逻辑, 包括引用自己模型、编写函数返回值
# 2、指定模型打包主函数中 code_path 路径，该code_path 为使用到的自定义模块路径
# 3、根据需求选择是否配置odps以及编写 udf 文件


class MyModel(mlflow.pyfunc.PythonModel):

    def __init__(self):
        # 可以根据需求定义自己的初始化函数
        pass

    def predict(self, context, model_input):
        # 在predict方法中调用上述模型源码中的预测逻辑
        # context 未占位参数（不可缺省）, model_input 为 模型输入数据

        # 自定义返回值
        return 


if __name__ == '__main__':
    # 根据类创建实例
    my_model = MyModel()
    model_path = "{}"
    if not model_path:
        print("Model path not specified, please confirm.")
        sys.exit(1)

    regex = re.compile(r"_v\d+")
    result = regex.findall(model_path)
    if not result:
        print("The model name does not meet the requirements, please refer to my_model_v1")
        sys.exit(1)

    if os.path.exists(model_path):
        print("The model file already exists. Please delete the model file if you want to reseal it")
        sys.exit(1)
    try:
        # 模型打包主函数 
        code_path = ""
        code_list = os.listdir(code_path) if code_path else None
        model = mlflow.models.model.Model()
        # 定义模型的描述信息以及数据列名
        {}
        model.add_flavor("ziya_model", **ziya_dict)
        mlflow.pyfunc.save_model(path=model_path, code_path=code_list, python_model=my_model, mlflow_model=model)
    except Exception as e:
        print(e)

    # 在 mlflow 封装的模型的基础上添加自己的odps配置文件
    # 如果想让模型支持ODPS 部署，请将 FLAG 置为1并设置对应的参数
    FLAG = 0
    if FLAG:
        odps_cfg_tmplate = \"\"\"
"env_path": "", 
"odps_py_name": "", 
"py_file_path": "", 
"class_type": "", 
"model_name_version": "", 
"extra_resource_list": ""
\"\"\"

        with open(os.path.join(model_path, 'odps.cfg'), 'w') as fp:
            fp.write(odps_cfg_tmplate)

        odps_index_template = \"\"\"
from odps.udf import annotate


# odps 要求的装饰器，指定函数 evalueta 的输入和输出
@annotate("*->string")
class MyUdf(object):
    def __init__(self):
        import sys

    def evaluate(self, *args):
        # 编辑自己的函数逻辑，设定返回值

        return 'success'
\"\"\"
        with open(os.path.join(model_path, 'odps_index.py'), 'w') as fp:
            fp.write(odps_index_template)
        

"""

    with open('commit_init.py', 'w') as fp:
        model_str = "ziya_dict = {'description': 'model description','columns': ''}"
        fp.write(template_str.format(model_name_version, model_str))


if __name__ == '__main__':
    pack_model("demo")

