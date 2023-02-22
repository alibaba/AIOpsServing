import os
from pip._internal import main as download_main
import sys
import time
from core.utils import generate_client
from core.info import list_models as ziya_list_models


class ODPSServeDeploy:
    def __init__(self):
        try:
            o = generate_client.generate_odps_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        self.o = o

    # 1.使用 pip download 下载依赖文件到指定文件夹
    def download_requirement(slef, dst_dir, model_dir):
        """
        :param dst_dir: 依赖下载目录
        :param model_dir: 模型文件所在目录，mlflow风格模型
        :return: None
        """

        req = os.path.join(model_dir, "requirements.txt")
        if not os.path.exists(req):
            raise Exception("模型依赖文件不存在，请确认requirement.txt 文件是否存在")
        download_main(['download', '-d', f'{dst_dir}', '-r', f'{req}'])
        print("-------->:依赖文件下载成功")

    # 2.遍历文件夹更改下载文件后缀为.zip， TODO 解压依赖重新编译待自动化
    def change_suffix(self, dst_dir):
        """
        :param dst_dir: 依赖所在的目录
        :return:
        """
        file_list = os.listdir(dst_dir)
        resource_list = [i.name for i in self.o.list_resources()]
        for file_name in file_list:
            # TODO文件名存在，文件名的判断  ***.***, **.**.**-**.zip
            tmp_file_name = file_name.split('.')[0]
            if tmp_file_name in resource_list or file_name.split(".")[-1] == "zip" or file_name.startswith("."):
                continue
            if (file_name.split(".")[-1] != "whl"):
                #             print(file_name)
                raise Exception("请手动将后缀为tar.gz 格式的依赖 [{}] 编译为whl，替换掉tar.gz 文件"
                                "1：解压tar.gz"
                                "2：进入解压目录执行 python setup.py bdist_wheel"
                                "3：将dist文件夹中的whl 文件拷贝至依赖包中".format(file_name))
            file_path = os.path.join(dst_dir, file_name)
            new_path = file_path.replace('.whl', '.zip')
            os.rename(file_path, new_path)

    # 3.通过 ODPS api 上传zip依赖文件，指定archive格式
    def upload_zip_by_odps(self, dst_dir):
        """
        :param dst_dir: 依赖所在的目录
        :return:
        """
        resource_list = [i.name for i in self.o.list_resources()]
        file_list = os.listdir(dst_dir)
        file_list = [i for i in file_list if not i.startswith(".")]
        for file_name in file_list:

            if file_name in resource_list or "zip" not in file_name:
                print(f"-------->:{file_name}依赖已存在")
                continue
            file_path = os.path.join(dst_dir, file_name)
            # 依赖需要以rb的方式打开
            print(file_name)
            # TODO  资源文件大上传失败，需记录上传的资源名称到log中，设置上传文件timeout
            try:
                self.o.create_resource(file_name, 'archive', file_obj=open(file_path, 'rb'))
                print(f"-------->:{file_name}依赖文件上传成功")
                time.sleep(1)
            except Exception as e:
                raise Exception("依赖文件过大，请手动上传")
        odps_resource_list = [self.o.get_resource(archive_name) for archive_name in file_list]

        return odps_resource_list

    # 4.编写的py文件中需要自动声明依赖，原始py文件中必须要有 __init__ 方法，且该方法需要有 import sys
    # TODO 更改py文件需要更改的点。1、是否有__init__方法，没有，则主动创建_init__,存在，在__init__ 方法中添加引用声明
    def generate_odps_py(self, file_path, pkg_path):
        """
        :param file_path: UDF 本地路径
        :param pkg_path:  依赖所在的目录
        :return:
        """
        with open(file_path, 'r') as fp:
            lines = fp.readlines()

        index = lines.index('        import sys\n')
        index_end = lines.index([i for i in lines if 'evaluate' in i][0])

        new_lines = lines[:index + 1] + lines[index_end:]
        for file in os.listdir(pkg_path):
            insert_value = "        sys.path.insert(0, 'work/{}')\n".format(file)
            new_lines.insert(index + 1, insert_value)

        with open(file_path, 'w') as fp:
            fp.writelines(new_lines)

    # 5.上传 PY 文件
    def upload_py(self, odps_py_name, py_file_path):
        # TODO 文件已存在还需处理
        """
        :param odps_py_name: py 文件在ODPS 中的资源名称
        :param py_file_path:  py 文件所在的路径
        :return:
        """
        self.o.create_resource(odps_py_name, 'py', file_obj=open(py_file_path))
        print("-------->:py文件上传成功")

    # 6. 添加额外的资源，额外的资源包括
    def get_extra_resource(self, res_list):
        """
        :param res_list: UDF所需要的额外资源列表，包括不限于 txt，pkl，py模块
        :return: 对应在ODPS 中的资源列表
        """

        resource_list = []
        for res in res_list:
            recourse = self.o.get_resource(res)
            resource_list.append(recourse)
        return resource_list

    # 6.创建 UDF
    def create_udf(self, odps_py_name, class_type, odps_resource_list, odps_func_name, extra_res_list):
        # TODO udf 已存在还需处理
        """
        :param odps_py_name: ODPS 中的py 资源名称
        :param class_type: py资源模块和类名  e.g. module A 中的class MyClass   A.MyClass
        :param odps_resource_list: 依赖资源列表
        :param odps_func_name: UDF 名称
        :param extra_res_list: 额外的资源列表
        :return:
        """
        # 获取 odps 中上传的py文件资源
        recourse = self.o.get_resource(odps_py_name)
        # py文件资源和依赖资源一起添加至列表
        odps_resource_list.append(recourse)
        # 需要关联的额外资源
        if extra_res_list:
            extra_res = self.get_extra_resource(extra_res_list)
            odps_resource_list.extend(extra_res)

        function = self.o.create_function(f'{odps_func_name}', class_type=f'{class_type}', resources=odps_resource_list)
        print(f"-------->:{odps_func_name}   UDF创建成功")

    # 7. 根据名称删除资源列表
    def delete_recourse(self, res_list):
        for res_name in res_list:
            self.o.delete_resource(res_name)

    # 8. 删除指定UDF
    def delete_udf(self, udf_list):
        for udf_name in udf_list:
            self.o.delete_function(udf_name)

    # TODO 上传到ODPS 中的文件如果已存在（重名，则会抛出错误），是否覆盖？
    def main(self, model_dir, pkg_dir, odps_py_name, py_file_path, class_type, odps_func_name, extra_resource_list):
        """
        :param model_dir: 模型文件所在路径 mlflow 风格
        :param pkg_dir: 依赖文件下载到指定目录
        :param odps_py_name: 上传的py文件在odps资源中的名称
        :param py_file_path: 需要上传的py文件的路径
        :param class_type: odps py文件中的模型名和类名 eg. a_pandas.use_api_pandas, 其中a_pandas 对应的资源名称
        :param odps_func_name: 创建的odps UDF 函数名称
        :return:
        """
        # 如果依赖文件夹不存在，则下载依赖

        odps_func_dict = ziya_list_models.list_odps_models()
        if odps_func_name in odps_func_dict["odps_models"]:
            raise Exception(f"model {odps_func_name} already exists in ODPS")

        if not os.path.exists(pkg_dir):
            self.download_requirement(pkg_dir, model_dir)

        # 将模型文件上传，TODO存在问题：mflow保存的模型文件名均为 python_model.pkl,重复上传会报错,此处可以重命名
        model_path = os.path.join(model_dir, "python_model.pkl")

        if model_dir.endswith("/"):
            model_dir = model_dir.rstrip("/")

        model_name = os.path.split(model_dir)[1]
        res_name = model_name + "_model.pkl"

        self.o.create_resource(res_name, 'file', file_obj=open(model_path, 'rb'))

        if extra_resource_list:
            extra_resource_list = eval(extra_resource_list)
            extra_resource_list.append(res_name)
        else:
            extra_resource_list = [res_name]

        self.change_suffix(pkg_dir)
        odps_resource_list = self.upload_zip_by_odps(pkg_dir)
        self.generate_odps_py(py_file_path, pkg_dir)
        self.upload_py(odps_py_name, py_file_path)
        self.create_udf(odps_py_name, class_type, odps_resource_list, odps_func_name, extra_resource_list)
