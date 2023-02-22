import docker
import pandas as pd
import os
import requests
import socket
from core.utils.oss_util import OssUtil


class DockerServeDeploy:

    def __init__(self):
        self.client = docker.from_env()
        self.my_oss = OssUtil()

    def create_image(self, model_name_version, file_path, docker_file):
        # 根据dockerfile 创建镜像
        print("creating images...")
        model_name, model_version = self.my_oss.split_model_name_version(model_name_version)
        # 镜像名称必须是小写
        tag = f"{model_name.lower()}:{model_version}"
        image = self.client.images.build(
            path=file_path,
            dockerfile=docker_file,
            tag=tag,
        )
        return image[0]

    def download_model(self, model_path, model_name_version):
        # 下载模型文件至当前路径
        self.my_oss.download_model_from_oss(model_path, model_name_version)

    def predict_by_docker(self, model_name_version, df):

        # 根据模型名称获取对应容器
        container_list = self.get_container_list()
        if model_name_version not in container_list:
            return f"model{model_name_version} not exist in docker, please check"

        # 根据容器对应的端口调用docker服务
        container = self.client.containers.get(model_name_version)
        # 获取对应的本地端口
        port = container.ports['9000/tcp'][0]['HostPort']
        # 调用服务

        url = f" http://127.0.0.1:{port}/{model_name_version}"

        data = df.to_json(orient="records")
        response = requests.post(url, data)
        result = response.content
        return result

    def get_container_list(self):
        # 获取模型容器列表
        container_list = self.client.containers.list()
        container_name_list = [i.name for i in container_list]
        return container_name_list

    def get_local_port(self):
        # 检查本地可用端口 9100～9500
        ports = range(9100, 9501)
        ip = '127.0.0.1'
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        for port in ports:
            try:
                # 本地端口已被占用
                s.connect((ip, port))
                s.shutdown(2)
                continue
            except:
                return port

    def create_deployment(self, model_name_version):
        # 1、下载模型到指定目录
        self.download_model("./", model_name_version)
        # 2、根据dockerfile 创建镜像，启动容器 TODO Dockerfile 路径待确定
        image = self.create_image(model_name_version, "./", "Dockerfile")

        # 3、生成模型服务脚本,指定模型存放目录/home（针对模型加载）
        template_py = """
from flask import Flask
from flask import request
import joblib
import pandas as pd
import mlflow

app = Flask(__name__)


@app.route('/model_path', methods=['GET','POST'])
def main():

    request_data = request.stream.read().decode()
    model = mlflow.pyfunc.load_model("/home/model_path")

    df = pd.read_json(request_data)
    print("input data shape is ...")
    print(df.shape)
    result = model.predict(df)
    result = pd.DataFrame(result).to_json(orient="records")
    return result


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9000)

"""

        template_py = template_py.replace('model_path', model_name_version)
        with open(f"./ziya_serve.py", 'w') as fp:
            fp.write(template_py)

        # 4、创建容器
        pwd = os.path.abspath('.')
        # 以模型名称版本作为容器名称
        # TODO 容器查询本地端口 名字_ziya
        local_port = self.get_local_port()

        # 如果已存在则无需创建
        container_list = self.get_container_list()
        if model_name_version in container_list:
            print(
                f"container {model_name_version} already exists, "
                f"You have to remove (or rename) that container to be able to reuse that name.")
            container = self.client.containers.get(model_name_version)
        else:
            container = self.client.containers.run(image.short_id, name=f'{model_name_version}',
                                                   # 端口映射  容器port:本地port
                                                   ports={"9000": [local_port]},
                                                   detach=True, remove=False, tty=True,
                                                   # 指定映射目录,映射至docker中的home目录
                                                   volumes=[f'{pwd}:/home'])

        # 5、读取模型依赖，容器中安装模型依赖
        cmp_pip = f"pip install -r /home/{model_name_version}/requirements.txt"
        container.exec_run(cmp_pip, stream=True)

        # 6、容器中开启模型服务
        cmd_run = f"python /home/ziya_serve.py"
        container.exec_run(cmd_run, stream=True)
        return "success"


if __name__ == '__main__':
    # main()
    # container = client.containers.run('docker-example',
    #                                   detach=True)
    # container_list = client.containers.list()
    # # print(container_list)
    # for i in container_list:
    #     print(i.short_id)
    #     print(i.name)
    d = DockerServeDeploy()
    # result = d.get_local_port()
    # print(result)
    # result = d.get_container_list()
    # print(result)
    # container = d.client.containers.get("ovo_4")
    # print(container.ports['9000/tcp'][0]['HostPort'])
    # d.main('1')
    # df = pd.read_csv(r"/Users/zhangchaochao/PycharmProjects/MyProject/ali_utils/abnormal_0.csv")
    # print(df.shape)
    # result = d.predict_by_docker('AnomalyDetection_acoe_v1', df)
    # print(pd.read_json(result))

    #  OSS、ray、docker、nas
