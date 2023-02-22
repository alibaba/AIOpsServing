import configparser
import os
import sys
import logging

from alibabacloud_fc_open20210406.client import Client as FC_Open20210406Client
from alibabacloud_tea_openapi import models as open_api_models

logging.basicConfig(level=logging.CRITICAL)
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

"""

"""

CONF_PATH = os.path.join(os.path.expanduser('~'), '.ziya.conf')


def download_conf_from_oss():
    # TODO 从oss中获取默认config 配置，配置信息加密

    pass


def generate_ziya_conf():
    """
    当前目录中没有查找到ziya.conf， 自动生成配置模版
    :return:
    """

    with open(CONF_PATH, 'w') as fp:
        template_str = """
[odps]
project_name=odps project name
access_id=odps access id
access_key=odps access key
end_point=odps endpoint

[fc]
end_point=fc endpoint
access_id=fc access id
access_key=fc access key
fc_service_name=fc service name

[oss]
project_name=oss project name
end_point=oss endpoint
access_id=oss access id
access_key=oss access key

[log]
access_id=aliyun log access id
access_key=aliyun log access key
end_point=aliyun log endpoint
project_name=aliyun log project name
log_store_name=aliyun log logstore name
     
        """
        fp.write(template_str)


def check_conf_exists():
    if not os.path.exists(CONF_PATH):
        print(f"路径[{os.path.expanduser('~')}]下未检查到子牙配置文件，正在生成配置模板，请按需配置指定路径子牙配置文件")
        generate_ziya_conf()
        sys.exit(1)


# 通过config获取 AK 信息
def get_config(section, key):
    config = configparser.ConfigParser()
    config.read(CONF_PATH)
    return config.get(section, key)


# 每个客户端单独根据函数生成，为了防止调用时干扰，主要和ray客户端有关
def generate_fc_client():
    import fc2
    endpoint_url = get_config("fc", "end_point")
    fc_access_key_id = get_config("fc", "access_id")
    fc_access_key_secret = get_config("fc", "access_key")
    fc_service_name = get_config("fc", "fc_service_name")
    try:
        client = fc2.Client(
            endpoint=endpoint_url,
            accessKeyID=fc_access_key_id,
            accessKeySecret=fc_access_key_secret,
            Timeout=600)

        client.list_functions(fc_service_name)
        return client
    except:
        raise Exception(f"""
FC 配置信息出错，请参考以下链接配置正确信息后重试
endpoint信息参考：https://help.aliyun.com/document_detail/52984.html
用户AK信息参考：https://help.aliyun.com/document_detail/295894.html
创建服务或选择已有服务：https://help.aliyun.com/document_detail/51783.html
""")


def generate_aliyun_client() -> FC_Open20210406Client:
    """
    使用AK&SK初始化账号Client
    @param access_key_id:
    @param access_key_secret:
    @return: Client
    @throws Exception
    """
    access_key_id = get_config("fc", "access_id")
    access_key_secret = get_config("fc", "access_key")
    endpoint = get_config("fc", "end_point")

    config = open_api_models.Config(
        # 您的 AccessKey ID,
        access_key_id=access_key_id,
        # 您的 AccessKey Secret,
        access_key_secret=access_key_secret,
        read_timeout=1000000,  # 读超时时间 单位毫秒(ms) 100s
        connect_timeout=5000000  # 连接超时 单位毫秒(ms)  50s
    )
    # 访问的域名

    # config.endpoint = f'1816972088710034.cn-beijing.fc.aliyuncs.com'
    config.endpoint = endpoint
    return FC_Open20210406Client(config)


def generate_ray_client():
    try:
        from core.deploy.deploy_to_ray import ray_serve
        client = ray_serve
        return client
    except:
        raise Exception(f"""
当前ray未启动，若想支持ray，请执行命令 
1、'ray start --head' 
2、 'serve start' 启动ray服务 
""")


def generate_odps_client():
    from odps import ODPS
    access_id = get_config("odps", "access_id")
    access_key = get_config("odps", "access_key")
    project_name = get_config("odps", "project_name")
    end_point = get_config("odps", "end_point")
    try:
        # TODO：odps 客户端验证目前通过列举表来判断key，id 是否正确，待优化
        client = ODPS(access_id, access_key, project_name, endpoint=end_point)
        for table in client.list_tables():
            pass
        return client
    except:
        raise Exception(f"""
ODPS 配置信息出错，请参考以下链接配置正确信息后重试
endpoint信息参考：https://help.aliyun.com/document_detail/89754.html
用户AK信息参考：https://help.aliyun.com/document_detail/183946.html
项目名称参考：https://help.aliyun.com/document_detail/27815.html
""")


def generate_oss_client():
    import oss2
    access_id = get_config("oss", "access_id")
    access_key = get_config("oss", "access_key")
    project_name = get_config("oss", "project_name")
    end_point = get_config("oss", "end_point")
    try:
        auth = oss2.Auth(access_id, access_key)

        bucket = oss2.Bucket(auth, end_point, project_name)
        bucket.list_objects()

        return bucket
    except:
        raise Exception(f"""
OSS 配置信息出错，请参考以下链接配置正确信息后重试
endpoint信息参考：https://help.aliyun.com/document_detail/31837.html
用户AK信息参考：https://help.aliyun.com/document_detail/93720.html
创建服务或选择已有服务：https://help.aliyun.com/document_detail/31885.html
""")


def generate_docker_client():
    try:
        from core.deploy.deploy_to_docker import DockerServeDeploy
        client = DockerServeDeploy()
        return client
    except:
        raise Exception(f"""
请确认本地docker可正常运行
""")


# 根据log_store 名称获取对应日志仓库客户端
def generate_log_handler():
    access_id = get_config("log", "access_id")
    access_key = get_config("log", "access_key")
    project_name = get_config("log", "project_name")
    end_point = get_config("log", "end_point")
    log_store_name = get_config("log", "log_store_name")

    import logging, logging.config
    from aliyun.log import LogClient

    try:
        client = LogClient(end_point, access_id, access_key)
        # 测试key id 是否正确
        result = client.list_project()

        ziya_sys_log_conf = {'version': 1,
                             'formatters': {'rawformatter': {'class': 'logging.Formatter',
                                                             'format': '%(message)s'}
                                            },
                             'handlers': {'log_handler': {'()':
                                                              'aliyun.log.QueuedLogHandler',
                                                          'level': 'INFO',
                                                          'formatter': 'rawformatter',

                                                          'end_point': end_point,
                                                          'access_key_id': access_id,
                                                          'access_key': access_key,
                                                          'project': project_name,
                                                          'log_store': log_store_name,
                                                          'extract_json': True,
                                                          },

                                          },

                             'loggers': {'log': {'handlers': ['log_handler', ],
                                                 'level': 'INFO',
                                                 'propagate': False},
                                         }
                             }
        logging.config.dictConfig(ziya_sys_log_conf)

        log_logger = logging.getLogger('log')

        return log_logger

    except:
        raise Exception(f"""
SLS 配置信息出错，请参考以下链接配置正确信息后重试
endpoint信息参考：https://help.aliyun.com/document_detail/29008.html
用户AK信息参考：https://help.aliyun.com/document_detail/175967.html
创建服务或选择已有服务：https://help.aliyun.com/document_detail/48984.html

""")


# fc 服务名称
try:
    service_name = get_config("fc", "fc_service_name")
except:
    service_name = None

try:
    bucket_name = get_config("oss", "project_name")
except:
    bucket_name = None
