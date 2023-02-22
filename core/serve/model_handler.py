import pandas as pd
from core.info import list_models as ziya_list_models
from core.run import run_model
from core.utils import generate_client

# 设置本地log文件记录模型调用情况
import logging
from logging import handlers


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, filename, printflag=False, level='info', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        if printflag:
            sh = logging.StreamHandler()
            sh.setFormatter(format_str)
            self.logger.addHandler(sh)
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')  # 往文件里写入#指定间隔时间自动生成文件的处理器
        th.setFormatter(format_str)
        self.logger.addHandler(th)


# 主要处理post 请求，模型调用部分
# 目前仅支持已部署模型的调用
# 加入指定后端参数 headers  ziya-backend
def handler(m, df):
    df = pd.DataFrame(df)

    try:
        # 读取配置中的阿里云log 配置
        logger = generate_client.generate_log_handler()
    except:
        # 启用本地log日志
        import time

        logpath = './log.log'
        logger = Logger(logpath, level='info').logger
        logger.info('==={}===\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    print(f'model_handler: model_name_version {m.name}, model_param {m.path}')

    m.stopped = False
    m.proc = None
    m.handlers = None

    model_name = m.name

    backend = m.backend

    # 判断是否在headers中指定参数backend，如果指定，则通过指定后端计算（需判断是否存在），未指定，按照FC、ray的逻辑计算
    if backend == 'fc':

        # 查询 fc 中模型列表
        fc_list = ziya_list_models.list_fc_models()["fc_models"]
        if model_name in fc_list:
            response = run_model._run_model(model_name, backend, df)
        else:
            # 提示模型在指定后端不存在
            response = 'model not exists in fc, please retry'
        return response

    elif backend == "ray":
        # 查询 ray 中模型列表
        ray_list = ziya_list_models.list_ray_models()["ray_models"]

        if model_name in ray_list:
            response = run_model._run_model(model_name, backend, df)
        else:
            # 提示模型在指定后端不存在
            response = 'model not exists in ray, please retry'
        return response

    elif backend == "docker":

        docker_list = ziya_list_models.list_docker_models()["docker_models"]

        if model_name in docker_list:
            response = run_model._run_model(model_name, backend, df)
        else:
            response = 'model not exists in docker, please retry'
        return response

    elif backend is None:
        # 未指定计算后端：
        fc_list = ziya_list_models.list_fc_models()["fc_models"]
        ray_list = ziya_list_models.list_ray_models()["ray_models"]

        # 查询模型是否在fc中
        if model_name in fc_list:
            response = run_model._run_model(model_name, "fc", df)
            # 结果转为json

            if not isinstance(response, pd.DataFrame):
                # 如果返回的不是df 格式数据，说明模型调用出错
                response = f"FC 调用出错，具体出错原因请参考{response}"

            response = response.to_json()
            source = "FC"
            flag = 1

        elif model_name in ray_list:
            response = run_model._run_model(model_name, "ray", df)
            # 结果转为json
            response = response.to_json()
            source = "RAY"
            flag = 1

        else:
            # 模型不存在，不做处理，返回
            response = False
            source = None
            flag = 0
        # 数据存入日志服务
        # 字段格式：time，model_name, model_version, source, status
        predict_log = f"model_name_version: {m.name}, source: {source}, flag: {flag}"
        logger.info(predict_log)
        return response
    else:
        # 指定计算后端名称错误，
        response = "Error in specifying the calculation backend name. Please confirm"
        return response

        # 数据库字段格式, 数据存入sqlite数据库
        # time(DATA), model_name(text), model_version(text), source(text), status(int)
        # con = sqlite3.connect("/home/admin/zcc/jupyter/sqlite.db")
        # cur = con.cursor()
        # insert_sql = "INSERT INTO model_use VALUES(?, ?, ?, ?, ?, ?)"
        # now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # cur.execute(insert_sql, (None, now_time, m.name, m.tag, source, flag))
        # con.commit()
        # cur.close()
        # con.close()


def start_serve(model_name):
    # 拉起服务
    # 拉起服务 ，服务名称必须指定为规定格式  model_name + version  e.g. add_n_modelv1
    pass
