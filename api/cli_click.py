# encoding: utf-8
# ziya cli 接口
"""
子牙算法平台接口，用于模型的部署、评测，目前支持的计算后端[RAY、FC、ODPS]
"""

import click
import os
import subprocess
import re
import shutil

from core.deploy import ziya_deploy as deploy
from core.utils import generate_client
from core.trigger import ziya_trigger as trigger
from core.run import run_model
from core.datafetch.ziya_datafetch_odps import DataFetch
from core.utils import oss_util
from core.serve import ziya_serve as ziya_url_serve
from core.commit import ziya_commit
from core.commit import create_template

@click.group()
@click.version_option(version="1.0.2")
def cli():
    """
"ziya support python based algorithm/model deployment and benchmark on multiple cloud and local computation
backends: fc (Alibaba Cloud Function Compute)，odps (Alibaba Cloud MaxCompute) and
ray (Ray from UC Berkeley RISE Lab). type ziya status for more information",
"doc": "http://yuque",

    """
    generate_client.check_conf_exists()
    pass


def get_abs_path(dir_path):
    return os.path.join(os.getcwd(), dir_path)


# @cli.command("deploy")
# @click.option("--backend", help="computing backend, support FC、RAY、ODPS",
#               type=click.Choice(["fc", "ray", "odps"]))
# @click.option("--model_name_version", help="deployed model name and version")
# @click.option("--oss", help="default False, if True, model path enter OSS model name, "
#                             "else enter local model path", default=False)
# @click.option("-m", "--model_path", help="model path")
# @click.option("-env_path", "--env_path", help="the model needs to depend on the download location", default=None)
# @click.option("-odps_py_name", "--odps_py_name", help="odps py file name, end with .py", default=None)
# @click.option("-py_file_path", "--py_file_path", help="current py file path", default=None)
# @click.option("-class_type", "--class_type", help="odps py file module.class", default=None)
# @click.option("-e", "--extra_resource_list", help="extra resource_list", default=None)
# @click.option("-verbose", "--verbose", help="show deploy details", default=None)
# # 44-49 odps_config  verbose True
# def ziya_deploy(backend, model_name_version, model_path, oss, env_path, odps_py_name, py_file_path, class_type,
#                 extra_resource_list, verbose):
#     if not all([backend, model_name_version, model_path]):
#         click.echo("Please run [ziya deploy --help] to view the parameter information")
#         return
#     backend = backend.lower()
#     # 统一将模型路径改为绝对路径
#
#     if oss:
#         try:
#             o = OssUtil()
#             o.download_model_from_oss(model_path)
#         except Exception as e:
#             click.echo(e)
#             return
#
#     model_path = get_abs_path(model_path)
#     if backend == "fc":
#         result = deploy.deploy_model_to_fc(model_name_version, model_path, verbose)
#         click.echo(result)
#         return result
#     elif backend == "ray":
#         result = deploy.deploy_model_to_ray(model_name_version, model_path, verbose)
#         click.echo(result)
#         return result
#     elif backend == "odps":
#         result = deploy.deploy_model_to_odps(model_dir=model_path, pkg_dir=env_path, odps_py_name=odps_py_name,
#                                              py_file_path=py_file_path, class_type=class_type,
#                                              odps_func_name=model_name_version,
#                                              extra_resource_list=extra_resource_list, verbose=verbose)
#         click.echo(result)
#         return result
#     else:
#         click.echo("指定计算后端不存在，目前支持后端[RAY、FC、ODPS]")
#         return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"
#
#     pass


@cli.command("deploy")
@click.option("--backend", help="computing backend, support FC、RAY、ODPS、docker",
              type=click.Choice(["fc", "ray", "odps", "docker"]))
@click.option("--model_name_version", help="deployed model name and version")
@click.option("--odps_cfg", help="config for deploy model to odps, type:dict", default=None)
@click.option("-verbose", "--verbose", help="show deploy details", default=None)
def ziya_deploy(backend, model_name_version, odps_cfg, verbose):
    # TODO odps 部署暂不支持
    if backend == "odps":
        print(f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]")
        return f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]"

    if not all([backend, model_name_version]):
        click.echo("Please run [ziya deploy --help] to view the parameter information")
        return
    backend = backend.lower()
    # 统一将模型路径改为绝对路径

    try:
        o = oss_util.OssUtil()
        # 模型默认存储路径为 /share/ziya/models
        default_model_path = "/share/ziya/models"
        o.download_model_from_oss(default_model_path, model_name_version)
    except Exception as e:
        click.echo(e)
        return

    public_model_path = os.path.join(default_model_path, model_name_version)
    model_path = get_abs_path(public_model_path)
    if backend == "fc":
        result = deploy.deploy_model_to_fc(model_name_version, model_path, verbose)
        click.echo(result)
        return result
    elif backend == "ray":
        result = deploy.deploy_model_to_ray(model_name_version, model_path, verbose)
        click.echo(result)
        return result
    elif backend == "docker":
        result = deploy.deploy_model_to_docker(model_name_version, model_path, verbose)
        click.echo(result)
    elif backend == "odps":
        if odps_cfg is None:
            return "当前odps模型部署参数未配置，请指定odps_cfg参数"
        try:
            env_path = odps_cfg.get("env_path")
            odps_py_name = odps_cfg.get("odps_py_name")
            py_file_path = odps_cfg.get("py_file_path")
            class_type = odps_cfg.get("class_type")
            model_name_version = odps_cfg.get("model_name_version")
            extra_resource_list = odps_cfg.get("extra_resource_list")
        except:
            return "odps_cfg 参数配置错误，请确定所有字段均存在[env_path,odps_py_name,py_file_path,class_type,model_name_version,extra_resource_list]"

        result = deploy.deploy_model_to_odps(model_dir=model_path, pkg_dir=env_path, odps_py_name=odps_py_name,
                                             py_file_path=py_file_path, class_type=class_type,
                                             odps_func_name=model_name_version,
                                             extra_resource_list=extra_resource_list, verbose=verbose)
        click.echo(result)
        return result
    else:
        click.echo("指定计算后端不存在，目前支持后端[RAY、FC、ODPS]")
        return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"

    pass


# @cli.command("old_benchmark")
# @click.option("-b", "--backend", help="computing backend, support FC、RAY、ODPS")
# @click.option("-mnv", "--model_name_version", help="deployed model name")
# @click.option("-oss", "--oss", help="default False, if true, input file path enter oss file name,"
#                                     "else enter local file path", default=False)
# @click.option("-i", "--df_file_path", help="input file path [json]")
# @click.option("-file_label", "--file_label", help="input file true label")
# @click.option("-table", "--table_name", help="odps table name", default=None)
# @click.option("-field_tuple", "--field_tuple", help="odps field tuple", default=None)
# @click.option("-odps_label", "--odps_label", help="odps true label field", default=None)
# @click.option("-verbose", "--verbose", help="show details", default=None)
# def old_ziya_benchmark(backend, model_name_version, oss, df_file_path, file_label, table_name, field_tuple, odps_label,
#                        verbose):
#     if not all([backend, model_name_version, df_file_path, file_label]):
#         click.echo("Please run [ziya benchmark --help] to view the parameter information")
#         return
#
#     backend = backend.lower()
#
#     if oss:
#         try:
#             o = oss_util.OssUtil()
#             df_file_path = o.get_df_from_oss(df_file_path)
#         except Exception as e:
#             click.echo(e)
#             return
#
#     if backend == "fc" or backend == "ray":
#         result = benchmark.main(backend, model_name_version=model_name_version, df_file_path=df_file_path,
#                                 y_true=file_label, verbose=verbose)
#         click.echo(result)
#         return result
#     elif backend == "odps":
#         result = benchmark.main_odps(backend, model_name_version=model_name_version, table_name=table_name,
#                                      field_tuple=field_tuple, label=odps_label, verbose=verbose)
#         click.echo(result)
#         return result
#     else:
#         click.echo("指定计算后端不存在，目前支持后端[RAY、FC、ODPS]")
#         return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"


# @cli.command("trigger")
# @click.option("-s", "--service_name", "the service where the function is deployed")
# @click.option("-f", "--func_name", "deployed function name")
# @click.option("-o", "--operate_type", "operate type")
# @click.option("-name", "--trigger_name", "trigger name")
# @click.option("-type", "--trigger_type", "trigger type")
# @click.option("-r", "--region", "fc server region")
# @click.option("-account", "--account_id", "fc server account id", default=None)
# @click.option("-prefix", "--prefix", "oss trigger prefix", default=None)
# @click.option("-suffix", "--suffix", "oss trigger suffix", default=None)
# @click.option("-b", "--bucket", "oss bucket name", default=None)
# @click.option("-log_project", "--log_project", "sls log project", default=None)
# @click.option("-log_store", "--trigger_store", "sls trigger store", default=None)
# @click.option("-source", "--record_store", "sls record store")
# def ziya_trigger(service_name, func_name, operate_type, trigger_name, trigger_type, region, account_id, prefix, suffix,
#                  bucket, log_project, trigger_store, record_store):
#     operate_type = operate_type.lower()
#     t = trigger.ZiYaTrigger(service_name, func_name)
#     if operate_type == "delete":
#         result = t.delete_trigger(trigger_name)
#         click.echo(result)
#         return result
#
#     elif operate_type == "get":
#         result = t.get_trigger(trigger_name)
#         click.echo(result)
#         return result
#
#     elif operate_type == "list":
#         result = t.list_trigger()
#         click.echo(result)
#         return result
#
#     elif operate_type == "add":
#
#         if trigger_type == "oss":
#
#             result = t.create_oss_trigger(trigger_name=trigger_name, region=region, account_id=account_id,
#                                           prefix=prefix, suffix=suffix, bucket=bucket)
#             click.echo(result)
#             return result
#
#         elif trigger_type == "sls":
#
#             result = t.create_sls_trigger(trigger_name=trigger_name, region=region, account_id=account_id,
#                                           log_project=log_project, trigger_store=trigger_store,
#                                           record_store=record_store)
#             click.echo(result)
#             return result
#
#         else:
#             click.echo("目前仅支持两种类型的trigger [sls/oss]")
#             return
#
#     elif operate_type == "update":
#         click.echo("目前暂不支持trigger更新")
#         return
#
#     else:
#         click.echo("目前支持的操作[delete/get/add]")
#         return


# @cli.command("list")
# @click.option("-b", "--backend", help="computing backend, support FC、RAY、ODPS, default None show all models",
#               default=None)
# def ziya_list(backend):
#     model_json = ziya_list_models.list_ziya_models(backend)
#     click.echo(model_json)
#     return model_json

# cli init --model_name
# 简单demo 代码 hello world ，补充demo数据集

@cli.command("init")
@click.option("--model_name_version", help="Initialize before model submission")
def ziya_init_model(model_name_version):
    if not model_name_version:
        # 未指定 model_name_version 参数
        print("Please specify the model name")
        return

    # 生成模型部署模板
    regex = re.compile(r"_v\d+")
    result = regex.findall(model_name_version)
    if not result:
        print("The model name does not meet the requirements, please refer to my_model_v1")
        return
    create_template.pack_model(model_name_version)


@cli.command("commit")
@click.option("--model_name_version", help="model name version specified by init command")
@click.option("--describe", help="model description information, default None", default=None)
def ziya_commit_model(model_name_version, describe=None):

    # 如果当前路径已存在模型文件，可能是重复打包，或者打包成功上传失败（oss未正确配置导致），删除当前模型文件
    if os.path.exists(model_name_version):
        shutil.rmtree(f"./{model_name_version}")

    # 检查当前是否存在模型封住模版代码（commit_init.py），不存在，则提示运行 ziya init ，存在，运行该脚本
    if not os.path.exists("./commit_init.py"):
        return "Run the Ziya init command and modify the commit_init.py！"

    command_list = [f'python commit_init.py']
    result = subprocess.run(command_list, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding="utf-8", )
    if result.returncode == 0:
        # 脚本运行成功
        print("Model packaging completed, uploading to model library")
    else:
        try:
            result = subprocess.run([f'python3 commit_init.py'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    encoding="utf-8", )
            if result.returncode == 0:
                # 脚本运行成功
                print("Model packaging completed, uploading to model library")
            else:
                # 脚本运行失败
                print("Please check whether Python can be used correctly")
        except:
            return "Model packaging error, please check the python running environment or script commit_ Is the init script in error"

    try:
        result = ziya_commit.commit_model_to_oss(model_name_version, describe)
    except Exception as e:
        click.echo(e)
        return e
    click.echo(result)
    return


# @cli.command("datasets")
# def ziya_list_datasets():
#     try:
#         data_sets = oss_util.OssUtil().list_data_sets()
#         click.echo(data_sets)
#         return data_sets
#     except Exception as e:
#         click.echo(e)
#         return


@cli.command("run")
@click.option("--backend", help="computing backend, support FC、RAY、ODPS、docker") # 不指定backend，调用路由ziya serve
@click.option("--model_name_version", help="run model")
@click.option("--file_type", help="file type [local/oss]")
@click.option("--file_name", help="file path or file name in oss")
@click.option("--odps_cfg", help="config for deploy model to odps, type:dict", default=None)
def ziya_run(backend, model_name_version, file_type, file_name, odps_cfg):

    # TODO odps 调用暂不支持
    if backend == "odps":
        print(f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]")
        return f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]"

    if not backend:
        # 如果未指定计算后端，默认调用ziya server 接口
        # click.echo("Please run [ziya run --help] to view the parameter information", file=sys.stdin)
        result = run_model.run_with_server(model_name_version, file_type, file_name)
        return result

    backend = backend.lower()
    if backend == 'fc' or backend == 'ray' or backend == 'docker':
        result = run_model.run_main_backend(model_name_version, backend, file_type, file_name)

    elif backend == 'odps':
        try:
            table_name = odps_cfg.get("table_name")
            field_tuple = odps_cfg.get("field_tuple")
            option = odps_cfg.get("option")
        except:
            return "odps_cfg 参数配置错误，请确定所有字段均存在[table_name,field_tuple,option]"
        result = run_model._run_model_odps(model_name_version, backend, table_name, field_tuple, option)

    else:
        click.echo("指定计算后端不存在，目前支持后端[RAY、FC、ODPS]")
        return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"

    # if output:
    #     suffix = output.split(".")[-1]
    #     if suffix == "csv":
    #         result.to_csv(output)
    #     elif suffix == "pkl":
    #         result.to_pickle(output)
    #     else:
    #         click.echo("目前文件保存仅支持csv、pandas pkl 存储方式")
    # else:
    #     click.echo(result)
    click.echo(result)
    return result


# @cli.command("datafetch")
@click.option("--download_type", help="odps data download type [table/sql]", default=None)
@click.option("--save_type", help="data save type [oss/local]", default=None)
@click.option("--sql", help="odps download sql", default=None)
@click.option("--file_path", help="save data path", default=None)
@click.option("--table_name", help="odps table name", default=None)
@click.option("--oss_name", help="upload to oss, oss file name", default=None)
@click.option("--partition", help="odps partition info", default=None)
def ziya_datafetch(download_type, save_type, sql, file_path, table_name, oss_name, partition):
    data_fetch = DataFetch()
    if download_type == "table":
        if save_type == "oss":
            data_fetch.download_data_by_table_to_oss(table_name, oss_name)
        elif save_type == "local":
            data_fetch.download_data_by_table_to_local(table_name, file_path, partition)
        else:
            click.echo("目前存储仅支持 [oss/local] 两种")
            return
    elif download_type == "sql":
        if save_type == "oss":
            data_fetch.download_data_by_sql_to_oss(sql, oss_name)
        elif save_type == "local":
            data_fetch.download_data_by_sql_to_local(sql, file_path)
        else:
            click.echo("目前存储仅支持 [oss/local] 两种")
            return
    else:
        click.echo("目前odps仅支持 [table/sql] 两种下载方式")
        return


from core.info.run_status import RunStatus


@cli.command("info")
@click.option("--force_update", help="Get real-time information", is_flag=True)
def ziya_info(force_update):

    run_client = RunStatus()
    model_info = run_client.get_model_and_datasets_info(update=force_update)
    # click.echo(model_info)
    print(model_info)
    return model_info


from core.benchmark.ziya_benchmark_v2 import Benchmark


@cli.command("benchmark")
@click.option("--model_name_version", help="model name")
@click.option("--datasets", help="oss datasets", default="*")
@click.option("--backend", help="computing backend", default="*")
@click.option("--force_update", help="force update ", is_flag=True)
# 默认从oss查询缓存，输出时间戳，提示force_update
def ziya_benchmark(model_name_version, datasets, backend, force_update):

    # TODO odps benchmark 暂不支持
    if backend == "odps":
        print(f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]")
        return f"benchmark操作[odps]暂不支持，目前仅支持[RAY/FC]"

    b = Benchmark()
    result = b.new_benchmark(model_name_version, datasets, backend, force_update)
    print(result)
    return result


@cli.command("serve")
@click.option("--port", default=8888, help='port for the ziya server')
def ziya_serve(port):
    ziya_url_serve.run(port)


cli.add_command(ziya_deploy)
cli.add_command(ziya_benchmark)
# cli.add_command(ziya_trigger)
# cli.add_command(ziya_list)
# cli.add_command(ziya_list_datasets)
cli.add_command(ziya_init_model)
cli.add_command(ziya_commit_model)
cli.add_command(ziya_run)
# cli.add_command(ziya_status)
cli.add_command(ziya_info)
cli.add_command(ziya_serve)
# cli.add_command(ziya_datafetch)

if __name__ == "__main__":
    cli()

    # init commit deploy run
    # 单文件 api
    # 目录结构
    # 后端  模板一：只生成需要用到的模版
    # 根据conda、req 生成环境，生成readme，使用说明

    # 梳理模型兼容后端情况，部署情况说明（mlflow、fc不支持原因）
    # odps 模型部署（单文件多文件）多文件->单文件
    # ray 远端节点部署

