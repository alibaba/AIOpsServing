# ziya cli 接口
"""
子牙算法平台接口，用于模型的部署、评测，目前支持的计算后端[RAY、FC、ODPS]
"""

import hug

from core.deploy import ziya_deploy as deploy
from core.benchmark import ziya_benchmark as benchmark
from core.trigger import ziya_trigger as trigger


def main():
    @hug.cli(name="deploy", doc="用于模型部署，支持的计算后端[RAY、FC、ODPS],  e.g. ziya deploy fc model_v1 'mlflow model_path' verbose")
    def ziya_deploy(backend, *args):

        if not args:
            return """
请输入计算后端对应的参数
FC: model_name_version, model_path, verbose
RAY: model_name_version, model_path, verbose
ODPS: model_dir, pkg_dir, odps_py_name, py_file_path, class_type, odps_func_name, extra_resource_list, verbose
            """

        backend = backend.lower()

        if backend == "fc":
            model_name_version = args[0]
            model_path = args[1]
            verbose = args[2] if len(args) == 3 else False

            result = deploy.deploy_model_to_fc(model_name_version, model_path, verbose)
            return result

        elif backend == "ray":
            model_name_version = args[0]
            model_path = args[1]
            verbose = args[2] if len(args) == 3 else False

            result = deploy.deploy_model_to_ray(model_name_version, model_path, verbose)
            return result

        elif backend == "odps":
            model_dir = args[0]
            pkg_dir = args[1]
            odps_py_name = args[2]
            py_file_path = args[3]
            class_type = args[4]
            odps_func_name = args[5]
            extra_resource_list = args[6]
            verbose = args[7] if len(args) == 8 else False

            result = deploy.deploy_model_to_odps(model_dir=model_dir, pkg_dir=pkg_dir, odps_py_name=odps_py_name,
                                                 py_file_path=py_file_path, class_type=class_type,
                                                 odps_func_name=odps_func_name,
                                                 extra_resource_list=extra_resource_list, verbose=verbose)
            return result
        else:
            return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"

    @hug.cli(name="benchmark", doc="用于模型评测，支持的计算后端[RAY、FC、ODPS], e.g. ziya benchmark fc model_v1 'input file path' y_true verbose")
    def ziya_benchmark(backend, *args):
        backend = backend.lower()

        if not args:
            return """
    请输入计算后端对应的参数
    FC: model_name_version, df_file_path, y_true, verbose
    RAY: model_name_version, df_file_path, y_true, verbose
    ODPS: model_name_version, table_name, field_tuple, label, verbose
                """

        if backend == "fc" or backend == "ray":
            model_name_version = args[0]
            df_file_path = args[1]
            y_true = args[2]
            verbose = args[3] if len(args) == 4 else False

            result, y_predict = benchmark.main(backend, model_name_version=model_name_version, df_file_path=df_file_path,
                                    y_true=y_true, verbose=verbose)
            return result

        elif backend == "odps":
            model_name_version = args[0]
            table_name = args[1]
            field_tuple = args[2]
            label = args[3]
            verbose = args[4] if len(args) == 5 else False

            result = benchmark.main_odps(backend, model_name_version=model_name_version, table_name=table_name,
                                         field_tuple=field_tuple, label=label, verbose=verbose)

            return result

        else:
            return "指定计算后端不存在，目前支持后端[RAY、FC、ODPS]"

    """
     - trigger 操作
     - add
        - oss
        - sls
     - delete （name）
     - update （name） TODO
     - get （name）
    
    
    """

    @hug.cli(name="trigger", doc="用于给FC已创建函数添加trigger, 支持[oss/sls]")
    def ziya_trigger(service_name: hug.types.text, func_name: hug.types.text, operate_type, *args):
        operate_type = operate_type.lower()

        t = trigger.ZiYaTrigger(service_name, func_name)

        if not args:
            return """
请输入不同操作对应的参数
delete: trigger_name
list: None
get: trigger_name
add:
    - oss: trigger_name, trigger_type, region, account_id, prefix, suffix, bucket
    - sls: trigger_name, trigger_type, region, account_id, log_project, source_log_store


            """

        trigger_name = args[0]

        if operate_type == "delete":
            result = t.delete_trigger(trigger_name)
            return result

        elif operate_type == "get":
            result = t.get_trigger(trigger_name)
            return result

        elif operate_type == "list":
            result = t.list_trigger()
            return result

        elif operate_type == "add":
            trigger_type = args[1].lower()
            region = args[2]
            account_id = args[3]

            if trigger_type == "oss":
                prefix = args[4]
                suffix = args[5]
                bucket = args[6]

                result = t.create_oss_trigger(trigger_name=trigger_name, region=region, account_id=account_id,
                                              prefix=prefix, suffix=suffix, bucket=bucket)
                return result

            elif trigger_type == "sls":
                log_project = args[4]
                trigger_store = args[5]
                record_store = args[6]

                result = t.create_sls_trigger(trigger_name=trigger_name, region=region, account_id=account_id,
                                              log_project=log_project, trigger_store=trigger_store, record_store=record_store)
                return result

            else:
                print("目前仅支持两种类型的trigger [sls/oss]")
                return

        elif operate_type == "update":
            print("目前暂不支持trigger更新")
            return

        else:
            print("目前支持的操作[delete/get/add]")
            return

    hug.API(__name__).cli()


if __name__ == "__main__":
    # hug.API(__name__).cli()
    main()
