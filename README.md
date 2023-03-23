# AIOpsServing

AIOpsServing 是一个开源项目，专为算法为中心的研发、测试、评估和服务的轻量级流程提供支持。

## 设计目标

AIOpsServing 旨在实现以下目标：

1. 快速发布：支持算法模型在多个平台上发布，包括阿里云函数计算（FC）、大数据平台 ODPS、分布式平台 Ray 和容器 Docker。
2. 生命周期管理：负责算法迭代、归档、发布和回滚的管理。
3. 性能评估：提供离线和在线算法性能评估。
4. 模型复用：计划整合异常检测、行为画像等模型仓库。


## 安装与运行

要求 Python 版本 >= 3.7.10。

安装方法：

```commandline
# pip install alibaba-aiopsserving (future)
pip install git+https://github.com/alibaba/AIOpsServing.git
```

检查版本：

```commandline
ziya --version
```

## 配置文件 
- 初次使用时，可通过命令ziya info自动生成配置文件模版，配置文件路径为 /home/.ziya/ziya.conf。

配置文件模板包括以下部分：

```
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
```

## 配置字段说明
* odps：将模型部署至ODPS,需要配置对应的AK信息和 endpoint 信息。,参考[ODPS AK](https://help.aliyun.com/document_detail/183946.html) ,[endpoint信息](https://help.aliyun.com/document_detail/89754.html)
* fc：将模型部署至阿里云FC,需要配置对应的AK信息和 endpoint 信息。,参考[阿里云FC AK](https://help.aliyun.com/document_detail/295894.html), [endpoint信息](https://help.aliyun.com/document_detail/52984.html)
* oss：模型库配置,需要配置对应的AK信息和 endpoint 信息。,参考[阿里云OSS AK](https://help.aliyun.com/document_detail/93720.html), [endpoint信息](https://help.aliyun.com/document_detail/31837.html)
* log：将日志存储到阿里云 SLS 日志服务，需要配置对应的 AK 信息和 endpoint 信息,参考[阿里云SLS AK](https://help.aliyun.com/document_detail/175967.html), [endpoint信息](https://help.aliyun.com/document_detail/29008.html)


## 功能调用

### ziya init

init 命令用于为模型的封装做准备。使用 ziya init 接口会在当前目录生成 commit_init.py 文件，用户可以根据提示在该文件中引用自己的模型。

示例：初始化 my_first_model_v1 模型，需要指定模型的名称和版本号。

```commandline
ziya init my_first_model_v1
```

### ziya commit

commit 命令用于将模型提交到 OSS 模型仓库。使用此接口需要在配置文件中配置 OSS 模型库的相关信息，请参考配置文件说明。

示例：将 my_first_model_v1 模型提交到 OSS 模型库。

```
ziya commit my_first_model_v1
```

### ziya deploy

deploy 命令用于将模型部署到指定的计算后端。目前支持的计算后端包括 Ray、阿里云 FC 和 Docker。使用阿里云 FC 时需要在配置文件中配置阿里云 FC 的相关信息。

示例：将 OSS 模型库中的模型部署到阿里云 FC 后端。

```
ziya deploy --backend 'fc' --model_name_version "my_first_model_v1"
```

### ziya info

info 命令根据配置文件展示当前模型库、数据集和支持的计算后端等信息。

示例：展示当前模型和数据集信息。使用 --force_update 参数查询最新信息，不指定则返回上一次查询信息。

```
ziya info --force_update
```

### ziya run 
run 命令支持用户调用已部署的模型。用户可以使用本地文件或 OSS 文件调用已部署在指定计算后端的模型。

示例：通过本地数据（JSON 格式）调用已部署在 FC 的模型。数据输入支持 OSS 数据和本地数据。

```
ziya run --backend "fc" --model_name_version "my_first_model_v1" --file_type "local" --file_name "input_data.json"
```

