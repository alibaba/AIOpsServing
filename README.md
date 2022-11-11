# AIOpsServing

Open source code for AIOpsServing

## 设计目标

以算法为中心的研发、测试、评估、服务的轻量级流程支持工具。

## 功能特性

快速发布：支持算法模型的多平台发布，默认支持阿里云函数计算FC，大数据平台ODPS，以及分布式平台Ray，容器Docker

生命周期：管理算法迭代,归档、发布、回滚

性能评估：提供算法离线和在线的性能评估

模型复用：计划整合异常检测、行为画像等模型仓库


## 安装运行
```commandline
# require python >= 3.7.10

# pip install alibaba-aiopsserving (future)
pip install git+https://github.com/alibaba/AIOpsServing.git

ziya --version
```

## 配置文件 
- 初次使用时，可通过命令ziya info自动生成配置文件模版，配置文件路径/home/.ziya/ziya.conf。
- 配置文件模版
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

* 字段说明
    * odps：将模型部署至ODPS,需要配置对应的AK信息,参考[ODPS AK](https://help.aliyun.com/document_detail/183946.html) ,[endpoint信息](https://help.aliyun.com/document_detail/89754.html)
    * fc：将模型部署至阿里云FC,需要配置对应的AK信息,参考[阿里云FC AK](https://help.aliyun.com/document_detail/295894.html), [endpoint信息](https://help.aliyun.com/document_detail/52984.html)
    * oss：模型库配置,需要配置对应的AK信息,参考[阿里云OSS AK](https://help.aliyun.com/document_detail/93720.html), [endpoint信息](https://help.aliyun.com/document_detail/31837.html)
    * log：将log日志存储至阿里云sls日志服务,参考[阿里云SLS AK](https://help.aliyun.com/document_detail/175967.html), [endpoint信息](https://help.aliyun.com/document_detail/29008.html)


## 功能调用
### ziya init
- init 命令主要是为模型的封装做准备,使用 ziya init 接口会在当前目录生成commit_init.py 文件,用户可以根据提示在该文件中引用自己的模型。
- 示例,初始化my_first_model_v1 模型,需要指定模型的名称以及版本号
```commandline
ziya init my_first_model_v1
```

### ziya commit
- commit 命令是将模型提交到OSS模型仓库,使用此接口需要在子牙配置文件中配置关于OSS模型库的信息,参考子牙配置文件说明。
- 示例,提交my_first_model_v1 模型至OSS模型库
```
ziya commit my_first_model_v1
```

### ziya deploy
- deploy 命令是将模型部署到指定的计算后端,目前子牙支持的计算后端包括[ray、阿里云FC、docker],在使用阿里云FC时需要在子牙配置文件中配置关于阿里云FC的信息。
- 示例, 将OSS模型库中的模型部署到指定后端阿里云FC上
```
ziya deploy --backend 'fc' --model_name_version "my_first_model_v1"
```

### ziya info
- info 命令是根据子牙配置文件展示当前模型库、数据集、支持计算后端等信息。
- 示例，展示目前模型和数据集信息,可通过参数 --force_update 查询最新信息,不指定则返回上一次查询信息
```
ziya info --force_update
```

### ziya run 
- run 命令支持用户调用已部署的模型,用户可使用本地文件/OSS文件调用已部署在指定计算后端的模型。
- 示例, 通过本地数据调用已部署在FC的模型,数据输入支持OSS数据,本地数据（json格式）
```
ziya run --backend "fc" --model_name_version "my_first_model_v1" --file_type "local" --file_name "input_data.json"
```

