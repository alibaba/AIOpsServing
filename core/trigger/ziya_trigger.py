import sys
from core.utils import generate_client

class ZiYaTrigger:
    def __init__(self, service_name, func_name):
        try:
            self.client = generate_client.generate_fc_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        self.service_name = service_name
        self.func_name = func_name

    def list_trigger(self):
        trigger_list = self.client.list_triggers(self.service_name, self.func_name)
        return trigger_list.data

    def delete_trigger(self, trigger_name):
        self.client.delete_trigger(self.service_name, self.func_name, trigger_name)

    # TODO 目前的event 指定是bucket中有新内容增加，有需要再修改
    # TODO source_arn， invocation_role 是否需要直接通过配置文件给出
    def create_oss_trigger(self, trigger_name, region, account_id, prefix, suffix, bucket):
        triggers = self.list_trigger()
        trigger_names = [t["triggerName"] for t in triggers["triggers"]]
        if trigger_name in trigger_names:
            return f"trigger {trigger_name} already exist"

        trigger_config = {
            'events': ['oss:ObjectCreated:*'],
            'filter': {
                'key': {
                    'prefix': prefix,
                    'suffix': suffix
                }
            }
        }

        source_arn = 'acs:oss:{0}:{1}:{2}'.format(region, account_id, bucket)
        invocation_role = f'acs:ram::{account_id}:role/{self.service_name}-{self.func_name}'
        description = f'create oss trigger [{trigger_name}]'
        try:
            print(self.service_name, self.func_name, trigger_name, 'oss', trigger_config, source_arn, invocation_role)
            trigger_response = self.client.create_trigger(self.service_name, self.func_name, trigger_name, 'oss',
                                                          trigger_config, source_arn, invocation_role,
                                                          description=description)

            return trigger_response.data
        except Exception as e:
            return e

    def create_sls_trigger(self, trigger_name, region, account_id, log_project, trigger_store, record_store):
        triggers = self.list_trigger()
        trigger_names = [t["triggerName"] for t in triggers["triggers"]]
        if trigger_name in trigger_names:
            return f"trigger {trigger_name} already exist"
        source_arn = 'acs:log:{0}:{1}:project/{2}'.format(region, account_id, log_project)
        trigger_config = {
            'sourceConfig': {
                'logstore': trigger_store
            },
            'jobConfig': {
                'triggerInterval': 60,
                'maxRetryTime': 10
            },
            'functionParameter': {},
            'logConfig': {
                'project': log_project,
                'logstore': record_store
            },
            'enable': False
        }
        invocation_role = f'acs:ram::{account_id}:role/{self.service_name}-{self.func_name}'
        description = f'create sls trigger [{trigger_name}]'
        try:
            trigger_response = self.client.create_trigger(self.service_name, self.func_name, trigger_name, "log",
                                                          trigger_config, source_arn, invocation_role,
                                                          description=description)
            return trigger_response.data
        except Exception as e:
            return e

    def update_trigger(self, trigger_type, ):
        # TODO
        pass

    def get_trigger(self, trigger_name):
        trigger = self.client.get_trigger(self.service_name, self.func_name, trigger_name)
        return trigger.data
