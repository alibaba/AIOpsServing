import pandas as pd
import sys
from core.utils import generate_client


class DataFetch:

    def __init__(self):

        try:
            o = generate_client.generate_odps_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        # generate_client.check_conf_exists()
        self.o = o


    def _download_data_sql(self, tmp_sql):
        instance = self.o.execute_sql(tmp_sql)
        with instance.open_reader(tunnel=True) as reader:
            pd_df = reader.to_result_frame()
        tmp_df = pd.DataFrame(pd_df)
        print("df shape by odps is...", tmp_df.shape)
        return tmp_df

    def _download_data_table(self, table_name, partition):
        t = self.o.get_table(table_name)
        with t.open_reader(partition=partition) as reader:
            pd_df = reader.to_pandas()
        tmp_df = pd.DataFrame(pd_df)
        print("df shape by odps is...", tmp_df.shape)
        return tmp_df

    def download_data_by_sql_to_local(self, tmp_sql, file_path):
        odps_df = self._download_data_sql(tmp_sql)
        suffix = file_path.split(".")[-1]
        if suffix == "csv":
            odps_df.to_csv(file_path)
        elif suffix == "pkl":
            odps_df.to_pickle(file_path)
        elif suffix == "xlsx":
            odps_df.to_excel(file_path)
        else:
            print("文件仅支持 csv、xlsx、pkl格式")
            return

    def download_data_by_table_to_local(self, table_name, file_path, partition=None):
        odps_df = self._download_data_table(table_name, partition)
        suffix = file_path.split(".")[-1]
        if suffix == "csv":
            odps_df.to_csv(file_path)
        elif suffix == "pkl":
            odps_df.to_pickle(file_path)
        elif suffix == "xlsx":
            odps_df.to_excel(file_path)
        else:
            print("文件仅支持 csv、xlsx、pkl格式")
            return
        pass

    def download_data_by_sql_to_oss(self, tmp_sql, oss_name):
        odps_df = self._download_data_sql(tmp_sql)
        try:
            oss_client = generate_client.generate_oss_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        try:
            oss_client.put_df_to_oss(oss_name, odps_df)
            return
        except Exception as e:
            print(e)

    def download_data_by_table_to_oss(self, table_name, oss_name, partition=None):
        odps_df = self._download_data_table(table_name, partition)
        try:
            oss_client = generate_client.generate_oss_client()
        except Exception as e:
            print(e)
            sys.exit(1)
        try:
            oss_client.put_df_to_oss(oss_name, odps_df)
            return
        except Exception as e:
            print(e)


if __name__ == '__main__':
    pass
