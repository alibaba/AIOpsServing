import sqlite3
import time
import threading


class CacheData:
    """
    提供缓存机制, 使用sqlite3 作为缓存， 在安装 ziya-api 时初始化数据库
    缓存的信息
    1、子牙 run status 信息
    2、子牙 benchmark 信息
    """
    _single_lock = threading.Lock()

    def __init__(self):
        # 创建表信息
        self.create_db()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with cls._single_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(CacheData, cls).__new__(cls)
        return cls._instance

    def get_conn(self):
        # 数据库暂时存放在当前目录
        con = sqlite3.connect("./ziya.db")
        return con

    def get_now_time(self):
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return now_time

    def create_db(self):
        """
        1、创建数据库 ziya.db
        2、创建四张表
            - model_info: 模型信息&计算后端信息缓存表
                - id, time，content
            - benchmark_all_model_info: 所有模型做 benchmark 结果缓存表
                - id, time，datasets, backend, content
            - benchmark_all_data_info: 所有datasets 做 benchmark 结果缓存表
                - id, time, model_name, backend, content
            - benchmark_all_backend_info: 所有backend 做 benchmark 结果缓存表
                - id, time, model_name, datasets, content
            - benchmark_no_variables_info: 指定具体的后端、模型、数据做benchmark 缓存表
                - id, time, model_name, datasets, backend, content
        :return:
        """

        create_table_model = "create table if not exists model_info (id integer primary key, time DATA, content TEXT)"
        create_all_model_table_benchmark = "create table if not exists benchmark_all_model_info (id integer primary key, time DATA, datasets TEXT, backend TEXT, content TEXT)"
        create_all_data_table_benchmark = "create table if not exists benchmark_all_datasets_info (id integer primary key, time DATA, model_name TEXT, backend TEXT, content TEXT)"
        create_all_backend_table_benchmark = "create table if not exists benchmark_all_backend_info (id integer primary key, time DATA, model_name TEXT, datasets TEXT, content TEXT)"
        create_no_variables_table_benchmark = "create table if not exists benchmark_no_variables_info (id integer primary key, time DATA, model_name TEXT, datasets TEXT, backend TEXT, content TEXT)"

        con = self.get_conn()
        con.execute(create_table_model)
        con.execute(create_all_model_table_benchmark)
        con.execute(create_all_data_table_benchmark)
        con.execute(create_all_backend_table_benchmark)
        con.execute(create_no_variables_table_benchmark)
        con.close()

    # 将内容存入缓存
    def cache_info(self, insert_sql):
        con = self.get_conn()
        cur = con.cursor()
        try:
            cur.execute(insert_sql)
            con.commit()
            cur.close()
            con.close()
        except Exception as e:
            con.rollback()

    # 从缓存中获取最新记录
    def read_cached_info(self, select_sql):
        con = self.get_conn()
        cur = con.cursor()
        try:
            cur.execute(select_sql)
            data = cur.fetchall()
            cur.close()
            con.close()
            # 当前可能没有缓存
            if data:
                return data[0][-1]
            else:
                return
        except Exception as e:
            print(e)

