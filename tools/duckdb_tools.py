# -*- encoding: utf-8 -*-
'''
@File    :   duckdb_tools.py
@Time    :   2023/11/03 10:45:54
@Author  :   cep 
'''

import duckdb

from .config import *
import logging
import threading,queue
from tqdm import tqdm


# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
#                     level=logging.DEBUG)
class Duck(object):
    def __init__(self, queue_num):
        self.name = 'duck'
        # self.db_path = "/mnt/c/Users/Administrator/Desktop/file.db" 
        self.db_path = "/mnt/d/DuckdbTemplate/amazon_rate.db"
        self.queue_num = queue_num
        self.account_path =  "/mnt/c/Users/Administrator/Desktop/account_data.xlsx"
        # 初始化日期时间
        self.start_date = '2023-09-01'
        self.end_date = '2023-10-01'
        self.save_name = 'amazon_listing_test'



    def con_duck(self):
        c = duckdb.connect(database=self.db_path, read_only=False)
        return c
    
    def down_func(self,item):
        con = Client(host=host_ck, port=9001, user=username_ck, password=password_ck, database=database_ck, send_receive_timeout=3000)
        data = con.execute(item[0])
        df = pd.DataFrame(data=data, columns=item[1])
        # print(df)
        # 数据写入dk
        self.writeDuck(df)  # 调用函数写入

    def writeDuck(self, data):
        result_df = data.copy()
        local_con = self.con_duck().cursor()
        local_con.sql(f"insert into {self.save_name} select * from result_df")
        local_con.close()  # 用完关闭


    def thread_func(self, task_list):
        
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        list_bar = tqdm(task_list)
        for tid in list_bar:
            # list_bar.set_description("Processing")
            # 创建线程并放入队列中
            t = threading.Thread(target=self.down_func, args=(tid,))
            q.put(t)
            # 队列队满
            if q.qsize() == self.queue_num:
                # 用于记录线程，便于终止线程
                join_thread = []
                # 从对列取出线程并开始线程，直到队列为空
                while not q.empty():
                    t = q.get()
                    join_thread.append(t)
                    t.start()
                # 终止上一次队满时里面的所有线程
                for t in join_thread:
                    t.join()


class Amazon(Duck):
    def __init__(self, queue_num, save_name):
        super(Amazon, self).__init__(queue_num)
        self.save_name = save_name


    # 下载链接任务构建
    def get_amazon_listing_task(self):

        df_account = pd.read_excel(self.account_path)
        df_account = df_account.loc[df_account['group_name']== '武汉16组']

        account_li = df_account['erp_id'].to_list()
        print(len(account_li))
        
        task_list = []
        for i in account_li:
            sql_cl = f"""
            WITH
                (select '{self.start_date }') as begin_date,
                (select '{self.end_date}') AS end_date,
                (select {i} ) as ac_id,
                source_list as (
                select account_id, seller_sku, source_name
                from yibai_product_kd_sync.yibai_amazon_listing_source
                where source_name is not null and sync_time >'2023-04-01'
                and account_id = ac_id
                ),
                -- 匹配捆绑表
                map_data as (select account_id ,seller_sku , sku from yibai_product_kd_sync.yibai_amazon_sku_map where account_id = ac_id ),
                listing as (
                select account_id, seller_sku, sku, toDateTime(open_date) as open_date
                from yibai_product_kd_sync.yibai_amazon_listing_alls
                where open_date >= begin_date and open_date < end_date and fulfillment_channel = 'DEF'
                AND status = 1 AND add_delete != 'del'
                and account_id = ac_id
                )
                select a.account_id, a.seller_sku, c.sku,a.open_date, b.source_name from listing a
                left join source_list b on a.account_id = b.account_id and a.seller_sku = b.seller_sku
                left join map_data c on a.account_id = c.account_id and a.seller_sku = c.seller_sku
                settings max_memory_usage = 40000000000
            """
            columns = ['account_id', 'seller_sku', 'sku', 'open_date', 'source_name']
            task_list.append((sql_cl, columns))

        return task_list

    # 下载链接表
    def get_amazon_listing(self):
        # 获取listing的下载任务
        task_amazon_down_list = self.get_amazon_listing_task()
        # 抓取数据列名
        self.columns = ['account_id', 'seller_sku', 'sku', 'open_date', 'source_name']
        # 调用函数下载订单
        self.thread_func(task_amazon_down_list)





    



  

