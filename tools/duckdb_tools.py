# -*- encoding: utf-8 -*-
'''
@File    :   duckdb_tools.py
@Time    :   2023/11/03 10:45:54
@Author  :   cep 
'''
import pandas as pd
import duckdb
import logging
import threading,queue
from tqdm import tqdm


# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
#                     level=logging.DEBUG)
class Duck(object):
    def __init__(self) -> None:
        self.name = 'duck'
        # self.db_path = "/mnt/c/Users/Administrator/Desktop/file.db" 
        self.db_path = "/mnt/d/DuckdbTemplate/amazon_rate.db" 
        self.queue_num = 5
    

    def con_duck(self):
        c = duckdb.connect(database=self.db_path, read_only=False)
        return c

    def thread_func(self, task_list, func_do):
        
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        list_bar = tqdm(task_list)
        for tid in list_bar:
            # list_bar.set_description("Processing")
            # 创建线程并放入队列中
            t = threading.Thread(target=func_do, args=(tid,))
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






class DownData(Duck):
    def __init__(self) -> None:
        super().__init__()

    def get_data_down(self, task_list, func_do):
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        list_bar = tqdm(task_list)
        for tid in list_bar:
            # list_bar.set_description("Processing")
            # 创建线程并放入队列中
            t = threading.Thread(target=func_do, args=(tid,))
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
    def __init__(self) -> None:
        super().__init__()
        






    



    # c = duckdb.connect("/mnt/c/Users/Administrator/Desktop/file.db")
    # try:
    #     c.sql('CREATE TABLE test(i INTEGER)')
    # except Exception as e:
    #     print('error')
    #     logging.error(e)

    # c.sql('show tables').show()
    # # logging.info('info 信息')
    # # logging.warning('warning 信息')
    # # logging.error('error 信息')
    # # logging.critical('critial 信息')

    # c.close()



