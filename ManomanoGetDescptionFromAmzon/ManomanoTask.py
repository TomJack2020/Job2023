# -*- encoding: utf-8 -*-
'''
@File    :   down.py
@Time    :   2023/10/26 17:11:34
@Author  :   cep 
'''

from Job2023.tools.config import *
import pandas as pd
import os

engine25 = create_engine(loc_con)
engine3 = create_engine(con3)


def have_orders(task_path):
    # path1 = "/mnt/c/Users/Administrator/Desktop/test.xlsx"
    # task_path = "" 
    site_tuple = ("FR", 'FR')
    # 读取历史处理过的sku过滤处理
    task_sku = pd.read_excel(task_path)[['sku']]
    # 剔除掉已经处理过的sku
    # task_sku = task_sku.loc[~task_sku['sku'].isin(list(set(task_have['SKU'].values)))]
    task_sku['sku'] = task_sku['sku'].astype('str')
    print(task_sku)

    # 删除历史数据
    engine25.execute('truncate res_fr_data')
    # 读取账号FR站点数据  注意部分可能需要gb、us站点
    fr_account = pd.read_sql(f"SELECT id FROM yibai_system.yibai_amazon_account WHERE site in {site_tuple} AND status = 1", engine3)
    task_list = []
    for account_id_x in set(fr_account['id'].values):
        # # 法国站点销量排序数据
        sql = f"""
        SELECT a.account_id ,a.seller_sku,a.sku_sales, a.sku, b.asin1,b.item_name 
        FROM yibai_order.yibai_platform_sku_sales a
        LEFT JOIN yibai_product.yibai_amazon_listing_alls b
        ON a.account_id = b.account_id AND a.seller_sku = b.seller_sku
        WHERE a.account_id = {account_id_x} AND a.platform_code = 'AMAZON' AND b.account_id = {account_id_x}
        AND  b.add_delete not in ('1', 'del') and a.sku in {tuple(task_sku['sku'].to_list())} and b.sku in {tuple(task_sku['sku'].to_list())}
        """
        task_list.append((sql, engine3, 'res_fr_data'))


    def tread_connection_db(sql_x, engine_x, save_name):
        try:
            df = pd.read_sql(sql_x, engine_x)
            df['sku'] = df['sku'].astype('str')
            # 数据取出来 最终去重取值最大因为同一个sku会存在不同的店铺
            # 数据匹配对应的sku 取值交集
            m1 = pd.merge(df, task_sku, on=['sku'], how='inner')
            m1['write_time'] = datetime.datetime.now()

            # 数据去重保留第一个
            m1.to_sql(save_name, engine25, index=False, if_exists='append')
        except Exception as e:
            print(f"{sql_x}---Error:{e}")


    print(len(task_list))
    # task_list = task_list[390:]

    t0 = datetime.datetime.now()
    if __name__ == '__main__':
        start = time.time()
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        pbar = tqdm(task_list)
        for id in pbar:
            pbar.set_description("Processing %s" % id[2])
            # 创建线程并放入队列中
            t = threading.Thread(target=tread_connection_db, args=id)
            q.put(t)
            # 队列队满
            if q.qsize() == 4:
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


def have_no_order(task_path):
    site_tuple = ("FR", 'FR')
    # 读取历史处理过的sku过滤处理
    task_sku = pd.read_excel(task_path)[['sku']]
    # 剔除掉已经处理过的sku
    # task_sku = task_sku.loc[~task_sku['sku'].isin(list(set(task_have['SKU'].values)))]
    task_sku['sku'] = task_sku['sku'].astype('str')
    # 删除之前的数据表
    engine25.execute('truncate res_fr_data_no_order')
    print("history have delete")
    # 已经获取到的sku数据
    have_sale_sku_li = [i[0] for i in engine25.execute("select distinct sku from res_fr_data").fetchall()]

    # 需要拉取链接数据非出单信息
    sku_tuple = tuple(set(task_sku['sku'].values).difference(set(have_sale_sku_li)))  # 取差集
    task_list = []

    fr_account = pd.read_sql(f"SELECT id FROM yibai_system.yibai_amazon_account WHERE site in {site_tuple} AND status = 1", engine3)
    for account_id_x in set(fr_account['id'].values):
        # 仅仅asin数据
        sql = f"""
        SELECT sku, seller_sku , asin1 ,item_name ,account_id
        FROM yibai_product.yibai_amazon_listing_alls
        WHERE account_id = {account_id_x}
        AND add_delete not in ('1', 'del')  and sku in {sku_tuple}
        """

        task_list.append((sql, engine3, 'res_fr_data_no_order'))


    def tread_connection_db(sql_x, engine_x, save_name):
        try:
            df = pd.read_sql(sql_x, engine_x)

            # 数据取值交集
            df = df.loc[df['sku'].isin(list(sku_tuple))]
            if not df.empty:
                df.drop_duplicates(subset=['sku'], inplace=True)
                df.to_sql(save_name, engine25, index=False, if_exists='append')
            else:
                pass
        except Exception as e:
            print(f"{sql_x}---Error:{e}")


    print(len(task_list))
    # task_list = task_list[210:]

    t0 = datetime.datetime.now()
    if __name__ == '__main__':
        start = time.time()
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        pbar = tqdm(task_list)
        for id in pbar:
            pbar.set_description("Processing %s" % id[2])
            # 创建线程并放入队列中
            t = threading.Thread(target=tread_connection_db, args=id)
            q.put(t)
            # 队列队满
            if q.qsize() == 4:
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

