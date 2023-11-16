# -*- encoding: utf-8 -*-
'''
@File    :   down_listing.py
@Time    :   2023/11/14 10:50:32
@Author  :   cep 
'''

from tools.duckdb_tools import * 
from tools.config import *

# 账号数据
# account_sheet = pd.read_excel("./Duckdb/account_data.xlsx")
# account_li = account_sheet['erp_id'].to_list()


path = "/mnt/c/Users/Administrator/Desktop/account_data.xlsx"
df_account = pd.read_excel(path)



df_account = df_account.loc[df_account['group_name']== '武汉16组']

account_li = df_account['erp_id'].to_list()
print(len(account_li))


task_list = []
for i in account_li:
    sql_cl = f"""
    WITH
        (select '2023-09-01') as begin_date,
        (select '2023-10-01') AS end_date,
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
    task_list.append(sql_cl)

# 下载函数
def fun(sql_x):
    con = Client(host=host_ck, port=9001, user=username_ck,
                 password=password_ck, database=database_ck, send_receive_timeout=3000)

    columns = ['account_id', 'seller_sku', 'sku', 'open_date','source_name']
    data = con.execute(sql_x)
    df = pd.DataFrame(data=data, columns=columns)
    # print(df)
    # 数据写入dk
    writeDuck(df) # 调用函数写入

def writeDuck(data):
    local_con  = Duck().con_duck().cursor()
    local_con.sql("insert into amazon_listing select * from data")
    local_con.close()  # 用完关闭
    

print(len(task_list))
# task_list = task_list[13943:]
g = DownData()
g.get_data_down(task_list=task_list, func_do=fun)
