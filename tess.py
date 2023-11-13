# -*- encoding: utf-8 -*-
'''
@File    :   tess.py
@Time    :   2023/11/07 16:08:09
@Author  :   cep 
'''
from tools.config import * 
import pandas as pd

start_date = "2023-11-02 00:00:00"
num = 32
# 刊登失败任务  拉取前10天的数据存量
sql_publish_fail_list = []
for i in range(2):
    # 时间倒推注意顺序
    dt1 = datetime.datetime.strptime(start_date, '%Y-%m-%d 00:00:00') + datetime.timedelta(days=-i - 1)
    dt2 = datetime.datetime.strptime(start_date, '%Y-%m-%d 00:00:00') + datetime.timedelta(days=-i)
    sql = f"""
    SELECT DISTINCT a.publish_id , DATE_FORMAT(FROM_UNIXTIME(a.last_review_time), '%%Y-%%m-%%d') as syn_time, b.error_code ,
        CASE
        WHEN a.publish_type = 2 THEN '手动刊登'
        ELSE '智能刊登'
        END AS source_publish
        FROM (select publish_id,last_review_time, publish_type from yibai_sale_center_amazon.yibai_amazon_publish_ready_failed 
        where `type` = 2 and last_review_time BETWEEN UNIX_TIMESTAMP('{str(dt1)}') AND UNIX_TIMESTAMP('{str(dt2)}') ) a
        LEFT JOIN yibai_sale_center_amazon.yibai_amazon_publish_fail_message_list b
        ON a.publish_id = b.publish_id 
        WHERE b.error_code IS NOT NULL
    """
    print(dt1)

    df = pd.read_sql(sql, create_engine(con12))

    print(df)

