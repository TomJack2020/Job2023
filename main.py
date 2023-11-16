# -*- encoding: utf-8 -*-
'''
@File    :   main.py
@Time    :   2023/11/14 11:06:30
@Author  :   cep 
'''

from tools.config import *
from tools.duckdb_tools import * 

# # duck db 创建数据表
c = Duck().con_duck().cursor()

# c.execute("drop table amazon_listing")

# 在线listing表数据创建
# create_listing = f"""
# create table amazon_listing (
#     account_id INTEGER, 
#     seller_sku varchar , 
#     sku varchar,
#     open_date datetime, 
#     source_name varchar
#     )
# """
# c.sql(create_listing)


def save_account():
    df_account = pd.read_excel(r"/mnt/c/Users/Administrator/Desktop/account_data.xlsx")
    # print(df_account)
    return df_account

# ac = save_account()
# insert_acccount = f"""create table account as select * from ac"""
# c.sql(insert_acccount)

# 
c.sql("from account").show()

# 查询武汉16组后天刊登明细
# sql_check = f"""select  * from amazon_listing """
# c.sql(sql_check).show()



# 查询9月刊登的链接有无YXH
# sql_yxh = f"""
# select * from amazon_lisitng where sku in (select sku from sku_spu)
# """
# c.sql(sql_yxh).show()

# c.sql("select count(1) from amazon_listing").show()
# sql = """CREATE TABLE amazon_listing(
# account_id INTEGER, 
# seller_sku varchar,
# sku varchar,
# open_date datetime
# );
# """
# c.execute(sql)

c.sql("from amazon_listing where source_name = '后台刊登'").show()

# c.sql("show tables").show()



res = c.sql("from amazon_listing where source_name = '后台刊登'").df()

res.to_excel(r"/mnt/c/Users/Administrator/Desktop/wh16.xlsx",index=False)


c.close()


