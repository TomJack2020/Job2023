# -*- encoding: utf-8 -*-
'''
@File    :   main_test.py
@Time    :   2023/11/16 21:35:39
@Author  :   cep 
'''

from tools.duckdb_tools import *









# 下载订单表

# 初始化原表
c = Amazon()
l = c.con_duck().cursor()
# c.save_name = 'amazon_listing'
# c.start_date = '2023-10-01'
# c.end_date = '2023-11-01'


# l.sql("truncate amazon_listing")




c.get_amazon_listing()


# 数据查询
l.sql("from amazon_listing").show()






