# -*- encoding: utf-8 -*-
'''
@File    :   main_test.py
@Time    :   2023/11/16 21:35:39
@Author  :   cep 
'''

from tools.duckdb_tools import *

# 初始化原表
c = Amazon(queue_num=5, save_name='amazon_listing')

# print(c.get_amazon_listing_task()[0])

l = c.con_duck().cursor()

l.sql("truncate amazon_listing")

c.get_amazon_listing()



l.sql("select count(1) from amazon_listing").show()




