# -*- encoding: utf-8 -*-
'''
@File    :   down_product.py
@Time    :   2023/10/24 14:02:12
@Author  :   cep 
'''

from tools.config import * 
from tools.duckdb_tools import * 


con = Client(host=host_ck, port=9001, user=username_ck, password=password_ck, database=database_ck, send_receive_timeout=3000)

sql = f"""
SELECT a.spu, a.sku,a.dev_type_p, a.title_cn , a.end_time,  product_sx, product_status, a.line1,a.line2 ,a.line3, a.line4 FROM (
SELECT a.spu as spu ,
b.sku as sku ,
b.new_price,
b.t_sales,
a.title_cn as title_cn,
a.title_en as title_en,
if(a.end_time is null, toDateTime('0000-00-00 00:00:00') , a.end_time) as end_time,
d.path_name as path_name,
CASE
WHEN a.product_is_multi = 0 THEN '普通单品'
WHEN a.product_is_multi = 1 THEN '多属性单品'
WHEN a.product_is_multi = 3 THEN '捆绑'
WHEN a.product_is_multi = 4 THEN '组合'
END AS product_sx,
CASE
WHEN a.devp_type = 1 THEN '常规产品'
WHEN a.devp_type = 2 THEN '试卖产品'
WHEN a.devp_type = 3 THEN 'FBA精品'
WHEN a.devp_type = 4 THEN '代销产品'
ELSE 'other'
END AS dev_type_p,
CASE
WHEN b.resource_type = 1 THEN '正常'
WHEN b.resource_type = 2 THEN '停产'
WHEN b.resource_type = 3 THEN '缺货 '
WHEN b.resource_type = 3 THEN '停产找货中 '
ELSE '其他'
END AS resource_type,
CASE
WHEN a.product_status = 0 THEN '已创建'
WHEN a.product_status = 1 THEN '待修改'
WHEN a.product_status = 2 THEN '待修改'
WHEN a.product_status = 3 THEN '待买样'
WHEN a.product_status = 4 THEN '待品检'
WHEN a.product_status = 5 THEN '待编辑'
WHEN a.product_status = 6 THEN '待拍摄'
WHEN a.product_status = 7 THEN '待编辑待拍摄'
WHEN a.product_status = 8 THEN '待修图'
WHEN a.product_status = 9 THEN '在售中'
WHEN a.product_status = 10 THEN '审核不通过'
WHEN a.product_status = 11 THEN '停售'
WHEN a.product_status = 12 THEN '待清仓'
WHEN a.product_status = 13 THEN '已滞销'
WHEN a.product_status = 14 THEN '待物流审核'
WHEN a.product_status = 15 THEN '待关务审核'
WHEN a.product_status = 16 THEN '文案驳回到开发 '
WHEN a.product_status = 17 THEN '文案驳回品控'
WHEN a.product_status = 18 THEN '摄影驳回到开发'
WHEN a.product_status = 19 THEN '摄影驳回到品控'
WHEN a.product_status = 20 THEN '取消开发'
WHEN a.product_status = 21 THEN '侵权待审核'
WHEN a.product_status = 22 THEN '待终审'
WHEN a.product_status = 23 THEN 'ECN资料变更中'
WHEN a.product_status = 24 THEN 'ECN资料变更驳回'
END as product_status,
splitByChar('|', replaceAll(d.path_name,'>>', '|'))[1] as line1,
splitByChar('|', replaceAll(d.path_name,'>>', '|'))[2] as line2,
splitByChar('|', replaceAll(d.path_name,'>>', '|'))[3] as line3,
splitByChar('|', replaceAll(d.path_name,'>>', '|'))[4] as line4
FROM yibai_prod_base_sync.yibai_prod_spu a
LEFT JOIN yibai_prod_base_sync.yibai_prod_sku b
ON a.spu = b.spu
LEFT JOIN yb_datacenter.yb_product c
ON c.sku = b.sku
LEFT JOIN yb_datacenter.yb_product_linelist d
ON toInt64(d.id) = c.product_linelist_id ) a 
settings max_memory_usage = 40000000000
"""
# columns = ['spu','sku','dev_type','title_cn','end_time','product_sx','product_status','line1','line2','line3','line4']
# data = con.execute(sql)
# df_product = pd.DataFrame(data=data, columns=columns)
# print(df_product.info())


# l = DuckDbTools().con_duck().cursor()
# l.sql(
# """
# create table product (
# spu varchar,
# sku varchar,
# dev_type varchar,
# title_cn varchar,
# end_time datetime,
# product_sx varchar,
# product_status varchar,
# line1 varchar,
# line2 varchar,
# line3 varchar,
# line4 varchar
# )
# """
# )


# l.sql("insert into product select * from df_product")

# l.sql('from product').show()



# l.close()