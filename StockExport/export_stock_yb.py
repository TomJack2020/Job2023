# -*- encoding: utf-8 -*-
'''
@File    :   export_stock_yb.py
@Time    :   2023/10/30 09:27:59
@Author  :   cep 
'''



import sys
import os
from tools.config import * 

def get_stock(warehouse_id_tuple, n):

    
    sql = f"""
    SELECT a.sku,
    a.stock as `实际库存`,
    a.available_stock  as `可用库存`,
    a.purchase_on_way_count as `在途库存`,
    case   
    when toInt32(a.warehouse_id) = 340 then '谷仓法国仓'
    when toInt32(a.warehouse_id) = 325 then '谷仓捷克仓'
    when toInt32(a.warehouse_id) = 88 then '谷仓英国仓'
    when toInt32(a.warehouse_id) = 50 then '谷仓美国东仓'
    when toInt32(a.warehouse_id) = 818 then '谷仓西班牙仓'
    when toInt32(warehouse_id) = 478 then '小包仓_虎门'
    when toInt32(warehouse_id) = 653 then '谷仓德国仓'
    when toInt32(warehouse_id) = 35 then '万邑通德国仓-DE Warehouse'
    when toInt32(warehouse_id) = 481 then '海外虚拟仓_虎门'
    end as ss
    FROM yb_datacenter.yb_stock a
    WHERE warehouse_id in{warehouse_id_tuple} AND sync_time > toDate(now()) AND a.stock >= {n} limit 10
    """
    con = Client(host=host_ck, port=9001, user=username_ck, password=password_ck,database=database_ck,send_receive_timeout=3000)
    data = con.execute(sql)
    # 读取当天库存数据
    df = pd.DataFrame(data, columns=['sku','实际库存', '可用库存', '在途库存', '仓库名字'])
    return df


# """
# 478 小包仓虎门
# 340 谷仓法国仓
# 325 谷仓捷克仓
# 653 谷仓德国仓
#  35 万邑通德国仓
#  88 谷仓英国仓
# """


create_date = str(datetime.datetime.now())[:10]
# 肖玉聘---库存数据  平台[mano]
"""德国/法国/英国/小包虎门仓"""
df = get_stock((340,325,478,653,35, 88), 1)
print(df)

# # 数据存储到本地
# # df.to_excel(fr'C:\\Users\\Administrator\\Desktop\\{create_date}_stock.xlsx',index=False, engine='xlsxwriter')




