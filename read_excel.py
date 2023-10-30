# -*- encoding: utf-8 -*-
'''
@File    :   read_excel.py
@Time    :   2023/10/26 18:26:41
@Author  :   cep 
'''

import pandas as pd
import os
folder = "/mnt/c/Users/Administrator/Desktop/"

path = os.path.join(folder, "xx.xlsx")


df = pd.read_excel(path)
print(df)


