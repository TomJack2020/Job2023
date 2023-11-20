# -*- coding: utf-8 -*- 
# @Time : 2023/11/17 21:55 
# @Author : cep 
# @File : seller_tools.py




import requests

url = "https://api.sellersprite.com/v1/keyword/miner"


headers = {
    'secret-key':'afcb9a792d974c2d9842aebcbc5d6eb3',
    'Content-Type':'application/json;charset=utf-8'
}

param = {
    'keyword':'tools'

}

res = requests.post(url=url, params=param, headers=headers)


print(res.text)