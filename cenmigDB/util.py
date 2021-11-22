#!/usr/bin/env python3 
#==================================== Load my packages =================================#

from import_modules import *
# import setting
# import shortcut
# import text as txt
# import ast

#==================================== Load library =====================================#

# from pymongo import MongoClient
# import os
# import time
# import pandas as pd 

#==================================== Util function ==========================================#

#------------------------------------ MongoDB function -----------------------------------------#

def connect_database(url = setting.database_url, port = setting.database_port, name = setting.database_name, collection = setting.database_collection ):
    '''
    ใช้เปิดการเชื่อมต่อฐานข้อมูล mongoDB ตัวแปรที่ใช้ :
    - url : url หรือที่อยู่ของฐานข้อมูลเช่น 'mongodb://localhost'
    - port : เลข port ของโปรแกรม  mongoDB ค่า default คือ 27017
    - name : ชื่อดาต้าเบสที่ต้องการเชื่อมต่อ
    - collection : ชื่อของ collection หรือตารางที่ต้องการใช้ 
    '''
    print('Connecting to ' + str(name) + ' Collection ' + str(collection))
    client = MongoClient(url, port)
    database = client[name]
    dbCollection = database[collection]
    print('Connecting successfully')
    return dbCollection

def close_connect(url = setting.database_url, port = setting.database_port):
    '''
    ปิดการเชื่อมต่อ ตัวแปรที่ใช้ :
    - url : url หรือที่อยู่ของฐานข้อมูลเช่น 'mongodb://localhost'
    - port : เลข port ของโปรแกรม  mongoDB ค่า default : 27017
    ''' 
    MongoClient(url, port).close

#------------------------------------ search function -----------------------------------------#

def query(query_file):
    dbCollection = connect_database()
    data = txt_to_dict(query_file)
    result = dbCollection.aggregate(data) # Pipeline with query dict
    data = pd.DataFrame.from_dict(result)
    print('Records found : ' + str(len(data)))
    print(data.head())
    return data

def txt_to_dict(txt_file):
    # READ txt and convert to dict for query
    file = open(txt_file, "r")
    contents = file.read()
    dictionary = ast.literal_eval(contents)
    return dictionary
#==================================== End Text ===========================================#
