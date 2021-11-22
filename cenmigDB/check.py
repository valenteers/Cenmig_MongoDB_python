#!/usr/bin/env python3  

#==================================== Load my packages =================================#

# from pymongo import MongoClient
from cenmigDB.import_modules import *

#==================================== Load library =====================================#

# import config

#==================================== connect function =====================================#

# def DBconnect(DBUrl = config.DBUrl, DBPort = config.DBPort, DBName = config.DBName, DBcollection = config.DBcollection ):
# 	'''
# 	DBconnect(str=config.DBUrl, int=config.DBPort, str=config.DBName, str=config.DBcollection)
# 	ใช้เปิดการเชื่อมต่อฐานข้อมูล mongoDB
# 	- ตัวแปรที่ใช้ :
# 	- DBUrl : url หรือที่อยู่ของฐานข้อมูลแบบ 'mongodb://localhost'
# 	- DBPort : เลข port ของโปรแกรม  mongoDB ค่า default : 27017
# 	- DBName : ชื่อดาต้าเบสที่ต้องการเชื่อมต่อ
# 	- DBcollection : ชื่อของ collection หรือตารางที่ต้องการใช้
# 	'''
# 	client = MongoClient(DBUrl, DBPort)
# 	db = client[DBName]
# 	dbConnect = db[DBcollection]
# 	return dbConnect

# def closeConnect(DBUrl = config.DBUrl, DBPort = config.DBPort):
# 	'''
# 	closeConnect(str=config.DBUrl, int=config.DBPort)
# 	ปิดการเชื่อมต่อ
# 	- ตัวแปรที่ใช้ :
# 	- DBUrl : url หรือที่อยู่ของฐานข้อมูลแบบ 'mongodb://localhost'
# 	- DBPort : เลข port ของโปรแกรม  mongoDB  ค่า default : 27017
# 	''' 
# 	MongoClient(DBUrl, DBPort).close

#==================================== End ========================================#