#!/usr/bin/env python3  

#==================================== Load my packages =================================#

from packages.mongoDB import connect
from packages.mongoDB import save
# import text as txt
import config

#==================================== Load library =====================================#

# from pymongo import MongoClient
# from shutil import copy
# from tqdm import tqdm 

import pandas as pd 
import json
import os

#==================================== update function =====================================#

def noReplace(pathFile, indexColumn = config.indexColumn):
	'''
	noReplace(str, str=config.indexColumn) 
	ถ้า CSV มีข้อมูลที่ index ซ้ำกับในดาต้าเบส CSV ข้อมูลในบรรทัดนั้นจะไม่ถูกนำเข้าไปในดาต้าเบส
	- ตัวแปรที่ใช้ :
	- path : พาธไฟล์ CSV ที่ต้องการนำข้อมูลเข้า mongoDB
	- indexColumn : ชื่อคอลัมภ์ที่ใช้เป็น index ในการตรวจสอบความซ้ำ
	'''
	getData = connect.DBconnect.distinct(indexColumn)
	dataCSV = pd.read_csv(pathFile, low_memory=False) 
	csvCheck = list(dataCSV[indexColumn])
	for x in csvCheck:
		if x in getData:
			dataCSV.drop(dataCSV.loc[dataCSV[indexColumn] == x].index, inplace=True)
		else:
			pass      
	importData = json.loads(dataCSV.to_json(orient='records'))
	connect.DBconnect.insert(importData)
	listToCopy = list(dataCSV[indexColumn])
	save.copyFile(pathFile,config.pathFileDB,listToCopy)
	connect.closeConnect()

def replace(pathFile, indexColumn = config.indexColumn): 
	'''
	replace(str, str=config.indexColumn) 
	ถ้า CSV มีข้อมูลที่ index ซ้ำกับในดาต้าเบส CSV จะลบข้อมูลเดิมในดาต้าเบสก่อนนำข้อมูลจาก CSV เข้า
	- ตัวแปรที่ใช้ :
	- path : พาธไฟล์ CSV ที่ต้องการนำข้อมูลเข้า mongoDB
	- indexColumn : ชื่อคอลัมภ์ที่ใช้เป็น index ในการตรวจสอบความซ้ำ
	'''
	getData = connect.DBconnect.distinct(indexColumn)
	dataCSV = pd.read_csv(pathFile, low_memory=False) 
	csvCheck = list(dataCSV[indexColumn])
	for x in csvCheck:
		if x in getData:
			connect.DBconnect.delete_many({indexColumn : x})
		else:
			pass
	importData = json.loads(dataCSV.to_json(orient='records'))
	connect.DBconnect.insert(importData)
	listToCopy = list(dataCSV[indexColumn])
	save.copyFile(pathFile,config.pathFileDB,listToCopy)      
	connect.closeConnect()

def new(path, indexColumn = config.indexColumn):
	'''
	new(str, str=config.indexColumn) 
	นำข้อมูลใน CSV เข้าทั้งหมด ไม่มีการตรวจเช็ค เหมาะไว้ใช้ตอนนำเข้าข้อมูลครั้งแรกเท่านั้น
	- ตัวแปรที่ใช้ :
	- path : พาธไฟล์ CSV ที่ต้องการนำข้อมูลเข้า mongoDB
	- indexColumn : ชื่อคอลัมภ์ที่ใช้เป็น index ในการตรวจสอบความซ้ำ
	'''
	dataCSV = pd.read_csv(path, low_memory=False)
	importData = json.loads(dataCSV.to_json(orient='records'))
	connect.DBconnect.insert(importData)
	listToCopy = list(dataCSV[indexColumn])
	save.copyFile(path,config.pathFileDB,listToCopy) 
	connect.closeConnect()

#==================================== End ========================================#