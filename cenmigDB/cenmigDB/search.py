#!/usr/bin/env python3  

from cenmigDB import text as txt
from cenmigDB import config

from pymongo import MongoClient
from tqdm import tqdm 
from shutil import copy

import pandas as pd 
import os


class MongoDB:

	def __init__(self, DBUrl, DBPort, DBName, DBcollection):
		__DBUrl = DBUrl
		__DBPort = DBPort
		__DBName = DBName
		__DBcollection = DBcollection

	def DBconnect(self):
		client = MongoClient(self.__DBUrl, self.__DBPort)
		db = client[self.__DBName]
		dbConnect = db[self.__DBcollection]
		return dbConnect

	# def import_noreplace(self, path):
	# 	dbConnect = MongoDB_method.DBconnect()
	# 	getData = dbConnect.distinct(indexColumn)
	# 	dataCSV = pd.read_csv(path, low_memory=False) 
	# 	csvCheck = dataCSV[indexColumn]
	# 	csvCheck =  list(csvCheck)
	# 	for x in csvCheck:
	# 		if x in getData:
	# 			dataCSV.drop(dataCSV.loc[dataCSV[indexColumn] == x].index, inplace=True)
	# 		else:
	# 			pass      
	# 	importData = json.loads(dataCSV.to_json(orient='records'))
	# 	dbConnect.insert(importData)
	# 	listToCopy = dataCSV[indexColumn]
	# 	listToCopy =  list(listToCopy)
	# 	copyFile(path,pathFileDB,listToCopy)
	# 	MongoClient(databaseUrl, databasePort).close

	# def import_replace(self, path): 
	# 	dbConnect = MongoDB_method.DBconnect()
	# 	getData = dbConnect.distinct(indexColumn)
	# 	dataCSV = pd.read_csv(path, low_memory=False) 
	# 	csvCheck = dataCSV[indexColumn]
	# 	csvCheck =  list(csvCheck)
	# 	for x in csvCheck:
	# 		if x in getData:
	# 			dbConnect.delete_many({indexColumn : x})
	# 		else:
	# 			pass
	# 	importData = json.loads(dataCSV.to_json(orient='records'))
	# 	dbConnect.insert(importData)
	# 	listToCopy = dataCSV[indexColumn]
	# 	listToCopy =  list(listToCopy)
	# 	copyFile(path,pathFileDB,listToCopy)      
	# 	MongoClient(databaseUrl, databasePort).close

	# def import_new(self, path):
	# 	dbConnect = MongoDB_method.DBconnect()
	# 	dataCSV = pd.read_csv(path, low_memory=False)
	# 	importData = json.loads(dataCSV.to_json(orient='records'))
	# 	dbConnect.insert(importData)
	# 	listToCopy = dataCSV[indexColumn]
	# 	listToCopy =  list(listToCopy)
	# 	copyFile(path,pathFileDB,listToCopy) 
	# 	MongoClient(databaseUrl, databasePort).close

	# def show_find_data(condition, databaseUrl, databasePort, databaseName, collectionName):
	# 	dbCollection = connect_database(databaseUrl, databasePort, databaseName, collectionName)
	# 	getData = dbCollection.find(condition)
	# 	df =  pd.DataFrame(list(getData))
	# 	df =  del_id(df)
	# 	print (df)
	# 	MongoClient(databaseUrl, databasePort).close

	def saveFile(self, findCondition, pathForSave, saveFileState): #(dic,str,int)
		dbConnect = MongoDB_method.DBconnect()
		getData = dbConnect.find(findCondition)
		df =  pd.DataFrame(list(getData))
		df =  Pandas_method.del_id(df) #ลบคอลัมภ์ id ทิ้งออกจาก Dataframe
		df.to_csv(pathForSave, index=False) #เซฟเป็น csv
		if saveFileState == 1 :
			listToCopy = list(df[indexColumn])#ดึงคอลัมภ์มาทำให้เป็น array
			pathFolder = Path_method.splitPath(pathForSave) #เอา path file มาตัดส่วนชื่อไฟล์ออก
			MongoDB_method.copyFile(pathFolder,pathFolder,listToCopy)
		MongoClient(self.__DBUrl, self.__DBPort).close
		System_method.complete()

	def copyFile(pathCopy,pathSave,listToCopy): #(str,str,list) ใช้ copy ไปดาต้าเบส หรือ โหลดไฟล์มาให้ user 
		listFile = os.listdir(pathCopy) #ทำ list file ในโฟลเดอร์
		if len(listFile) == 0:
			print ('\n Folder to copy is empty \n') 
		else:
			for f in tqdm(listFile, unit='B', unit_scale=True, unit_divisor=1024):
				for c in listToCopy:
					if f.find(c) == 0 :
						filePath = os.path.join(pathCopy,f)
						copy(filePath, pathSave)
						# print ('Complete copy file : ',fp) #เก็บไว้ทำ logfile กรณีถูกหบุดกลางคัน      
			#ตรงนี้ไว้ลบ logfile กรณีที่ copy เสร็จสมบูรณ์ตามปกติ ซึ่งถ้าลูปถูกหยุดกลางคัน log file จะไม่ถูกลบออกไป 
			
	def delFile(pathDel,listToDel): #(str,list)
		listFile = os.listdir(pathDel) #ทำ list file ในโฟลเดอร์
		if len(listFile) == 0:
			print ('\n Folder to delete is empty \n') 
		else:
			for f in tqdm(listFile, unit='B', unit_scale=True, unit_divisor=1024):
				for d in listToDel:
					if f.find(d) == 0 :
						filePath = os.path.join(pathDel,f)
						os.remove(filePath)

	def delDoc(self, findCondition): #(str)
		dbConnect = MongoDB_method.DBconnect()
		dbConnect.delete_many(findCondition)
		MongoClient(self.__DBUrl, self.__DBPort).close
		System_method.complete()

	def delDocFile(self, findCondition): #(str)
		dbConnect = MongoDB_method.DBconnect()
		getData = dbConnect.find(findCondition)
		df =  pd.DataFrame(list(getData))
		listToDel = list(df[indexColumn]) #ทำลิสต์ของข้อมูลที่จะลบ ไว้ไปลบไฟล์
		getData = dbConnect.delete_many(findCondition) #ลบข้อมูลในฐานข้อมูลทิ้ง
		MongoDB_method.delFile(pathFileDB,listToDel) #ลบไฟล์ทิ้ง
		MongoClient(self.__DBUrl, self.__DBPort).close
		System_method.complete()

	# def load_data_by_csv(path, databaseUrl, databasePort, databaseName, collectionName, loadDBfromCSV, typeSave, depthNode):
	# 	if path.find('/',0,1) == 0:
	# 		dbCollection = connect_database(databaseUrl, databasePort, databaseName, collectionName)
	# 		getData = dbCollection.distinct(indexColumn)
	# 		dataCSV = pd.read_csv(path, low_memory=False) 
	# 		csvCheck = dataCSV[indexColumn]
	# 		csvCheck =  list(csvCheck)
	# 		firstTime = True
	# 		s = path.split('/',-1)[-1]
	# 		parentPath = path.partition(s)[0]
	# 		pathSaveDB = os.path.join(parentPath,loadDBfromCSV)
	# 		for x in tqdm(csvCheck, unit='B', unit_scale=True, unit_divisor=1024):
	# 			if x in getData:
	# 				if firstTime == True:
	# 					findDBtoSave = dbCollection.find({indexColumn : x})
	# 					findDBtoSave = pd.DataFrame(list(findDBtoSave))
	# 					findDBtoSave = del_id(findDBtoSave)
	# 					findDBtoSave.to_csv(pathSaveDB,mode='a', index=False)
	# 					firstTime = False
	# 					if typeSave == 1:
	# 						pass #save only doc
	# 					elif typeSave == 2:
	# 						loadFile(pathFileDB,parentPath,x)
	# 						pass #save file
	# 					else:
	# 						print ('Error with "typeSave" value')
	# 				else:
	# 					findDBtoSave = dbCollection.find({indexColumn : x})
	# 					findDBtoSave = pd.DataFrame(list(findDBtoSave))
	# 					findDBtoSave =  del_id(findDBtoSave)
	# 					findDBtoSave.to_csv(pathSaveDB,mode='a',header=False, index=False)
	# 					if typeSave == 1:
	# 						pass #save only doc
	# 					elif typeSave == 2:
	# 						loadFile(pathFileDB,parentPath,x)
	# 						pass #save file
	# 					else:
	# 						print ('Error with "typeSave" value')
	# 			else:
	# 				pass
	# 		dataCSV.to_csv(pathNoSameData,mode='a', index=False)      
	# 		MongoClient(databaseUrl, databasePort).close
	# 		depthNode = complete_and_back()
	# 		return depthNode
	# 	else:
	# 		depthNode = system_key(path,depthNode)
	# 		return depthNode
