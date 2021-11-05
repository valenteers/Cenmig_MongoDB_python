#!/usr/bin/env python3  

#==================================== Load my packages =================================#

from cenmigDB import util
from cenmigDB import setting
from cenmigDB import shortcut
import text as txt

#==================================== Load library =====================================#

# from pymongo import MongoClient
from shutil import copy
from tqdm import tqdm 

import pandas as pd 
import os

#==================================== Save function =====================================#

def save_data_file(search_condition, path_to_save,index_column = setting.index_column,
	path_folder_database = setting.path_folder_database, save_file_state = "none"): 
	'''
	ใช้ดึงข้อมูลจากดาต้าเบสมาเซฟ ถ้า save_file_state = "save" จะทำการก็อปปี้ไฟล์ที่เกี่ยวข้องกับดาต้ามาด้วย ตัวแปรที่ใช้ :
	- search_condition : เงื่อนไขในการค้นหาข้อมูลจาก mongoDB
	- path_to_save : พาธสำหรับเซฟข้อมูลจากดาต้าเบส เช่น /home/your_user_name/name_folder/name_file.CSV ถ้าปล่อยว่างจะเซฟลงที่เดียวกับโฟลเดอร์ของโปรแกรม
	- index_column : ชื่อคอลัมภ์ที่ใช้อ้างอิงเพื่อโหลดไฟล์
	- path_file_database : พาธโฟลเดอร์ที่ใช้เก็บไฟล์ของฐานข้อมูล
	- save_file_state : "none" = โหลดแค่ข้อมูล, "save" = โหลดทั้งข้อมูล และไฟล์ที่เกี่ยวข้อง
	'''
	download_data = util.database_connect.find(search_condition)
	download_data = pd.DataFrame(list(download_data)) #นำข้อมูลที่ได้กลับมาจาก mongoDB ใส่ไว้ใน dataframe
	download_data = util.delete_id(download_data) #ลบคอลัมภ์ id ทิ้งออกจาก Dataframe (เป็น id ของ mongoDB ที่มันทำให้แบบออโต้)
	download_data.to_csv(path_to_save, index=False) #เซฟเป็น csv แบบไม่เอา index ของ dataframe
	if save_file_state == "save" : #ถ้าเป็น none ไม่ต้องก็อปปี้ไฟล์, save ให้ก็อปปี้ไฟล์ด้วย
		list_to_copy = list(download_data[index_column])#ดึงคอลัมภ์มาทำให้เป็น array
		path_folder = util.split_parent_path(path_to_save) #เอา path file มาตัดส่วนชื่อไฟล์ออก
		copy_file(path_folder_database,path_folder,list_to_copy) 
	util.close_connect()
	txt.complete()

def copy_file(path_to_copy, path_to_save, list_to_copy, empty_announce = txt.empty_folder): 
	'''
	ใช้ copy ไฟล์ จากโฟลเดอร์ของ  user ไปดาต้าเบส หรือ โหลดไฟล์จากดาต้าเบสมาให้ user ตัวแปรที่ใช้ :
	- path_to_copy : พาธของโฟลเดอร์ที่จะคัดลอกข้อมูล
	- path_to_save : พาธสำหรับเซฟข้อมูล เช่น /home/NameUser/NameFolder/NameFile.CSV ถ้าปล่อยว่างจะเซลงที่เดียวกับ main.py
	- list_to_copy : ลิสต์ของชื่อที่จะใช้หาไฟล์ที่จะคัดลอกจาก pathToCopy
	- empty_announce : text ที่จะแสดงหากโฟลเดอร์ที่ต้องารคัดลอกไม่มีไฟล์อะไรเลย
	'''
	list_file_in_folder = os.listdir(path_to_copy) #ทำ list file ในโฟลเดอร์ที่จะทำการก็อปปี้
	if len(list_file_in_folder) == 0: #เช็คว่ามีไฟล์อยู่ข้างใน ไม่งั้นจะเออเรอรตอนที่วนลูปดึงข้อมูล
		print (empty_announce) 
	else:
		for file in tqdm(list_file_in_folder, unit='B', unit_scale=True, unit_divisor=1024):
			for copy_file in list_to_copy:
				if file.find(copy_file) == 0 :
					path_to_copy = os.path.join(path_to_copy,file)
					copy(path_to_copy, path_to_save)
					# print ('Complete copy file : ',fp) #เก็บไว้ทำ logfile กรณีถูกหบุดกลางคัน      
		#ตรงนี้ไว้ลบ logfile กรณีที่ copy เสร็จสมบูรณ์ตามปกติ ซึ่งถ้าลูปถูกหยุดกลางคัน log file จะไม่ถูกลบออกไป 
		
def load_data_by_csv(path_file_csv, depth_node, save_file_state, index_column = setting.index_column, 
	request_by_csv = setting.request_by_csv, path_folder_database = setting.path_folder_database,
	file_no_duplicate_with_database = setting.file_no_duplicate_with_database):
	'''
	ใช้โหลดข้อมูลจากฐานข้อมูลด้วยไฟล์ CSV ตัวแปรที่ใช้ :
	- pathCSV : พาธของไฟล์ CSV ที่จะใช้ค้นหาในดาต้าเบส เพื่อที่จะคัดลอกข้อมูล เช่น /home/NameUser/NameFolder/NameFile.CSV
	- depthNode : ระดับขั้นความลึกของ tree degree ในคำสั่ง main.py
	- typeSave : ถ้ามีค่า 1 ให้ทำการคัดลอกไฟล์ที่เกี่ยวข้องจากดาต้าเบสด้วย
	- indexColumn : ชื่อคอลัมภ์ที่ใช้ในการค้นหาเพื่อโหลดข้อมูลจากดาต้าเบส
	- nameFile : ชื่อไฟล์ CSV ที่จะใช้เซฟข้อมูลจากดาต้าเบส
	- pathFileDB : พาธของโฟลเดอร์ที่ใช้เก็บไฟล์ของดาต้าเบส
	- pathNoSameData : พาธของไฟล์ CSV ที่ใช้เก็บข้อมูลที่ไม่ซ้ำกับในดาต้าเบส
	'''
	if path.checkPath(pathCSV) == 1 or path.checkPath(pathCSV) == 2 :
		firstTime = True
		getData = util.database_connect.distinct(indexColumn) 
		dataCSV = pd.read_csv(pathCSV, low_memory=False) 
		csvCheck = list(dataCSV[indexColumn])
		parentPath = path.splitParentPath(pathCSV)
		pathSaveDataFromDB = os.path.join(parentPath,nameFile)
		for x in tqdm(csvCheck, unit='B', unit_scale=True, unit_divisor=1024):
			if x in getData:
				findDBtoSave = util.database_connect.DBconnect.find({indexColumn : x})
				findDBtoSave = pd.DataFrame(list(findDBtoSave))
				findDBtoSave = delete.id(findDBtoSave)
				if firstTime == True:
					findDBtoSave.to_csv(pathSaveDataFromDB, index=False)
					firstTime = False
				elif firstTime == False:
					findDBtoSave.to_csv(pathSaveDataFromDB,mode='a',header=False, index=False)
				if typeSave == 1:
					copy_file(pathFileDB,parentPath,x)
			else:
				pass
		dataCSV.to_csv(pathNoSameData, index=False)      
		util.close_connect()
		depthNode = shortcut.completeAndBackToMenu()
		return depthNode
	else:
		depthNode = shortcut.systemKey(pathCSV,depthNode)
		return depthNode
		
#==================================== End ========================================#