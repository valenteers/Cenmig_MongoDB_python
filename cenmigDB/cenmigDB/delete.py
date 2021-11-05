#!/usr/bin/env python3  

#==================================== Load my packages =================================#

from cenmigDB import util
import cenmigDB.text as txt
import setting

#==================================== Load library =====================================#

from pymongo import MongoClient
from tqdm import tqdm 

import pandas as pd 
import os

#==================================== Del function =====================================#

def delete_file(path_folder_to_delete ,list_to_delete ,empty_announce = txt.empty_folder): 
	'''
	ใช้ลบไฟล์ในโฟลเดอร์ ตามลิสต์ที่ส่งมาให้ ตัวแปรที่ใช้ :
	- path_folder_to_delete : พาธของโฟลเดอร์ที่ต้องการลบไฟล์ เช่น /home/your_user_name/name_folder
	- list_to_delete : ลิสต์ของรายการชื่อที่ต้องการลบ แบบ regrex คือมีบางส่วนของชื่อไฟล์ตรงก็ลบทิ้ง
	- empty_annource : text ที่จะให้แสดง กรณีที่โฟลเดอร์นั้นไม่มีไฟล์อะไรเลย
	'''
	list_file_in_folder = os.listdir(path_folder_to_delete) #ทำ list file ในโฟลเดอร์
	if len(list_file_in_folder) == 0: #ไม่มีไฟล์ในโฟลเดอร์ให้แจ้ง แล้วจบ ไม่งั้นจะเออเรอร์ตอนพยายามลบไฟล์
		print (empty_announce) 
	else:
		for file in tqdm(list_file_in_folder, unit='B', unit_scale=True, unit_divisor=1024): #ลูป file ใน list_file_in_folder
			for delete_file in list_to_delete: #ลูป delete_file ในลิสต์ list_to_delete เมื่อรวมกับบรรทัดบนจะไล่เช็คทุกตัวในทั้งสองลิสต์
				if file.find(delete_file) == 0 : #ถ้ามี delete_file ในบางส่วนของชื่อ file จะให้ค่า 0 
					path_to_delete = os.path.join(path_folder_to_delete,file) #เอามารวมกับพาธโฟลเดอร์ของไฟล์ที่ต้องการลบ
					os.remove(path_to_delete) #ลบไฟล์
	
def delete_data_file(search_Condition, delete_file_state = "none"):
	'''
	ใช้ลบข้อมูลในฐานข้อมูล mongoDB ถ้าให้พาธโฟลเดอร์ดาต้าเบสมาจะลบไฟล์ในโฟลเดอร์ด้วย ตัวแปรที่ใช้ :
	- search_Condition : เงื่อนไขในการค้นหาข้อมูลจาก mongoDB
	- path_folder_database : โฟลเดอร์ที่ใช้เก็บไฟล์ของฐานข้อมูล
	'''
	if delete_file_state == "delete":
		download_data = util.database_connect.find(search_Condition) #โหลดข้อมูลที่ต้องการลบตามเงื่อนไข
		download_data =  pd.DataFrame(list(download_data))
		list_to_delete = list(download_data[setting.index_column]) #ทำลิสต์ของข้อมูลที่จะลบ ไว้ไปลบไฟล์
		delete_file(setting.path_folder_database,list_to_delete) #โยนพาธโฟลเดอร์ กับลิสต์ที่ต้องการลบ เข้าฟังก์ชั่น delete_file() ให้มันลบไฟล์ทิ้ง
	download_data = util.database_connect.delete_many(search_Condition) #ลบข้อมูลในฐานข้อมูลทิ้ง
	util.close_connect() #ปิดการเชื่อมต่อดาต้าเบส
	print (txt.complete) 

#==================================== End ========================================#
