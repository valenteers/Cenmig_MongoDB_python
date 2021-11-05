#!/usr/bin/env python3  

#==================================== Config Setting =====================================#

#------------------------------------ Database MongoDB -------------------------------------#

database_url = 'mongodb://localhost'
database_port = 27017
database_name = 'cenmigDB' #ชื่อดาต้าเบส
database_collection = 'epidemic' #ชื่อของเทเบิ้ล
index_column = 'run_accss' #คอลัมภ์ที่เป็น unique สำหรับใช้ค้นหา

#------------------------------------ Path -------------------------------------------------#

path = ''
path_folder_database = '/home/liria/Desktop/cenmigDB/' #path ที่เก็บไฟล์ของ Database
file_no_duplicate_with_database = 'no_duplicate_with_database.csv' #ชื่อไฟล์สำหรับฟังก์ชั่นที่ไว้ใช้เช็คไฟล์
default_path = '/home/liria/Desktop/import/cenmigDB.csv' #โฟลเดอร์สำหรับ import ไฟล์เข้า
request_by_csv = 'request.csv'

#------------------------------------ Deley Time -------------------------------------------#

delay_time = 0

#------------------------------------ depthNode -------------------------------------------------#

depth_node = 0

#==================================== End Setting ===========================================#