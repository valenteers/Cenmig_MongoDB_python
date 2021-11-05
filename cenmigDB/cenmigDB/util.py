#!/usr/bin/env python3 
#==================================== Load my packages =================================#

from cenmigDB import setting
from cenmigDB import shortcut
import cenmigDB.text as txt

#==================================== Load library =====================================#

from pymongo import MongoClient

import os
import time
import pandas as pd 

#==================================== Util function ==========================================#

#------------------------------------ MongoDB function -----------------------------------------#

def database_connect(url = setting.database_url, port = setting.database_port, name = setting.database_name, collection = setting.database_collection ):
	'''
	ใช้เปิดการเชื่อมต่อฐานข้อมูล mongoDB ตัวแปรที่ใช้ :
	- url : url หรือที่อยู่ของฐานข้อมูลเช่น 'mongodb://localhost'
	- port : เลข port ของโปรแกรม  mongoDB ค่า default คือ 27017
	- name : ชื่อดาต้าเบสที่ต้องการเชื่อมต่อ
	- collection : ชื่อของ collection หรือตารางที่ต้องการใช้ 
	'''
	client = MongoClient(url, port)
	database = client[name]
	database_connect = database[collection]
	return database_connect

def close_connect(url = setting.database_url, port = setting.database_port):
	'''
	ปิดการเชื่อมต่อ ตัวแปรที่ใช้ :
	- url : url หรือที่อยู่ของฐานข้อมูลเช่น 'mongodb://localhost'
	- port : เลข port ของโปรแกรม  mongoDB ค่า default : 27017
	''' 
	MongoClient(url, port).close
#------------------------------------ pandas function ---------------------------------------#

def delete_id(dataframe):
	'''
	ลบ id ที่ pandas สร้างอัตโนมัติใน dataframe ตัวแปรที่ใช้ :
	- dataframe : obj ของไลบรารี่ pandas
	'''
	if '_id' in dataframe:
		del dataframe['_id'] #ลบคอลัมภ์ id ที่สร้างอัตโนมัติของ mongodb ทิ้ง
	return dataframe

#------------------------------------ Path function -----------------------------------------#

def split_parent_path(path_file):
	'''ใช้แยกพาธไฟล์ ให้เหลือแค่พาธโฟลเดอร์'''
	p = path_file.split('/',-1)[-1] #เอา path file มาตัดส่วนชื่อไฟล์ออก
	path_folder = path_file.partition(p)[0] #เอา path file มาตัดส่วนชื่อไฟล์ออก
	return path_folder

def check_path(path):
	'''เช็คว่ามี / ในตัวแรกของ str หรือไม่'''
	path_state = ""
	if path == '': #ไม่ได้ใส่พาธมา มีสองตัวเลือก 1.ไม่ทำอะไรให้เซฟที่เดียวกับตัวสคริป 2.ใส่ค่า default path ให้แทน
		path_state = "program path"
	elif path.find('/',0,1) == 0: #มี / นำหน้า คาดว่าใส่พาธมา แต่พาธจะถูกไหมไม่รู้นะ
		path_state = "user path"
	else:
		path_state = "none"
	return path_state

#------------------------------------ shortcut function -----------------------------------------#

def system_key(command,depth_node):
    '''
	เช็คว่าตรงกับคำสั่งพื้นฐานของโปรแกรมหรือไม่ เช่น ย้อนกลับ, ออกโปรแกรม, คำแนะนำ, เมนู ฯลฯ ตัวแปรที่ใช้ :
	- command : คำสั่งที่ user ต้องการให้โปรแกรมทำงาน
	- depth_node : ระดับขั้นความลึกของโปรแกรมในแบบ tree degree ของโปรแกรม
	'''
    if command in shortcut.back:
        if depth_node == 0:
            print (txt.now_home_menu)
            time.sleep(setting.delay_time)
        else:
            depth_node = depth_node - 1
    elif command in shortcut.home_menu:
        print (txt.go_to_home_menu)
        time.sleep(setting.delay_time)
        depth_node = 0
    elif command in shortcut.exit_program:
        print (txt.exit_program)
        time.sleep(setting.delay_time)
        exit()
    elif command in shortcut.help_menu:
        time.sleep(setting.delay_time)
        print(txt.home_menu)
    elif command in shortcut.how_to_use_this_program:
        time.sleep(setting.delay_time)
        print(txt.how_to_use_this_program)
    else:
        time.sleep(setting.delay_time)
        print (txt.donot_have_this_command)
    return depth_node

def complete_and_back_to_home_menu():
    '''
    completeAndBackToMenu()
	ใช้แสดงผลการทำงานที่เสร็จสมบูรณ์แล้ว และย้อนกลับไปที่หน้าแรกสุดของโปรแกรม โดยกำหนดค่า tree degree : 0
	'''
    print (txt.complete)
    time.sleep(setting.delay_time)
    print (txt.go_to_home_menu)
    time.sleep(setting.delay_time)
    return 0 #return ให้ depth_node เป็น 0 เพื่อย้อนกลับไปที่ home_menu

#==================================== End Text ===========================================#
