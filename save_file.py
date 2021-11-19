# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 10:24:36 2021

@author: User
"""

#==================================== Load my packages =================================#

import util
import setting
import shortcut
import text as txt

#==================================== Load library =====================================#

# from pymongo import MongoClient
from shutil import copy
from tqdm import tqdm 

import pandas as pd 
import os

#==================================== Save function =====================================#

def save_data_query(query_file, file_out,
    db_path = setting.data_sequences, save_file_state = "none"): 
    '''
    ใช้ดึงข้อมูลจากดาต้าเบสมาเซฟ ถ้า save_file_state = "save" จะทำการก็อปปี้ไฟล์ที่เกี่ยวข้องกับดาต้ามาด้วย ตัวแปรที่ใช้ :
    - query_file : เงื่อนไขในการค้นหาข้อมูลจาก mongoDB
    - file_out : พาธสำหรับเซฟข้อมูลจากดาต้าเบส เช่น /home/your_user_name/name_folder/name_file.CSV ถ้าปล่อยว่างจะเซฟลงที่เดียวกับโฟลเดอร์ของโปรแกรม
    - db_path : พาธโฟลเดอร์ที่ใช้เก็บไฟล์ของฐานข้อมูล
    - save_file_state : "none" = โหลดแค่ข้อมูล, "save" = โหลดทั้งข้อมูล และไฟล์ที่เกี่ยวข้อง
    '''
    search_result = util.query(query_file)
    search_result.to_csv(file_out + 'query_get_data.csv', index=False) #เซฟเป็น csv แบบไม่เอา index ของ dataframe
    if save_file_state == "save" : #ถ้าเป็น none ไม่ต้องก็อปปี้ไฟล์, save ให้ก็อปปี้ไฟล์ด้วย
        # SRA
        sra = search_result[['Organism','Run']].dropna()
        # Assign data path based on species
        if len(sra) > 0:
            spp = setting.species
            for sp in spp:
                query = sp + '*'
                path = db_path + sp.replace(' ', '_') + '/sra/'
                df = sra.loc[sra['Organism'].str.contains(query, case = False, regex = True)]
                for file in df['Run']:
                    file = path + file + '.sra'
                    print('Copying ' + file + ' to ' + file_out)
                    copy(file, file_out)
        # Fasta
        fasta = search_result[['Organism','asm_acc']].dropna()
        # Assign data path based on species
        if len(fasta) > 0:
            spp = setting.species
            for sp in spp:
                query = sp + '*'
                path = db_path + sp.replace(' ', '_') + '/fasta/'
                df = fasta.loc[fasta['Organism'].str.contains(query, case = False, regex = True)]
                for file in df['asm_acc']:
                    file = path + file + '.fa'
                    print('Copying ' + file + ' to ' + file_out)
                    copy(file, file_out)      
                    
    util.close_connect()
    print('Copy completed')

def save_data_csv(csv_file, file_out, 
                  db_path = setting.data_sequences, save_file_state = "none"): 
    '''
    ใช้ดึงข้อมูลจากดาต้าเบสมาเซฟ ถ้า save_file_state = "save" จะทำการก็อปปี้ไฟล์ที่เกี่ยวข้องกับดาต้ามาด้วย ตัวแปรที่ใช้ :
    - csv_file : ข้อมูลที่ได้จากการ query mongoDB
    - file_out : พาธสำหรับเซฟข้อมูลจากดาต้าเบส เช่น /home/your_user_name/name_folder/name_file.CSV ถ้าปล่อยว่างจะเซฟลงที่เดียวกับโฟลเดอร์ของโปรแกรม
    - index_column : ชื่อคอลัมภ์ที่ใช้อ้างอิงเพื่อโหลดไฟล์
    - db_path : พาธโฟลเดอร์ที่ใช้เก็บไฟล์ของฐานข้อมูล
    - save_file_state : "none" = โหลดแค่ข้อมูล, "save" = โหลดทั้งข้อมูล และไฟล์ที่เกี่ยวข้อง
    '''
    
    search_result = pd.read_csv(csv_file, low_memory=False)
    if save_file_state == "save" : #ถ้าเป็น none ไม่ต้องก็อปปี้ไฟล์, save ให้ก็อปปี้ไฟล์ด้วย
        # SRA
        sra = search_result[['Organism','Run']].dropna()
        # Assign data path based on species
        if len(sra) > 0:
            spp = setting.species
            for sp in spp:
                query = sp + '*'
                path = db_path + sp.replace(' ', '_') + '/sra/'
                df = sra.loc[sra['Organism'].str.contains(query, case = False, regex = True)]
                for file in df['Run']:
                    file = path + file + '.sra'
                    print('Copying ' + file + ' to ' + file_out)
                    copy(file, file_out)
        # Fasta
        fasta = search_result[['Organism','asm_acc']].dropna()
        # Assign data path based on species
        if len(fasta) > 0:
            spp = setting.species
            for sp in spp:
                query = sp + '*'
                path = db_path + sp.replace(' ', '_') + '/fasta/'
                df = fasta.loc[fasta['Organism'].str.contains(query, case = False, regex = True)]
                for file in df['asm_acc']:
                    file = path + file + '.fa'
                    print('Copying ' + file + ' to ' + file_out)
                    copy(file, file_out)      
                    
    util.close_connect()
    print('Copy completed')
