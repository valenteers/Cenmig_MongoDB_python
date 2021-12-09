#!/usr/bin/env python3  

#==================================== Load my packages =================================#

import util
import setting


import os
import pandas as pd

def del_records_by_query(query_file, index_column , delete_file_state = "none"):
    search_result = util.query(query_file)
    dbCollection = util.connect_database()
    
    if len(search_result) > 0:
        for i in search_result[index_column].dropna():
            dict_forDel =  dict({str(index_column) : str(i)})
            print(i)
            dbCollection.delete_one(dict_forDel)
            print('delete data ' + str(i))
        print('\nDeleted successfully\n')
    
    if delete_file_state.upper() == 'YES':
        del_file(search_result, db_path = setting.data_sequences)
    
def del_records_by_csv(csv_filename, index_column , delete_file_state = "none"):
    search_result = pd.read_csv(csv_filename, low_memory=False) 
    dbCollection = util.connect_database()
    
    if len(search_result) > 0:
        for i in search_result[index_column].dropna():
            dict_forDel =  dict({str(index_column) : str(i)})
            print(i)
            dbCollection.delete_one(dict_forDel)
            print('delete data ' + str(i))
        print('\nDeleted successfully\n')
    
    if delete_file_state.upper() == 'YES':
        del_file(search_result)
        

def del_file(df_delete, db_path = setting.data_sequences): 
    '''
    ใช้ลบไฟล์ในโฟลเดอร์ ตามลิสต์ที่ส่งมาให้ ตัวแปรที่ใช้ :
    - df_delete : ลิสต์ของรายการชื่อที่ต้องการลบ แบบ regrex คือมีบางส่วนของชื่อไฟล์ตรงก็ลบทิ้ง
    - db_path : พาธของโฟลเดอร์ที่ต้องการลบไฟล์ เช่น /home/your_user_name/name_folder
    - empty_annource : text ที่จะให้แสดง กรณีที่โฟลเดอร์นั้นไม่มีไฟล์อะไรเลย
    '''
    # SRA
    sra = df_delete[['Organism','Run']].dropna()
    # Assign data path based on species
    if len(sra) > 0:
        spp = setting.species
        for sp in spp:
            query = sp + '*'
            path = db_path + sp.replace(' ', '_') + '/sra/'
            df = sra.loc[sra['Organism'].str.contains(query, case = False, regex = True)]
            for file in df['Run']:
                file = path + file + '.sra'
                print('Remove ' + file)
                os.remove(file)
    # Fasta
    fasta = df_delete[['Organism','asm_acc']].dropna()
    # Assign data path based on species
    if len(fasta) > 0:
        spp = setting.species
        for sp in spp:
            query = sp + '*'
            path = db_path + sp.replace(' ', '_') + '/fasta/'
            df = fasta.loc[fasta['Organism'].str.contains(query, case = False, regex = True)]
            for file in df['asm_acc']:
                file = path + file + '.fa'
                print('Remove ' + file)
                os.remove(file)