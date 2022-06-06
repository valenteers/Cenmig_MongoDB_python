#!/usr/bin/env python3  

#==================================== Load my packages =================================#

import util
import setting


import os
import pandas as pd
from bson.objectid import ObjectId  ## For search MongoDB with _id

def del_records_by_query(query_file, index_column , delete_file_state = "none"):
    search_result = util.query(query_file)
    dbCollection = util.connect_database()
    
    if len(search_result) > 0:
        for i in search_result[index_column].dropna():
            dict_forDel =  dict({str(index_column) : str(i)})
            del_data = dbCollection.find(dict_forDel)
            del_data = pd.DataFrame.from_dict(del_data)
            del_data = pd.DataFrame.to_dict(del_data)
            print(del_data)
            dbCollection.delete_one(dict_forDel)
            print('delete data ' + str(i))
        print('\nDeleted successfully\n')
    
    if delete_file_state.upper() == 'YES':
        del_file(search_result)
    
def del_records_by_csv(csv_update, index_column , delete_file_state = "none"):
    df_del = pd.read_csv(csv_update, low_memory=False) 
    dbCollection = util.connect_database()
    
    # if len(search_result) > 0:
    #     for i in search_result[index_column].dropna():
    #         dict_forDel =  dict({str(index_column) : str(i)})
    #         del_data = dbCollection.find(dict_forDel)
    #         del_data = pd.DataFrame.from_dict(del_data)
    #         del_data = pd.DataFrame.to_dict(del_data)
    #         print(del_data)
    #         dbCollection.delete_one(dict_forDel)
    #         print('delete data ' + str(i))
    #     print('\nDeleted successfully\n')
    
    # IF there is _id meaning there is that data in DB
    if '_id' in df_del.columns or index_column == '_id':
        index_column = '_id'
        ID_list = df_del[index_column]
        ID_list = list(map(ObjectId, ID_list)) # Convert _id str to ObjectId for MongoDB
        use_id = True
        print('use_id')
    else:
        ID_list = df_del[index_column]
        use_id = False
        
    
    # Delete
    print('Deleting ... \n')
    for _ , row in df_del.iterrows():
        update_dict = row.dropna(how ='all')
        
        if '_id' in update_dict.index:
            update_dict = update_dict.drop('_id')
        
        update_dict = update_dict.to_dict()
        
        if use_id == True:
            print('Deleting ' + row['_id'])
            dbCollection.delete_one({index_column : ObjectId(row['_id'])})
        elif use_id == False:
            print('Deleting ' + row['cenmigID'])
            dbCollection.delete_one({index_column : str(row[index_column])})
        
        print(update_dict)
    print('\nFinished\n')
    
    ## Also delete records in Resistant
    db_resist = util.connect_database(collection = 'Resistant')
    db_resist.delete_many({index_column : {'$in' : list(set(ID_list)) }})
    
    
    
    if delete_file_state.upper() == 'YES':
        del_file(df_del)
        

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
                if os.path.exists(file):
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
                if os.path.exists(file):
                    print('Remove ' + file)
                    os.remove(file)