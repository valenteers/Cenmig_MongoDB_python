# #!/usr/bin/env python3  

#==================================== Load my packages =================================#

import setting
import util

#==================================== Load library =====================================#

# from pymongo import MongoClient
# from shutil import copy
# from tqdm import tqdm 

import pandas as pd 
from datetime import datetime 
from bson.objectid import ObjectId  ## For search MongoDB with _id
import glob
import os
#==================================== update function =====================================#

def update_record(csv_update, index_column , filename_backup, upsert = True):
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    dbCollection = util.connect_database()
    # Read update csv file
    new_data = pd.read_csv(csv_update, low_memory=False) 

    old_data = pd.DataFrame()
    
    # IF there is _id meaning there is that data in DB
    if '_id' in new_data.columns or index_column == '_id':
        index_column = '_id'
        ID_list = new_data[index_column]
        ID_list = list(map(ObjectId, ID_list)) # Convert _id str to ObjectId for MongoDB
        use_id = True
        print('use_id')
    else:
        ID_list = new_data[index_column]
        use_id = False
        
    d = dbCollection.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
    old_data = pd.DataFrame.from_dict(d)
    
    ##### Check if found old data
    # if found backup first
    if len(old_data) > 0:
        print('Found old data ' + str(len(old_data)) + ' records')
        print('Backup old data')
        old_data.to_csv(filename_backup + now + '.csv', index = False)
        print(filename_backup + now + '.csv' + ' Created')
    
    # Update
    print('Updating ... \n')
    for _ , row in new_data.iterrows():
        update_dict = row.dropna(how ='all')
        
        if '_id' in update_dict.index:
            update_dict = update_dict.drop('_id')
        
        update_dict = update_dict.to_dict()
        
        if use_id == True:
            print('Updating ' + row['_id'])
            dbCollection.update_one({index_column : ObjectId(row['_id'])}, {'$set' : update_dict}, upsert= upsert)
        elif use_id == False and index_column != 'BioSample':
            print('Updating ' + row['cenmigID'])
            dbCollection.update_one({index_column : str(row[index_column])}, {'$set' : update_dict}, upsert= upsert)
        elif use_id == False and index_column == 'BioSample':
            print('Updating ' + row['BioSample'])
            dbCollection.update_many({'BioSample' : str(row['BioSample'])}, {'$set' : update_dict}, upsert= upsert)
        
        print(update_dict)
    print('\nFinished\n')
    
    # Create Df for change filename to new cenmigID
    if use_id:
        print('Starting edit filename ...')
        df_change_file_name = pd.merge(old_data[['_id', 'cenmigID']].astype(str), 
                                        new_data[['_id', 'cenmigID']].astype(str), 
                                        suffixes=['_old', '_new'], on ='_id')
        
        if len(df_change_file_name) > 0:
            print(df_change_file_name)
        
            # Change old filename in data_sequences
            rename_inhouse_data_sequences(df_change_file_name)
            
            # Edit records in Resistant
            change_cenmigID_ResistantDB(df_change_file_name)
    
def rename_inhouse_data_sequences(df):
    for _ , row in df.iterrows():
        old_filename = row['cenmigID_old']
        new_filename = row['cenmigID_new']
        
        file_list = glob.glob(setting.data_sequences + '*/*/' + old_filename + '.*')
        new_file_dir = [s.replace(old_filename, new_filename) for s in file_list]
        zip_name = zip(file_list, new_file_dir)
        
        if old_filename != new_filename:
            
            for z in list(zip_name):
                print('Changing filename From {} to {}'.format(z[0], z[1]))
                os.rename(z[0], z[1])    
        else:
            pass

def change_cenmigID_ResistantDB(df):
    db_resist = util.connect_database(collection = 'Resistant')
    
    for _ , row in df.iterrows():
        old_id = row['cenmigID_old']
        new_id = row['cenmigID_new']
        
        if old_id != new_id:
            print(old_id, new_id)
            db_resist.update_many({'cenmigID' : str(old_id)}, {'$set' : {'cenmigID' : str(new_id)}})
        else:
            pass
        
        
    
    
# def update_record(csv_update, index_column , filename_backup):
#     now = datetime.now()
#     now = now.strftime("%d-%m-%Y_%H_%M_%S")
#     dbCollection = util.connect_database()
#     # Read update csv file
#     new_data = pd.read_csv(csv_update, low_memory=False) 

#     ID_list = new_data[index_column]
#     old_data = pd.DataFrame()
#     ## TOO Slow ! deprecated
#     # for i in ID_list:
#     #     print(str(i))
#     #     d = dbCollection.find({index_column : str(i) }) # find matching ID
#     #     d = pd.DataFrame.from_dict(d)
#     #     old_data = old_data.append(d)
    
#     d = dbCollection.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
#     old_data = pd.DataFrame.from_dict(d)
    
#     ##### Check if found old data
#     # if found backup first
#     if len(old_data) > 0:
#         print('Found old data ' + str(len(old_data)) + ' records')
#         print('Backup old data')
#         old_data.to_csv(filename_backup + now + '.csv', index = False)
#         print(filename_backup + now + '.csv' + ' Created')
    
#     # Update
#     print('Updating ... \n')
#     for i in new_data[index_column]:
#         data_update = new_data[new_data[index_column] == str(i)]
#         data_update = data_update.dropna(axis=1,how='all')  # Remove empty column
#         update_dict = data_update.to_dict('records')        # return list
#         update_dict = update_dict.pop()                     # pop แม้งซะ
#         print(update_dict)
#         print('Updating ' + str(i))
#         dbCollection.update_one({index_column : str(i)}, {'$set' : update_dict}, upsert= True)
#     print('\nFinished\n')

# def edit_data_inhouse(csv_update, filename_backup, index_column = '_id'):
#     now = datetime.now()
#     now = now.strftime("%d-%m-%Y_%H_%M_%S")
#     dbCollection = util.connect_database()
#     # Read update csv file
#     new_data = pd.read_csv(csv_update, low_memory=False) 

#     ID_list = new_data[index_column]
#     ID_list = list(map(ObjectId, ID_list)) # Convert _id str to ObjectId for MongoDB
#     old_data = pd.DataFrame()  
#     d = dbCollection.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
#     old_data = pd.DataFrame.from_dict(d)
    
    
#     ##### Check if found old data
#     # if found backup first
#     if len(old_data) > 0:
#         print('Found old data ' + str(len(old_data)) + ' records')
#         print('Backup old data')
#         old_data.to_csv(filename_backup + now + '.csv', index = False)
#         print(filename_backup + now + '.csv' + ' Created')
    
#     # Update
#     print('Editing ... \n')
#     for i in new_data[index_column]:
#         data_update = new_data[new_data[index_column] == str(i)]
#         data_update = data_update.dropna(axis=1,how='all')  # Remove empty column
#         data_update = data_update.drop('_id', axis = 1) # Drop _id col to not reassign and get error
#         update_dict = data_update.to_dict('records')        # return list
#         update_dict = update_dict.pop()                     # pop แม้งซะ
#         print(update_dict)
#         print('Editing ' + str(i))
#         dbCollection.update_one({index_column : ObjectId(i)}, {'$set' : update_dict}, upsert=False)
#         # dbCollection.update_one({index_column : str(i)}, {'$set' : update_dict}, upsert=False)
    
    
#     # Create Df for change filename to new cenmigID
#     df_change_file_name = pd.merge(old_data[['_id', 'cenmigID']].astype(str), 
#                                    new_data[['_id', 'cenmigID']].astype(str), 
#                                    suffixes=['_old', '_new'], on ='_id')
    
#     # Change name
#     rename_inhouse_data_sequences(df_change_file_name)

def replace_record(csv_update, index_column, filename_backup, upsert = False):
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    dbCollection = util.connect_database()
    # Read update csv file
    new_data = pd.read_csv(csv_update, low_memory=False) 

    old_data = pd.DataFrame()
    
    # IF there is _id meaning there is that data in DB
    if '_id' in new_data.columns or index_column == '_id':
        index_column = '_id'
        ID_list = new_data[index_column]
        ID_list = list(map(ObjectId, ID_list)) # Convert _id str to ObjectId for MongoDB
        use_id = True
    else:
        ID_list = new_data[index_column]
        use_id = False
        
    d = dbCollection.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
    old_data = pd.DataFrame.from_dict(d)
    
    ##### Check if found old data
    # if found backup first
    if len(old_data) > 0:
        print('Found old data ' + str(len(old_data)) + ' records')
        print('Backup old data')
        old_data.to_csv(filename_backup + now + '.csv', index = False)
        print(filename_backup + now + '.csv' + ' Created')
    
    # Update
    print('Updating ... \n')
    for _ , row in new_data.iterrows():
        update_dict = row.dropna(how ='all')
        
        if '_id' in update_dict.index:
            update_dict = update_dict.drop('_id')
        
        update_dict = update_dict.to_dict()
        print('Updating ' + row['cenmigID'])
        print(update_dict)
        
        if use_id == True:
            dbCollection.replace_one({index_column : ObjectId(row['_id'])}, update_dict, upsert= upsert)
        elif use_id == False:
            dbCollection.replace_one({index_column : str(row[index_column])}, update_dict, upsert= upsert)
    
    print('\nFinished\n')
    
    # Create Df for change filename to new cenmigID
    if use_id:
        df_change_file_name = pd.merge(old_data[['_id', 'cenmigID']].astype(str), 
                                        new_data[['_id', 'cenmigID']].astype(str), 
                                        suffixes=['_old', '_new'], on ='_id')
    
        # Change old filename in data_sequences
        rename_inhouse_data_sequences(df_change_file_name)


        
# def replace_record(csv_update, index_column, filename_backup):
#     now = datetime.now()
#     now = now.strftime("%d-%m-%Y_%H_%M_%S")
#     dbCollection = util.connect_database()
#     # Read update csv file
#     new_data = pd.read_csv(csv_update, low_memory=False) 

#     ID_list = new_data[index_column]
#     old_data = pd.DataFrame()

#     # for i in ID_list:
#     #     d = dbCollection.find({index_column : str(i) }) # find matching ID
#     #     d = pd.DataFrame.from_dict(d)
#     #     old_data = old_data.append(d)
    
#     d = dbCollection.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
#     old_data = pd.DataFrame.from_dict(d)
    
#     ##### Check if found old data
#     # if found backup first
#     if len(old_data) > 0:
#         print('Found old data ' + str(len(old_data)) + ' records')
#         print('Backup old data')
#         old_data.to_csv(filename_backup + now + '.csv', index = False)
#         print(filename_backup + now + '.csv' + ' Created')
    
#     # Replacing
#     print('Replacing ... \n')
#     for i in new_data[index_column]:
#         data_replace = new_data[new_data[index_column] == str(i)]
#         update_dict = data_replace.to_dict('records')    # return list
#         update_dict = update_dict.pop()                 # pop แม้งซะ
#         print(update_dict)
#         print('Replacing ' + str(i))
#         dbCollection.replace_one({index_column : str(i)}, update_dict, upsert=True)
#     print('\nFinished\n')

# def update_resistantDB(csv_update, filename_backup):
#     db_resist = util.connect_database(collection = 'Resistant')
#     db_resist_backup = db_resist.find({})
#     db_resist_backup = pd.DataFrame.from_dict(db_resist_backup)
    
#     # Backup
#     now = datetime.now()
#     now = now.strftime("%d-%m-%Y_%H_%M_%S")
#     db_resist_backup.to_csv( setting.backup_metadata_path + filename_backup + now + '.csv', index = False)
    
#     # Insert new data
#     new_data = pd.read_csv(csv_update, low_memory=False)
#     print('Updating Resistant DB')
#     db_resist.insert_many(new_data.to_dict('records'))
#     print('\nFinished\n')
    
def update_resistantDB(csv_update, filename_backup, index_column = 'cenmigID'):
    db_resist = util.connect_database(collection = 'Resistant')
    db_resist_backup = db_resist.find({})
    db_resist_backup = pd.DataFrame.from_dict(db_resist_backup)
    
    # Backup
    new_data = pd.read_csv(csv_update, low_memory=False) 

    ID_list = new_data[index_column]
    old_data = pd.DataFrame()
    
    d = db_resist.find({index_column : {'$in' : list(ID_list) }}) # find matching ID
    old_data = pd.DataFrame.from_dict(d)
    
    ##### Check if found old data
    # if found backup first
    if len(old_data) > 0:
        print('Found old data ' + str(len(old_data)) + ' records')
        print('Backup old data')
        now = datetime.now()
        now = now.strftime("%d-%m-%Y_%H_%M_%S")
        old_data.to_csv(filename_backup + now + '.csv', index = False)
        print(filename_backup + now + '.csv' + ' Created')
    
    ## For Resistant db delete old cenmigID all first then insert new
    print('Updating Resistant DB')
    print('Replacing old records')
    db_resist.delete_many({index_column : {'$in' : list(set(ID_list)) }})
    print(list(set(ID_list)))
    db_resist.insert_many(new_data.to_dict('records'))
    print('\nFinished\n')
                     
# #==================================== End ========================================#