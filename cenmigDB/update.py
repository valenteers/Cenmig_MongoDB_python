# #!/usr/bin/env python3  

#==================================== Load my packages =================================#

from import_modules import *

# import setting
# import util

#==================================== Load library =====================================#

# from pymongo import MongoClient
# from shutil import copy
# from tqdm import tqdm 

# import pandas as pd 
# from datetime import datetime 
#==================================== update function =====================================#

def update_field(csv_update, index_column , filename_backup):
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    dbCollection = util.connect_database()
    # Read update csv file
    new_data = pd.read_csv(csv_update, low_memory=False) 

    ID_list = new_data[index_column]
    old_data = pd.DataFrame()

    for i in ID_list:
        d = dbCollection.find({index_column : str(i) }) # find matching ID
        d = pd.DataFrame.from_dict(d)
        old_data = old_data.append(d)
    
    ##### Check if found old data
    # if found backup first
    if len(old_data) > 0:
        print('Found old data ' + str(len(old_data)) + ' records')
        print('Backup old data')
        df_old = pd.DataFrame()
        for i in old_data[index_column]:
            data_old = dbCollection.find({index_column : str(i)})
            data_old = pd.DataFrame.from_dict(data_old)
            df_old = old_data.append(data_old)
        df_old.to_csv(filename_backup + now + '.csv', index = False)
        print(filename_backup + now + '.csv' + ' Created')
    
    # Update
    if  len(old_data) != len(new_data):
        print('Updating ... \n')
        for i in new_data[index_column]:
            data_update = new_data[new_data[index_column] == str(i)]
            data_update = data_update.dropna(axis=1,how='all')  # Renove empty column
            update_dict = data_update.to_dict('records')        # return list
            update_dict = update_dict.pop()                     # pop แม้งซะ
            print(update_dict)
            print('Updating ' + str(i))
            dbCollection.update_one({index_column : str(i)}, {'$set' : update_dict}, upsert=True)
        print('\nFinished\n')
        

def replace_record(csv_update, index_column, filename_backup):
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    dbCollection = util.connect_database()
    # Read update csv file
    new_data = pd.read_csv(csv_update, low_memory=False) 

    ID_list = new_data[index_column]
    old_data = pd.DataFrame()

    for i in ID_list:
        d = dbCollection.find({index_column : str(i) }) # find matching ID
        d = pd.DataFrame.from_dict(d)
        old_data = old_data.append(d)
    
    ##### Check if found old data
    # if found backup first
    if len(old_data) > 0:
        print('Found old data ' + str(len(old_data)) + ' records')
        print('Backup old data')
        df_old = pd.DataFrame()
        for i in old_data[index_column]:
            data_old = dbCollection.find({index_column : str(i)})
            data_old = pd.DataFrame.from_dict(data_old)
            df_old = old_data.append(data_old)
        df_old.to_csv(filename_backup + now + '.csv', index = False)
        print(filename_backup + now + '.csv' + ' Created')
    
    # Update
    if  len(old_data) != len(new_data):
        print('Replacing ... \n')
        for i in new_data[index_column]:
            data_replace = new_data[new_data[index_column] == str(i)]
            update_dict = data_replace.to_dict('records')    # return list
            update_dict = update_dict.pop()                 # pop แม้งซะ
            print(update_dict)
            print('Replacing ' + str(i))
            dbCollection.replace_one({index_column : str(i)}, update_dict, upsert=True)
        print('\nFinished\n')

# replace_record(index_column='cenmigID', filename_out='kkk.csv')
# #==================================== End ========================================#