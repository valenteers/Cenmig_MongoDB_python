# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 08:54:35 2021

@author: User
"""
from import_modules import *

# import util
# import setting

# import pandas as pd
# from datetime import datetime


def backup_all(file_out, filename):
    dbCollection = util.connect_database()
    data = dbCollection.find({})
    data = pd.DataFrame.from_dict(data)
    print('Retriving data completed')
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    filename = filename + now
    filename = file_out + filename + '.csv'
    print('Writing to filename : ' + filename)
    data.to_csv(filename, index = False)
    print('Full Back up Completed at Time : ' + now)
    