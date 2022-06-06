# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 08:54:35 2021

@author: User
"""

import util
import setting

import pandas as pd
from datetime import datetime


def export_all(out_path, filename):
    dbCollection = util.connect_database()
    data = dbCollection.find({})
    data = pd.DataFrame.from_dict(data)
    
    db_resist = util.connect_database(collection = 'Resistant')
    db_resist = db_resist.find({})
    db_resist = pd.DataFrame.from_dict(db_resist)
    
    print('Retriving data completed')
    
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    filename_main = filename + now
    filename_main = out_path + filename_main + '.csv'
    print('Writing to filename : ' + filename)
    data.to_csv(filename_main, index = False)
    filename_resist = filename + '_Resistant_'+ now
    filename_resist = out_path + filename_resist + '.csv'
    db_resist.to_csv(filename_resist + '.csv', index = False)
    print('Full Exporting Completed at Time : ' + now)
    