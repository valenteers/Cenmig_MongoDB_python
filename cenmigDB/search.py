#!/usr/bin/env python3  

import setting
import util
from datetime import datetime
import pandas as pd


def searchDB(query_file, out_path , result_filename ):
    df = pd.DataFrame(columns= setting.df_colnames)
    result = util.query(query_file)
    
    result = pd.concat([df, result], ignore_index= True)
    # output_csv(result, filename_out)
    dbCollection = util.connect_database(collection= 'Resistant')
    result_resist = dbCollection.find({'cenmigID' : {'$in' : list(set(result['cenmigID'])) }})
    result_resist = pd.DataFrame.from_dict(result_resist)
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    filename = out_path + result_filename + now +'.csv'
    filename_resist = out_path + result_filename + '_resist_' + now +'.csv'
    
    result.to_csv( filename, index = False, header= result.columns)
    if len(result_resist) > 0:
        result_resist.to_csv(filename_resist, index = False)
    print(filename)
    print('Completed')
