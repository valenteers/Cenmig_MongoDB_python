#!/usr/bin/env python3  


import util
from datetime import datetime


def searchDB(query_file, out_path , result_filename ):
    result = util.query(query_file)
    # output_csv(result, filename_out)
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    filename = out_path + result_filename + now +'.csv'
    result.to_csv( filename, index = False)
    print(filename)
    print('Completed')
