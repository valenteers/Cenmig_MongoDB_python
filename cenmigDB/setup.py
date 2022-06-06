# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 09:38:00 2022

@author: Admin
"""

import subprocess
import sys, os
import setting


## Script for set up folder and lib

def install_package(package = setting.package_required):
    for lib in package:
        subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
def make_folder(folder_list = setting.folder_list):
    for folder in folder_list:
        try:
            os.makedirs(folder)
        except:
            pass

def install():
    install_package()
    make_folder()
    
    print('''\ncenmigDB also use MongoDB, stringMLST.py, Resfinder, SeqSero2, sangerpathogens/mlst_check get_sequence_type(docker) and pysradb. \nPlease install these programmes manually, However also please check program path in analyze_SEA_data.py\n''')


