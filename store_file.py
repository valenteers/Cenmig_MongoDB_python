# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 13:55:21 2021

@author: User
"""

import setting

import pandas as pd
import shutil

import os

def store_fileDB(csv_file, db_path, file_in):
    df = pd.read_csv(csv_file, low_memory=False)
    spp = setting.species
    
    sra = df[['Organism', 'Run']].dropna()
    if len(sra) > 0 :
        for sp in spp:
            query = sp + '*'
            path = db_path + sp.replace(' ', '_') + '/sra/'
            df_sra = sra.loc[sra['Organism'].str.contains(query, case = False, regex = True)]
            if len(df_sra) > 0 :
                for file in df_sra['Run']:
                    file = file + '.sra'
                    file_store = path
                    # Check if there is file tomoving and not already exist to destination
                    if os.path.exists(file_in + file) and not os.path.exists(file_store + file) :
                        shutil.move(file_in + file, file_store)
                        print('Move ' + file_in + file + ' to ' + file_store + file)
                    else:
                        print(file + ' Not Found')
        print('\nCompleted moving sra\n')
                
    fasta = df[['Organism', 'asm_acc']].dropna()
    if len(fasta) > 0 :
        for sp in spp:
            query = sp + '*'
            path = db_path + sp.replace(' ', '_') + '/fasta/'
            df_fasta = fasta.loc[fasta['Organism'].str.contains(query, case = False, regex = True)]
            if len(df_fasta) > 0 :
                for file in df_fasta['asm_acc']:
                    file = file + '.fa'
                    file_store = path
                    # Check if there is file tomoving and not already exist to destination
                    if os.path.exists(file_in + file) and not os.path.exists(file_store + file) :
                        shutil.move(file_in + file, file_store)
                        print('Move ' + file_in + file + ' to ' + file_store + file)
                    else:
                        print(file + ' Not Found')
        print('\nCompleted moving fasta\n')