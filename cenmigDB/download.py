# -*- coding: utf-8 -*-
"""
Created on Mon Nov  8 09:35:42 2021

@author: User
"""
import setting
import pandas as pd
import glob2
import subprocess
import os
import numpy as np
import gzip
import shutil
import multiprocessing
from datetime import datetime


def download_from_ncbi(csv, file_out, db_path = setting.data_sequences):
    # Read csv
    tmp = pd.DataFrame(columns=(['Run', 'Organism', 'cenmigID', 'asm_acc', 'ftp_path']))
    df = pd.read_csv(csv, low_memory=False)
    df = tmp.append(df)
    df = df[['Run', 'Organism', 'cenmigID', 'asm_acc', 'ftp_path']]
    
    ### SRA section
    df_sra = df[['Run','cenmigID', 'Organism']].dropna()
    df_sra['Run_check'] = df['Run'].astype(str) + '.sra'
    
    # Check if no SRA in db
    db_sra_file_list = glob2.glob(db_path + '*/*/*.sra')
    db_sra_file_list = [i.rsplit('/', 1)[1] for i in db_sra_file_list]
    df_sra = df_sra.loc[~df_sra['Run_check'].isin(db_sra_file_list)]
    
    # Download SRA
    if len(df_sra) > 0 :
        download_sra_ncbi(df_sra, file_out)
    
    ### Fasta section
    df_fasta = df[['asm_acc','cenmigID', 'Organism', 'ftp_path']].dropna()
    df_fasta['asm_acc_check'] = df['asm_acc'].astype(str) + '.fa'
    
    # Check if no SRA in db
    db_fasta_file_list = glob2.glob(db_path + '*/*/*.fa')
    db_fasta_file_list = [i.rsplit('/', 1)[1] for i in db_fasta_file_list]
    df_fasta = df_fasta.loc[~df_fasta['asm_acc_check'].isin(db_fasta_file_list)]
    
    if len(df_fasta) > 0:
        if len(df_fasta) < 20 :
            download_fasta_ncbi(df_fasta, file_out)
        elif len(df_fasta) >= 20 :
            download_fasta_ncbi_multi(df_fasta, file_out)
    

def download_fasta_ncbi(df, file_out):
    #filepath = 'Staphylococcus_aurues_SEA.csv'
    # df = pd.read_csv(csv, low_memory=False)
    # df = df[['cenmigID', 'asm_acc', 'ftp_path']]
    # df = df.dropna(subset=['ftp_path'], inplace=False)
    print('Found fasta ' + str(len(df)) +' files')
    for i in df.index:
        data = df['asm_acc'][i]
        ftp = df['ftp_path'][i]
        ## Modified link
        link =ftp.rsplit('/', 1)
        ftp = ftp + '/' + link[1] + '_genomic.fna.gz'
        ### NCBI change from ftp to https CANT use regex anymore
        #ftp = ftp + '/GCA*.[0-9]_genomic.fna.gz' # Search by .1_genomic somedata has many genomic.fna.gz
        
        # Fasta
        file = link[1] + '_genomic.fna.gz'
        file = file_out + file
        if not os.path.exists(file):
            download_fasta = subprocess.run(["wget -q " + ftp + ' -P ' + file_out], shell=True )
            print(data + ' COMPLETED')
        else:
            print(file + ' Existed')
            pass
        
def download_fasta_ncbi_multi(df, file_out):
    # df = pd.read_csv(csv, low_memory=False)
    # df = df[['cenmigID', 'asm_acc', 'ftp_path']]
    # df = df.dropna(subset=['ftp_path'], inplace=False)
    print('Found fasta ' + str(len(df)) +' files')
    
    # Create list of cmd
    index = list(df.index)
    
    cmd = []
    for i in index:
        ftp = df['ftp_path'][i]
        ### NCBI change from ftp to https CANT use regex anymore
        #ftp = ftp + '/GCA*.[0-9]_genomic.fna.gz' # Search by .1_genomic somedata has many genomic.fna.gz
        
        ## Modified link
        link =ftp.rsplit('/', 1)
        ftp = ftp + '/' + link[1] + '_genomic.fna.gz'
        file = link[1] + '_genomic.fna.gz'
        file = file_out + file
        if not os.path.exists(file):
            cmd_i = "wget -q " + ftp + ' -P ' + file_out
            cmd.append(cmd_i)
        else:
            print(file + ' Existed')
            pass
    # join cmd to command line of mutiple task
    cmd_len = len(cmd)
    cmd_num = 20
    cmd_group = cmd_len // cmd_num

    cmd = np.array_split(cmd, cmd_group)
    for j in cmd:
        run = ' & '.join(j)
        download_fasta = subprocess.run([run], shell=True)
     
def unzip_fa(file_path = setting.dataport):
    print('Start unzipping')
    file_gz = glob2.glob(file_path + '*_genomic.fna.gz')
    for gz in file_gz:
        filename = gz
        filename = filename.rsplit('/', 1)[1]
        filename = filename.split('_')[:2]
        print(filename)
        filename = '_'.join(filename)
        print(filename)
        gz_in = gz
        fa_out = file_path + filename + '.fa'
        extract_gunzip(gz_in, fa_out)
    print('\nExtract COMPLETED\n')
        
def extract_gunzip(gz_in, fa_out):
    with gzip.open( gz_in , 'rb') as f_in:
        with open( fa_out, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            print(str(gz_in) + ' ' + 'was extracted')
            

# SRA
def download_sra_ncbi(df, file_out):
    # df = pd.read_csv(csv, low_memory=False)
    # df = df[['cenmigID', 'Run']]
    # df = df.dropna(subset=['Run'], inplace=False)
    print('Found SRA ' + str(len(df)) +' files')
    for i in df.index:
        data = df['cenmigID'][i]
        run = df['Run'][i]
        file = file_out + run + '.sra'
        if not os.path.exists(file):
            cmd = "prefetch -f yes -o " + file + ' ' + run
            download_sra = subprocess.run([cmd], shell=True )
            print(file + ' COMPLETED')
        else:
            print(data + ' Existed')
            
def convert_sra_to_fastq_gunzip(filepath = setting.dataport):
    sra_file = glob2.glob(filepath)
    core = multiprocessing.cpu_count()
    
    for sra in sra_file:
        file = sra
        print('Converting ' + str(file))
        filename = file.replace('.sra', '.fq')
        cmd = 'fasterq-dump -O . -o ' + filename + ' -e ' + str(core) + ' '
        cmd = cmd + file
        convert_sra = subprocess.run([cmd], shell=True )
        pigz = 'pigz -f -p ' + str(core) + ' *.fq'
        compress = subprocess.run([pigz], shell=True )
    print('Convert & Compress are finished')

def retrive_fa_header(filepath = setting.dataport):
    # Return df of asm_acc and wgs project acc
    fasta_path = glob2.glob(filepath)
    col = ['asm_acc', 'wgs_project']
    df = pd.DataFrame(columns=(col))
    print('Start parsing fasta header')
    for file in fasta_path:
        # retrive asm_acc from filename
        fasta_filename = file
        asm_acc = fasta_filename.replace('_ASM.*', '')
        #print(fasta_filename)
        #print(asm_acc)
        # open fata file
        fasta_file = open(file, 'r')
        fasta = fasta_file.read()
        fasta = fasta.splitlines()
        fasta_file.close()
        
        # Get fasta header
        count = 0
        for line in fasta:
            if line.startswith('>') and count == 0:
                count = 1
                header = line
                header = header.split(' ')[0]
                wgs_project = header.replace('>', '')
                #print(wgs_project)
        df_i = pd.DataFrame([[asm_acc , wgs_project]], columns=(col))
        df = df.append(df_i)
    print('Finish parse fasta header')
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    file_out = filepath + 'ASM_acc_WGS_project' + now +'.csv'
    df.to_csv(file_out, index= False)
