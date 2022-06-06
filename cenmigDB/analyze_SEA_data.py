# -*- coding: utf-8 -*-
"""
Created on Wed Dec 22 12:23:23 2021

@author: Admin
"""

import setting
import util

import subprocess
import os
import pandas as pd
import multiprocessing
import shutil
from datetime import datetime
import glob
import json
from download import unzip_fa
from metadata_create import metadata_drug_resistant_Resfinder
import time

def get_df(csv_file, query = setting.analyze_query):
    
    if csv_file == True:
        # Load data from Mongo
        db_metadata = util.connect_database()
        db_metadata = db_metadata.find(query)
        db_metadata = pd.DataFrame.from_dict(db_metadata)
    
    elif 'csv' in csv_file:
        db_metadata = pd.read_csv(csv_file)

    db_resist = util.connect_database(collection = 'Resistant')
    db_resist = db_resist.find({})
    db_resist = pd.DataFrame.from_dict(db_resist)
    
    return db_metadata, db_resist

    
    
def process_SEA_data(csv_file, file_out = 'Process_out/', temp_path = 'tmp_for_workflow/', 
                     core = 5, job = 12, db_path = setting.data_sequences):
    
    # Load module
    cmd_load_module = 'ml SRA-Toolkit/3.0.0-ubuntu64 SPAdes/3.13.2 SeqSero2/1.2.1'
    #subprocess.run([cmd_load_module], shell=True)
    print('Please check loading module before start since cant load module within python script')
    print(cmd_load_module)
    time.sleep(5)
    
    # # Load data from Mongo
    # db_metadata = util.connect_database()
    # db_metadata = db_metadata.find({'sub_region' : 'South-eastern Asia'})
    # db_metadata = pd.DataFrame.from_dict(db_metadata)
    
    # db_resist = util.connect_database(collection = 'Resistant')
    # db_resist = db_resist.find({})
    # db_resist = pd.DataFrame.from_dict(db_resist)
    
    ## Get data from Mongo or CSV file
    db_metadata , db_resist = get_df(csv_file)
    
    # Make tmp folder
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)
    # Make output folder
    if not os.path.exists(file_out):
        os.mkdir(file_out)
        
    ## SRA
    # try:
    #     df_sra = db_metadata[['Run', 'LibraryLayout', 'Organism', 'Platform','sub_region', 'ST', 'Predicted_serotype']].dropna(subset = ['Run'])
    # except: # For first time
    #       df_sra = db_metadata[['Run', 'LibraryLayout', 'Organism', 'Platform','sub_region']].dropna(subset = ['Run'])
    #       df_sra[['ST', 'Predicted_serotype']] = None
    sra_col = ['Run', 'LibraryLayout', 'Organism', 'Platform','sub_region', 'ST', 'Predicted_serotype']
    df_sra = pd.DataFrame(columns = sra_col)
    df_sra = pd.concat([df_sra, db_metadata], ignore_index= True)
    df_sra = df_sra[sra_col].dropna(subset = ['Run'])
    
    if len(df_sra) > 0 :
        print('Run SRA')
        process_SRA(df_sra, db_resist, file_out, temp_path, core, job, db_path)
    
    ## Fasta
    # try:
    #     df_fasta = db_metadata[['cenmigID', 'LibraryLayout', 'Organism', 'Platform','sub_region', 'ST', 'ftp_path', 'Predicted_serotype', 'asm_acc']]
    #     df_fasta = df_fasta.loc[df_fasta['cenmigID'].str.contains('GCA*', case = True, regex =True)]
    # except: # For first time
    #     df_fasta = db_metadata[['cenmigID', 'LibraryLayout', 'Organism', 'Platform','sub_region', 'ftp_path', 'asm_acc']]
    #     df_fasta[['ST', 'Predicted_serotype']] = None
    
    fasta_col = ['cenmigID', 'LibraryLayout', 'Organism', 'Platform','sub_region', 'ST', 'ftp_path', 'Predicted_serotype', 'asm_acc']
    df_fasta = pd.DataFrame(columns = fasta_col)
    df_fasta = pd.concat([df_fasta, db_metadata], ignore_index=True)
    df_fasta = df_fasta[fasta_col]
    df_fasta = df_fasta.loc[df_fasta['cenmigID'].str.contains('GCA*', case = True, regex =True)]
    if len(df_fasta) > 0 :
        print('Run FASTA')
        process_fasta(df_fasta, db_resist, file_out, temp_path, core, job, db_path)
    
    # Merge result
    append_mlst_result(file_out)
    metadata_drug_resistant_Resfinder(path_resfinder_result = file_out + '*/ResFinder_results_tab.txt', 
                                      path_std_json = file_out + '*/std_format_under_development.json',
                                      filename_out = file_out + 'metadata_drug_resistant.csv')
    append_seqsero2_result(file_out)
    
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    print('Finished at ' + now)
    return file_out

def generated_tuple_list_of_arg(sp, species, df_spp, sequence_typing_running, 
                                resfinder_completed, core, 
                                file_out, db_path, temp_path):
    # Create tuple of arg for run multiprocess task
    out_ls = []
    for i in df_spp.index:
        df_i = df_spp.loc[i]
        tuple_i = (sp, species, df_i, sequence_typing_running, 
                   resfinder_completed, core, file_out, db_path, temp_path)
        out_ls.append(tuple_i)
    return out_ls

#def append_seqsero2_result(file_in = '/data/home/tiravut.per/seqsero2_tmp/*/') 
def append_seqsero2_result(file_out, file_in = '/data/home/tiravut.per/seqsero2_tmp/*/SeqSero_result.tsv'):
    file = glob.glob(file_in)
    seqsero2_out = pd.DataFrame()
    if len(file) > 0:
        for i in file:
            file_i = pd.read_csv(i, low_memory=False, sep = '\t')
            seqsero2_out = seqsero2_out.append(file_i)
        
        seqsero2_out = seqsero2_out[['Sample name', 'O antigen prediction', 'H1 antigen prediction(fliC)', 
                                    'H2 antigen prediction(fljB)', 'Predicted identification', 
                                    'Predicted antigenic profile', 'Predicted serotype']]
        seqsero2_out.columns = ['cenmigID', 'O_antigen_prediction', 'H1_antigen_prediction(fliC)', 
                                    'H2_antigen_prediction(fljB)', 'Predicted_identification', 
                                    'Predicted_antigenic_profile', 'Predicted_serotype']

        
        seqsero2_out['cenmigID'] = seqsero2_out['cenmigID'].apply(fix_seqsero2_cenmigID)
        
        seqsero2_out.to_csv(file_out + 'seqsero2_out.csv', index= False)

def fix_seqsero2_cenmigID(i):
    if type(i) == str:
        if not 'GCA' in i:
            if '_' in i:
                txt = i.split('_')[0]
            elif '.fa' in i:
                txt = i.split('.')[0]
        elif 'GCA' in i:
            txt = i.split('.fa')[0]
    
        return txt
        
def append_mlst_result(file_out):
    # for SRA result
    file_list_sra = glob.glob(file_out + '*.mlst')
    file_list_fasta = glob.glob(file_out + '*mlst/mlst_results.allele.csv')
    
    if len(file_list_sra) > 0 or len(file_list_fasta) > 0 :
        mlst_out_sra = pd.DataFrame()
        if len(file_list_sra) > 0 :
            for i in file_list_sra:
                print(i)
                mlst = pd.read_csv(i, sep = '\t')
                mlst_out_sra = mlst_out_sra.append(mlst)
                
            mlst_out_sra = mlst_out_sra.rename(columns = {'Sample':'cenmigID'})
        
        mlst_out_fasta = pd.DataFrame()
        # For Fasta result
        
        if len(file_list_fasta) > 0 :
            for i in file_list_fasta:
                print(i)
                mlst = pd.read_csv(i, sep = '\t')
                mlst_out_fasta = mlst_out_fasta.append(mlst)
            
            mlst_out_fasta = mlst_out_fasta.rename(columns = {'Isolate':'cenmigID'})
            mlst_out_fasta = mlst_out_fasta.drop(['New ST', 'Contamination'], 1)
        
        if len(mlst_out_sra) > 0 or len(mlst_out_fasta) > 0:
            mlst_out = mlst_out_sra.append(mlst_out_fasta)
            mlst_out.to_csv(file_out + 'mlst_out.csv', index= False)

def process_SRA(df_sra, db_resist,file_out, temp_path, core, job, db_path):
    sequence_typing_running = df_sra.loc[df_sra['ST'].isna()]
    sequence_typing_running = set(sequence_typing_running['Run'])
    resfinder_completed = set(db_resist['cenmigID'])
        
    spp = setting.species
    for sp in spp:
        query = sp + '*'
        species = sp.replace(' ', '_')
        df_spp = df_sra.loc[df_sra['Organism'].str.contains(query, case = False, regex = True)]
        
        task = generated_tuple_list_of_arg(sp, species, df_spp, sequence_typing_running, 
                                           resfinder_completed, core, 
                                           file_out , db_path, temp_path)
        
        pool = multiprocessing.Pool(job)
        try:
            pool.starmap(run_job_SRA, task)
        except Exception:
            pass
    print('SRA completed')
    
def process_fasta(df_fasta, db_resist,file_out, temp_path, core, job, db_path):
    sequence_typing_running = df_fasta.loc[df_fasta['ST'].isna()]
    sequence_typing_running = set(sequence_typing_running['asm_acc'])
        
    resfinder_completed = set(db_resist['cenmigID'])
    
    spp = setting.species
    for sp in spp:
        query = sp + '*'
        species = sp.replace(' ', '_')
        df_spp = df_fasta.loc[df_fasta['Organism'].str.contains(query, case = False, regex = True)]
        
        task = generated_tuple_list_of_arg(sp, species, df_spp, sequence_typing_running, 
                                           resfinder_completed, core, 
                                           file_out , db_path, temp_path)
        
        pool = multiprocessing.Pool(job)
        try:
            pool.starmap(run_job_fasta, task)
        except Exception:
            pass
    print('fasta completed')

def run_job_fasta(sp, species, df_i, sequence_typing_running, 
            resfinder_completed, core, 
            file_out, db_path,  temp_path):
    
    store_file = False # For marking file to move if not in db
    acc = df_i['asm_acc']
    
    try:
        # Check if have to run MLST or Resfinder
        if acc in sequence_typing_running:
            ST = True
        else:
            ST = False
        
        if not acc in resfinder_completed:
            res = True
        else:
            res = False
        
        if sp == 'Salmonella enterica':
            serovarRun = df_i['Predicted_serotype']
            serovarRun = pd.isna(serovarRun)
            if serovarRun:
                seqsero2 = True
            else:
                seqsero2 = False
        elif sp != 'Salmonella enterica':
            seqsero2 = False
    
        download_file = ST or res or seqsero2
        
        # path = temp_path + acc + '.fa'
        file_in_db = db_path + sp.replace(' ', '_') + '/fasta/' + acc + '.fa'
        
        # Check if there is file in DB
        if download_file:
            if not os.path.exists(file_in_db):
                ftp = df_i['ftp_path']
                ## Modified link
                link =ftp.rsplit('/', 1)
                ftp = ftp + '/' + link[1] + '_genomic.fna.gz'
                cmd = "wget -q " + ftp + ' -P ' + temp_path
                
                print(cmd)
                subprocess.run([cmd], shell=True )
                unzip_fa(temp_path + link[1] + '_genomic.fna.gz')
                # If file SEA is not available in data_sequences 
                ## Mark this file to move to cenmigDB_internal to storage
                store_file = True
            else:
                # Copy file to temp
                print('Copying ' + file_in_db + ' to ' + temp_path)
                shutil.copy(file_in_db, temp_path)
        
            ## MTB use this scheme name
            if sp == 'Mycobacterium tuberculosis':
                scheme = 'Mycobacteria spp'
            else:   
                scheme = sp        
            
            # Run mlst_check & Resfinder
            fasta_mlst = '/input/' + acc + '.fa'
            fasta_resfinder = temp_path + acc + '.fa'
            fasta_seqsero2 = temp_path + acc + '.fa'
            
            file_mlst_out = '/out/' + acc + 'mlst'  #### /input/ & /output/ for docker to specify path
            out_dir_resfinder = file_out + acc
            out_dir_seqsero2 = ' -d /data/home/tiravut.per/seqsero2_tmp/' + acc + '/'
            
            # CMD for run
            docker_volume_in = '-v "$(pwd)"/' + temp_path + ':/input'
            docker_volume_out =  '-v "$(pwd)"/' + file_out + ':/out'
            scheme = '-s "' + scheme + '"'
            cmd_mlst = 'docker run --rm -it ' + docker_volume_in + ' ' + docker_volume_out  + ' sangerpathogens/mlst_check get_sequence_type ' + scheme + ' -o ' + file_mlst_out + ' ' + fasta_mlst
            cmd_resfinder = 'python3 /data/apps/resfinder/resfinder/run_resfinder.py -o ' + out_dir_resfinder + ' -l 0.6 -t 0.8 -acq -ifq ' + fasta_resfinder
            cmd_seqsero2 = 'python3 /data/apps/seqsero2/seqsero-1.2.1/bin/SeqSero2_package.py -p 4 -t 4 -m k' + out_dir_seqsero2 + ' -i ' + fasta_seqsero2
    
            # Run MLST or Resfinder based on condition
            if ST:
                print(cmd_mlst)
                subprocess.run([cmd_mlst], shell = True)
            if res:
                print(cmd_resfinder)
                subprocess.run([cmd_resfinder], shell = True)
            if seqsero2:
                print(cmd_seqsero2)
                subprocess.run([cmd_seqsero2], shell = True)
            
            # Remove fasta after finished
            # cmd_rm_fasta = 'rm ' + temp_path + acc + '*.fa'
            # subprocess.run([cmd_rm_fasta], shell = True)
            
            # Moving file to dataport if not in data_sequences
            if store_file:
                try:
                    print('Moving ' + acc + '.fa to dataport' )
                    shutil.move(temp_path + acc + '.fa', setting.dataport)
                except:
                    print(temp_path + acc + '.fa' + ' Not found')
            else:
                os.remove(temp_path + acc + '.fa')
        else:
            pass
    except Exception:
        os.remove(temp_path + acc + '.fa')
        pass
    
def run_job_SRA(sp, species, df_i, sequence_typing_running, 
            resfinder_completed, core, 
            file_out, db_path,  temp_path = 'tmp_for_workflow/' ):
    
    
    store_file = False # For marking file to move if not in db
    run = df_i['Run']
    path = temp_path + run + '.sra'
    
    try:
        # Check if have to run MLST or Resfinder
        if run in sequence_typing_running:
            ST = True
        else:
            ST = False
        
        if not run in resfinder_completed:
            res = True
        else:
            res = False
            
        if sp == 'Salmonella enterica':
            serovarRun = df_i['Predicted_serotype']
            serovarRun = pd.isna(serovarRun)
            if serovarRun:
                seqsero2 = True
            else:
                seqsero2 = False
        elif sp != 'Salmonella enterica':
            seqsero2 = False
    
        download_file = ST or res or seqsero2
          
        path = temp_path + run + '.sra'
        file_in_db = db_path + sp.replace(' ', '_') + '/sra/' + run + '.sra'
        
        # Check if there is file in DB
        if download_file:
            if not os.path.exists(file_in_db):
                cmd = "prefetch -f yes -o " + path + ' ' + run
                print(cmd)
                subprocess.run([cmd], shell=True )
                
                # If file SEA is not available in data_sequences 
                ## Mark this file to move to cenmigDB_internal to storage
                store_file = True
            else:
                # Copy file to temp
                print('Copying ' + file_in_db + ' to ' + temp_path)
                shutil.copy(file_in_db, temp_path)
            
            # Run fastq-dump
            if df_i['Platform'] == 'PACBIO_SMRT':
                cmd_convert = 'fastq-dump ' + ' -O ' + temp_path  + ' ' + temp_path + run + '.sra'
            else:
                cmd_convert = 'fasterq-dump -e ' + str(core) + ' -O ' + temp_path  + ' ' + temp_path + run + '.sra'
            subprocess.run([cmd_convert], shell = True)
            
            ## MTB use this scheme name
            if sp == 'Mycobacterium tuberculosis':
                scheme = '-P ' + setting.mlst_db + 'Mycobacteria_spp/Mycobacteria_spp'
            else:   
                scheme = '-P ' + setting.mlst_db + species + '/' + species
                
            file_mlst_out = file_out + run + '.mlst'
            out_dir_resfinder = file_out + run
            out_dir_seqsero2 = ' -d /data/home/tiravut.per/seqsero2_tmp/' + run + '/'
            
            # Run stringMLST & Resfinder
            if df_i['LibraryLayout'] == 'SINGLE':
                fastq_mlst = '-1 ' + temp_path + run + '.fastq'
                fastq_resfinder = temp_path + run + '.fastq'
                fastq_seqsero2 = temp_path + run + '.fastq'
                
                cmd_mlst = 'stringMLST.py --predict -s ' + ' ' + scheme + ' -o ' + file_mlst_out + ' ' + fastq_mlst
                cmd_resfinder = 'python3 /data/apps/resfinder/resfinder/run_resfinder.py -o ' + out_dir_resfinder + ' -l 0.6 -t 0.8 -acq -ifq ' + fastq_resfinder
                cmd_seqsero2 = 'python3 /data/apps/seqsero2/seqsero-1.2.1/bin/SeqSero2_package.py -p 4 -t 3 -m k' + out_dir_seqsero2 + ' -i ' + fastq_seqsero2
    
            elif df_i['LibraryLayout'] == 'PAIRED':
                fastq_mlst = '-1 ' + temp_path + run + '_1.fastq ' + '-2 ' + temp_path + run + '_2.fastq'
                fastq_resfinder = temp_path + run + '_1.fastq ' + temp_path + run + '_2.fastq'
                fastq_seqsero2 = temp_path + run + '_1.fastq ' + temp_path + run + '_2.fastq'
                
                cmd_mlst = 'stringMLST.py --predict' + ' ' + scheme + ' -o ' + file_mlst_out + ' ' + fastq_mlst
                cmd_resfinder = 'python3 /data/apps/resfinder/resfinder/run_resfinder.py -o ' + out_dir_resfinder + ' -l 0.6 -t 0.8 -acq -ifq ' + fastq_resfinder
                cmd_seqsero2 = 'python3 /data/apps/seqsero2/seqsero-1.2.1/bin/SeqSero2_package.py -p 4 -t 2 -m k' + out_dir_seqsero2 + ' -i ' + fastq_seqsero2
    
            # Run MLST or Resfinder based on condition
            if ST:
                print(cmd_mlst)
                subprocess.run([cmd_mlst], shell = True)
            if res:
                print(cmd_resfinder)
                subprocess.run([cmd_resfinder], shell = True)
            if seqsero2:
                print(cmd_seqsero2)
                subprocess.run([cmd_seqsero2], shell = True)
                
            
            # Remove fastq after finished
            
            file_remove = glob.glob(temp_path + run + '*.fastq')
            print(file_remove)
            for file in file_remove:
                print(file)
                os.remove(file)
            
            # try:
            #     # cmd_rm_fastq = 'rm ' + temp_path + run + '*.fastq'
            #     # subprocess.run([cmd_rm_fastq], shell = True)
            #     file_remove = glob.glob(temp_path + run + '*.fastq')
            #     for file in file_remove:
            #         print(file)
            #         os.remove(file)
            # except:
            #     pass
            
            # Moving sra to dataport if not in data_sequences
            if store_file:
                try:
                    print('Moving ' + run + '.sra to dataport' )
                    shutil.move(temp_path + run + '.sra', setting.dataport)
                except:
                    print(temp_path + run + '.sra' + ' Not Found')
            else:
                os.remove(temp_path + run + '.sra')
        else:
            pass
    except Exception:
        os.remove(temp_path + run + '.sra')
        pass
# append_seqsero2_result()
# process_SEA_data()
