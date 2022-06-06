# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 15:51:36 2022

@author: Admin
"""

# #!/usr/bin/env python3  

#==================================== Load my packages =================================#

import pymongo
import setting
import util

#==================================== Load library =====================================#


import pandas as pd 
import json
import os
import shutil
import glob
import re
import datetime
import ftplib
import subprocess
import time
import numpy as np
# import time
import multiprocessing
import pycountry
import difflib
from dateutil.relativedelta import *
from pysradb.sraweb import SRAweb
# =============================================================================
#  NCBI Metadata
# =============================================================================

#### Download raw metadata
### SRA info
## Download through pysradb program

# def download_bioproject_list():
#     file_out = setting.metadata + '/bioproject_list.txt'
#     cmd_download = 'wget -O' + file_out + ' https://ftp.ncbi.nlm.nih.gov/bioproject/summary.txt'
#     subprocess.run([cmd_download], shell = True)
#     return file_out
   
# def listing_bioproject(bioproject_txt_file_dir, species = setting.species):
    
#     # Filter species
#     query_word = []
#     for s in species:
#         query = s + '*'
#         query_word.append(query)
#     query_word = '|'.join(query_word)
    
#     df = pd.read_table(bioproject_txt_file_dir, sep='\t')
#     df = df[['Organism Name', 'Project Accession', 'Project Data Type']]
#     df = df.dropna(subset=['Organism Name'], inplace=False)
#     df = df.dropna(subset=['Project Data Type'], inplace=False)
#     df = df.loc[df['Organism Name'].str.contains(query_word, case = False, regex =True)]    
#     df = df.loc[df['Project Data Type'].str.contains('Genome sequencing.*', case = False, regex =True)]
    
#     bioproject_set = set(df['Project Accession'])
#     return bioproject_set

# def download_srainfo_data(bioproject_set, file_out_dir = setting.metadata):
#     num = 0
#     num_all_file = str(len(bioproject_set))

#     for i in bioproject_set:
#         num = num + 1
#         file_out = file_out_dir + str(i) + '.srainfo'
#         if not os.path.exists(file_out):
#             cmd_download_bio = 'pysradb metadata "' + str(i) + '" --detailed  --expand --desc --assay --saveto ' + file_out
#             print(cmd_download_bio)
#             print( str(num) + ' from ' + num_all_file )
#             subprocess.run([cmd_download_bio], shell = True)
#             time.sleep(3)
#         else: 
#             pass
#     return file_out_dir

def diff_1month(start, last):
    return ((last.year - start.year) * 12 + last.month - start.month) //1

def cal_diff_1M():
    now_time = datetime.datetime.now()
    last_update_date = open('last_update_log.txt','r').readlines()[-1] # Read lastest 
    last_update_date = datetime.datetime.strptime(last_update_date, '%Y-%m-%d %H:%M:%S.%f')
    num_diff_month = diff_1month(last_update_date, now_time)
    return num_diff_month, now_time, last_update_date

def generated_date_for_query(num_diff_month, now_time, last_update_date):
    # Split every 1 month
    date_ls = []
    x = 0
    day1 = last_update_date
    while x < num_diff_month:
        day2 = day1 + relativedelta(months=+3)
        add_list = [day1, day2]
        day1 = day2
        x = x+1
        date_ls.append(add_list)
        
    ## Modified last date
    date_ls[-1][1] = now_time
    return date_ls

def generated_query(date_ls, query_list = setting.search_text):
    search_text_list = []
    # Gen query
    for i in date_ls:
        day1 = i[0].strftime("%Y/%m/%d")
        day2 = i[1].strftime("%Y/%m/%d")
        for word in query_list:
            datetext =  ' AND ("$LAST_UPDATE_DATE"[Modification Date] : "$CURRENT_DATE"[Modification Date])'
            datetext = datetext.replace('"$LAST_UPDATE_DATE"', '"' + day1 + '"')
            datetext = datetext.replace('"$CURRENT_DATE"', '"' + day2 + '"')
            query = word + datetext
            search_text_list.append(query)
            
    ## Generate date start stop for loop through each SRP
    start = date_ls[0][0].strftime("%Y/%m/%d")
    stop = date_ls[-1][1].strftime("%Y/%m/%d")
    date_srp =  ' AND ("$LAST_UPDATE_DATE"[Modification Date] : "$CURRENT_DATE"[Modification Date])'
    date_srp = date_srp.replace('"$LAST_UPDATE_DATE"', '"' +  start + '"')
    date_srp = date_srp.replace('"$CURRENT_DATE"', '"' +  stop + '"')
    
    return search_text_list, date_srp

def get_ncbi_sradata(search_text_list):
    df_main = pd.DataFrame()
    for srp in search_text_list:
        print('\n' + srp + '\n')
        db = SRAweb()
        df = db.sra_metadata(srp, detailed = True, 
                          sample_attribute = True,
                          expand_sample_attributes=True,
                          assay = True)
        df_main = pd.concat([df_main, df], ignore_index= True)
        time.sleep(10)
    df_x = change_colnames(df_main)
    df_x = combine_colnames_ignorecase(df_x)
    df_x1 = df_x.drop_duplicates()
    df_x1.reset_index(drop=True, inplace=True)
    
    return df_x1

def retrive_data_ncbi_sra():
    num_diff_month, now_time, last_update_date = cal_diff_1M()
    date_ls = generated_date_for_query(num_diff_month, now_time, last_update_date)
    search_text_list, date_srp = generated_query(date_ls, query_list = setting.search_text)
    df = get_ncbi_sradata(search_text_list)
    return df, now_time

def download_srainfo_data_multi(bioproject_set, file_out_dir = setting.metadata, job = 3):
    task = generated_tuple_list_of_arg(bioproject_set, file_out_dir)
        
    pool = multiprocessing.Pool(job)
    pool.starmap(run_download_srainfo, task)
    

def generated_tuple_list_of_arg(bioproject_set, file_out_dir):
    # Create tuple of arg for run multiprocess task
    out_ls = []
    for i in bioproject_set:
        bio_i= str(i)
        tuple_i = (bio_i, file_out_dir)
        out_ls.append(tuple_i)
    return out_ls

def run_download_srainfo(bioproject_acc, file_out_dir):
    file_out = file_out_dir + str(bioproject_acc) + '.srainfo'
    
    if not os.path.exists(file_out):
        cmd_download_bio = 'pysradb metadata "' + str(bioproject_acc) + '" --detailed  --expand --desc --assay --saveto ' + file_out
        print(cmd_download_bio)
        subprocess.run([cmd_download_bio], shell = True)
    else: 
        # print(file_out + ' is existed')
        pass
    
def retrive_bioproject(filepath = setting.sra_path):
    # col_list = setting.df_colnames
    # df = pd.DataFrame(columns=['BioProject'])
    file_all = glob.glob(filepath)
    df_ls = []
    for i in file_all:
        print(i)
        df_i = pd.read_csv(i,low_memory=False)
        df_ls.append(df_i)
    
    df = pd.concat(df_ls, ignore_index= True)
    bioproject_set = set(df['BioProject'].dropna())
    
    df1 = df[['Run','Assay Type','Organism', 'Center Name', 'BioProject', 'BioSample',
             'Experiment', 'Library Name', 'LibraryLayout','LibrarySelection', 
             'LibrarySource', 'Platform', 'Instrument']]
    
    return bioproject_set, df

def check_srainfo_available(bioproject_set, filepath = setting.metadata):
    
    file_all = glob.glob(setting.metadata + '*.srainfo*')
    ls = []
    for i in file_all :
        word = i.rsplit('/', 1)[1]
        word = re.sub('.srainfo.*', '', word)
        ls.append(word)
    bioproject_set = set(bioproject_set) - set(ls)
    
    return bioproject_set

def check_missing(bioproject_set):
    print('Checking if any missing Bioproject')
    file_all = glob.glob(setting.metadata + '*.srainfo*')
    ls = []
    for i in file_all :
        word = i.rsplit('/', 1)[1]
        word = re.sub('.srainfo.*', '', word)
        ls.append(word)
    miss = set(bioproject_set) - set(ls)
    if len(miss) > 0 :
        print(' OR '.join(miss))
        print('Manual download these bioproject acc and save as $BIOPROJECT.srainfofix (tab-delim)')
        try:
            print('Process manu')
            if input('Go next ? : (Y/N)').upper() == 'Y':
                pass
        except:
            print('Stop')
            CANCEL
    # # Download again if missing
    # if len(miss) > 0 :
    #     download_srainfo_data_multi(miss, file_out_dir = setting.metadata)
    #     file_all = glob.glob(setting.metadata + '*.srainfo')
    #     ls = []
    #     for i in file_all :
    #         word = i.rsplit('/', 1)[1]
    #         word = word.replace('.srainfo', '')
    #         ls.append(word)
    #         miss_1 = set(bioproject_set) - set(ls)
    #     if len(miss_1) > 0 :
    #         print(' OR '.join(miss_1))
    #         print('Search these in SRA database and savs file.srainfo')
    # elif len(miss) == 0 :
    #     pass

### AMR PathoDB metadata Download

def ftp_connect():
    FTP_HOST = "ftp.ncbi.nlm.nih.gov"
    FTP_USER = 'anonymous'
    FTP_PASS = '@anonymous'
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS, timeout=6000)
    return ftp

def ftp_species_checking(ftp, path,spp = setting.species_ncbi):
    # path = 'pathogen/Results/'
    ftp.cwd(path) # Change path
    ncbi_spp_ls = pd.Series(ftp.nlst()) # Return list of species available
    
    interest_spp = list()
    for i in spp:
        sp = i + '*'
        sp = sp.replace(' ', '_')
        match_spp = list(ncbi_spp_ls.loc[ncbi_spp_ls.str.contains(sp,case = False, regex = True)])
        
        if len(match_spp) == 0 :
            miss = sp.split('_')[0]
            match_miss_spp = list(ncbi_spp_ls.loc[ncbi_spp_ls.str.contains(miss, case = False, regex = True)])
            
            # Check if can find missing spp
            if len(match_miss_spp) == 1 :
                interest_spp.append(match_miss_spp)
            elif len(match_miss_spp) > 1 :
                print('Found ' + miss + ' more than one matched folder names')
                print(match_miss_spp)
                break
            elif len(match_miss_spp) == 0 :
                pass
        
        elif len(match_spp) == 1 : # Match 1 then add
            interest_spp.append(match_spp)
        
        elif len(match_spp) > 1 :
             print('Found ' + sp + ' more than one matched folder names')
             print(match_spp)
             break
        
        ## Flattening list (str.contain output in list)
        interest_spp_flat_list = []
        for sublist in interest_spp:
            for item in sublist:
                interest_spp_flat_list.append(item)
    
    return interest_spp_flat_list
    

def wget_pathoDB_file(sp, pathogen_version, file_out):
     # Download PathogenDB
     pathogen_ftp = 'https://ftp.ncbi.nlm.nih.gov/pathogen/Results/' + sp + '/' + pathogen_version + '/Metadata/' + pathogen_version + '.metadata.tsv'
     print('Downloading ' + pathogen_ftp)
     cmd_pathogen = "wget -q " +  pathogen_ftp + ' -P ' + file_out
     download_pathogen = subprocess.run([cmd_pathogen], shell=True )
     # os.rename(file_out + 'assembly_summary.txt', sp + '_assembly_summary.txt' )
     
def wget_assemblyDB_file(sp, file_out):
    # Download assembly
    assembly_ftp = 'https://ftp.ncbi.nlm.nih.gov/genomes/.vol2/genbank/bacteria/' + sp + '/assembly_summary.txt'
    print('Downloading ' + assembly_ftp)
    cmd_assembly = "wget -q " +  assembly_ftp + ' -P ' + file_out
    download_pathogen = subprocess.run([cmd_assembly], shell=True )
    os.rename(file_out + 'assembly_summary.txt', sp + '_assembly_summary.txt' )
    
def download_pathoDB(file_out):
    # Connect to check species available
    ftp = ftp_connect()
    patho_ls = ftp_species_checking(ftp, path = 'pathogen/Results/', spp = setting.species)
    
    # Connect to check version & Download
    for sp in patho_ls:
        ftp = ftp_connect()
        path = 'pathogen/Results/' + sp
        ftp.cwd(path) # Change path
        file_ls = pd.Series(ftp.nlst())
        file_ls = file_ls[file_ls.str.contains('^P.*\d$', case = True, regex =True)]
        lastest_ver = [int(i.rsplit('.', 1)[1]) for i in file_ls] # Find lastest ver ## Spilt version and convert to int
        lastest_ver = max(lastest_ver) # Select max
        pathogen_version = list(file_ls[file_ls.str.contains(str(lastest_ver), case = True, regex =True)]).pop()
        print(pathogen_version)
        
        # Download file
        wget_pathoDB_file(sp, pathogen_version, file_out)

def download_assemblyDB(file_out):
    ftp = ftp_connect()
    assembly_spp_ls = ftp_species_checking(ftp, path = '/genomes/.vol2/genbank/bacteria/', spp = setting.species)
    
    # Download assembly metadata file
    for i in assembly_spp_ls:
        wget_assemblyDB_file(i, file_out)

def download_pathoDB_AssemblyDB_metadata(file_out = setting.metadata):
    download_pathoDB(file_out)
    download_assemblyDB(file_out)
    tsv_to_csv()
    
# =============================================================================
#  Generated metadata (NCBI)
# =============================================================================

#### SRA section ####
def merge_srainfo(file_in_dir):
    col_list = setting.df_colnames
    # df = pd.DataFrame()
    file_all = glob.glob(file_in_dir + '*.srainfo')
    df_ls = []
    for i in file_all:
        #print(str(i))
        bio = i.rsplit('/',1)[1]
        bio = bio.replace('.srainfo', '')
        df_i = pd.read_table(i,low_memory=False, sep = '\t')
        df_i.insert(1, 'bioproject', bio)
        df_i = change_colnames(df_i)
        df_i = combine_colnames_ignorecase(df_i)
        
        df_ls.append(df_i)
    
        # # Select column
        # df = pd.concat([df, df_i])
        # df.reset_index(drop=True, inplace=True)
    df = pd.concat(df_ls, ignore_index=True)
    df.drop_duplicates(subset = ['Run'] ,keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True) # Reset index before filterout
    # If any manual edit
    try:
        df = merge_srainfofix(df, file_in_dir)
    except:
        pass
    #df.to_csv('df_raw.csv', index = False)
    return df

def merge_srainfofix(df_old, file_in_dir):
    col_list = setting.df_colnames
    # df = pd.DataFrame()
    file_all = glob.glob(file_in_dir + '*.srainfofix')
    df_ls = []
    for i in file_all:
        #print(str(i))
        df_i = pd.read_table(i,low_memory=False, sep = '\t')
        df_i = combine_colnames_ignorecase(df_i)
        df_ls.append(df_i)
    
        # # Select column
        # df = pd.concat([df, df_i])
        # df.reset_index(drop=True, inplace=True)
        
    df = pd.concat(df_ls, ignore_index=True)
    df.drop_duplicates(subset = ['Run'] ,keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True) # Reset index before filterout
    
    df = pd.concat([df_old, df], ignore_index= False)
    df.drop_duplicates(subset = ['Run'] ,keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True) # Reset index before filterout
    #df.to_csv('df_raw.csv', index = False)
    return df


    
def change_colnames(df):
    # Change to lower case first
    # print('Changing colnames')
    old_col = df.columns
    ls = []
    for col in old_col:
        new = col.lower()
        ls.append(new)
    df.columns = ls
    
    # Then change base on dict
    change_dict = {'run' : 'run_accession',            
                'experiment' : 'experiment_accession',
                'organism' : 'organism_name',
                'assay type' : 'library_strategy' ,
                'librarysource' : 'library_source' ,
                'libraryselection' : 'library_selection',
                'librarylayout' : 'library_layout',
                'platform' : 'instrument_model_desc',
                'instrument' : 'instrument_model',
                'center name' : 'collected_by'
                }
    # invert key , Value
    change_dict = {v: k for k, v in change_dict.items()}
    
    df = df.rename(change_dict, axis='columns')
    
    return df

def drop_non_interest_species(df):
    print('Drop non interest species')
    df_old = df.copy()
    df = df.dropna(subset = ['Organism'], inplace = False)
    spp = setting.species_ncbi

    regex_spp = []
    for i in spp:
        i = i.replace(' ','.')
        i = '^' + i + '.*'
        regex_spp.append(i)
    regex_spp = '|'.join(regex_spp)

    df = df.loc[df['Organism'].str.contains(regex_spp, case=False)]
    df_filter_out = df_old.drop(df.index)
    df_filter_out.to_csv('non_interest_spp', index = False)
    # Drop dup
    df = df.drop_duplicates(keep='first', inplace=False) 
    df.reset_index(drop=True, inplace=True) # Reset index after filterout
    return df, df_filter_out

def combine_colnames_ignorecase(df, column_dict = setting.combine_columns_dict):
    print('\n Combined column with same name \n')
    df_out = pd.DataFrame()
    regex_list = []
    for key in column_dict.keys():
        value = column_dict[key]
        #print(value)
        value = set(value)
        
        for i in value: 
            i = '^' + i + '$'
            i = i.replace('_', '.*?')  ## ? mean there is one or none
            i = i.replace(' ', '.')
            i = i.replace('(', '.')
            i = i.replace(')', '.')

            regex_list.append(i)
            
    # Join regex to one str
    regex_query = '|'.join(regex_list)
    # print(regex_query)
    
    # Convert to series for str.contains method
    df_colname = df.columns
    col_series = pd.Series(df_colname)
    col_series = col_series[col_series.str.contains(regex_query, case = False, regex = True)]
    # Extract all column that in dict value
    df_x = df[list(set(col_series))]
    df_x.fillna('', inplace=True)
    
    col_df_x = df_x.columns
    # Merge col in each key into one col
    for col in column_dict.keys():
        rex = []
        col_list = column_dict[col]
        for i in col_list:
            i = '^' + i + '$'
            i = i.replace('_', '.*?')
            i = i.replace(' ', '.')
            i = i.replace('(', '.')
            i = i.replace(')', '.')

            rex.append(i)
        rex_query = '|'.join(rex)
        # print('\n')
        # print(rex_query)
       
        
        combine_target = col_df_x[col_df_x.str.contains(rex_query, case = False, regex =True)]
        
        ## Hard code since host*age can match hostXXXXstage
        if str(col) == 'host_age':
            combine_target = combine_target[~combine_target.str.contains('.*stage$',
                                                case = False, regex =True)]
        
        combine_target = list(combine_target)
        # print(str(col))
        # print(combine_target)
        
       
        df_out[str(col)] = df_x[combine_target].astype(str).agg(lambda x: ' '.join(x.unique()), axis=1) 
    
    df_out = df_out.replace(to_replace= r'\\', value= '', regex=True)
    
    return df_out
 
def guess_country_name(data, country_names = [x.name.upper() for x in pycountry.countries]):
    # https://stackoverflow.com/questions/15377832/pycountries-convert-country-names-possibly-incomplete-to-countrycodes/15378790
      
    match_countries = difflib.get_close_matches(data.upper(), country_names)
    
    if len(match_countries) > 0:
        return match_countries[0]
    elif len(match_countries) == 0:
        return np.nan
def dict_for_correct_country(countryname_col):
    countryname_set = set(countryname_col)
    country_names = [x.name.upper() for x in pycountry.countries]  
    
    country_dict = {}
    for data in countryname_set:
        if type(data) != float:
            match_countries = difflib.get_close_matches(data.upper(), country_names)
            if len(match_countries) > 0:
                match = match_countries[0]
            elif len(match_countries) == 0:
                match = 'Missing'
            
            # Fix with hard code
            if 'KOREA' in data.upper():
                match = 'KOREA, REPUBLIC OF'
            elif 'LAOS' in data.upper():
                match = "LAO PEOPLE'S DEMOCRATIC REPUBLIC"
            elif 'TAIWAN' in data.upper():
                match = 'TAIWAN, PROVINCE OF CHINA'
            elif 'SYRIA' in data.upper():
                match = 'SYRIAN ARAB REPUBLIC'
            elif 'TANZANIA' in data.upper():
                match = 'TANZANIA, UNITED REPUBLIC OF'
            elif 'RUSSIA' in data.upper():
                match = 'RUSSIAN FEDERATION'
            elif 'VENEZUELA' in data.upper():
                match = 'VENEZUELA, BOLIVARIAN REPUBLIC OF'
            elif 'PALESTINE' in data.upper():
                match = 'PALESTINE, STATE OF'
            elif 'SPAIN' in data.upper():
                match = 'SPAIN'
            
            
            
            add_dict = {str(data) : str(match)}
            country_dict.update(add_dict)
    
    return country_dict

def stamp_update_time(update_time):
    # Open a file with access mode 'a'
    file_object = open('last_update_log.txt', 'a')
    # Append 'hello' at the end of file
    file_object.write('\n' + str(update_time))
    # Close the file
    file_object.close()
        
def create_metadata_ncbi(file_out = 'cenmigDB_metadata.csv'):
    ## Get bioproject and df['Run']
    run_old, cenmigID_old, asmacc_old = retrive_id_from_database()
    
    # OLD
    bioproject_set, df1 = retrive_bioproject(filepath = setting.sra_path)
    # df_spp, df_dropout = drop_non_interest_species(df1)
    
    # Download raw via pysradb
    bioproject_set = check_srainfo_available(bioproject_set, filepath = setting.metadata)
    download_srainfo_data_multi(bioproject_set, file_out_dir = setting.metadata)
    check_missing(bioproject_set)
    
    ## Concat srainfo (This func because data shift if data from SRA run selector)
    df2 = merge_srainfo(file_in_dir= setting.metadata)
    df_sra, df_dropout = drop_non_interest_species(df2)
    # Add BioSample and BioProject
    df_sra = df_sra.merge(df1[['Run', 'BioSample']], how ='left', on='Run')
    df_sra = df_sra.drop_duplicates(subset =['Run'])
    # df_sra.to_csv('db.csv', index = False)

    ## NEW
    # df, update_time = retrive_data_ncbi_sra()
    # df_sra, df_dropout = drop_non_interest_species(df)
    
    # Remove data which already in DB
    df_sra = df_sra.loc[~df_sra['Run'].isin(run_old)]
    
    print('\nSRA part successed ...\n')
    
    # Create AMR data
    df_amr, df_amr_filterout = merge_AMR_data()
    
    ## remove Run & asm_acc which have in DB
    df_amr = df_amr.loc[~df_amr['Run'].isin(run_old)]
    df_amr = df_amr.loc[~df_amr['asm_acc'].isin(asmacc_old)]
    
    # Clean AMR
    df_amr_clean, df_amr_waste = clean_AMR_data(df_amr)
    
    print('\nAMR part successed ...\n')
    # Check if missing data in df_amr
    df_sra_completed = check_sra_missing_data(df_sra, df_amr_clean)
    
    # Match asm_acc with SRA id
    df_sra_asm_acc, df_file_dup = match_ASMacc_to_SRA(df_sra_completed, df_amr_clean)
    
    # Exact not match asm_acc but has AMR genotypes data
    df_asm_notmatch = exact_ASMacc_notmatch(df_sra_asm_acc, df_amr_clean)
    
    # Append df_file df_asm_notmatch
    # df_file = df_sra_asm_acc.append(df_asm_notmatch, ignore_index=True)
    df_file = pd.concat([df_sra_asm_acc, df_asm_notmatch], ignore_index=True)
    df_file.reset_index(drop=True, inplace=True)
    
    # Add ftp_path to asm_acc
    df_wgs, df_wgs_waste = merge_and_clean_WGS_assembly_data()
    df_file, df_ftp_drop = add_ftp_path_of_fasta(df_file, df_wgs)
    
    print('\nWGS part successed ...\n')
    
    # Fix country name
    df_file['geo_loc_name_country'] =  df_file['geo_loc_name_country'].apply(split_semicolon)
    df_file['geo_loc_name_country_fix'] = df_file['geo_loc_name_country'].replace({'\d+': np.nan, 'nan': np.nan, 
                                                                               'USA.*' : 'United states',
                                                                               'United Kingdom.*': 'United Kingdom',
                                                                               'Brazil.*': 'Brazil',
                                                                               'Australia,*' : 'Australia'}, 
                                                                              regex=True)
    
    correct_dict = dict_for_correct_country(countryname_col = df_file['geo_loc_name_country_fix'])
    
    df_file['geo_loc_name_country_fix'] = df_file['geo_loc_name_country_fix'].replace(correct_dict)
    # Match Country with UNGEO subregion
    df_file = ungeo_subregion(df_file)
    
    # Add column & cenmigID
    df_file = cenmigID_assigner(df_file)
    
    ## remove cenmigID which has in DB
    df_file = df_file.loc[~df_file['cenmigID'].isin(cenmigID_old)]
    
    df_file['geo_loc'] = df_file['geo_loc_name_country'].astype(str) + '; ' + df_file['geo_loc_name_country_continent'].astype(str)
    # df_file[['genome_material','other_attributes','clinical_data','demograhpic_data', 'sequence_typing'] ]= None
    
    df_file.drop_duplicates(inplace = True)
    
    ordered_colnames = ['cenmigID', 'Run', 'asm_acc', 'Sample_Name', 'BioProject', 'BioSample', 'Experiment', 
                    'Organism', 'Serovar', 'AMR_genotypes', 
                    'isolation_source', 'host', 'host_disease', 'host_disease_stage', 'host_age', 'host_sex', 
                    'Collection_date', 'Center_Name', 
                    'geo_loc', 'geo_loc_name_country', 'geo_loc_name_country_fix',  'geo_loc_name_country_continent', 'lat_lon', 'sub_region', 
                    'Assay_Type', 'LibrarySource', 'LibrarySelection', 'LibraryLayout', 'Platform', 'Instrument', 
                    'ftp_path']
    
    # Check data number
    if len(df_sra_asm_acc) + len(df_asm_notmatch) == len(df_file):
        df_file = df_file[ordered_colnames]
        print('\nPrepaare to write metadata csv file ...\n')
        df_file.to_csv(file_out, index= False)
        print('Finished')
        # stamp_update_time(update_time)
    else:
        print('UNGEO match error')


### AMR 
def merge_AMR_data(filepath = setting.amr_path):
    df_amr = pd.DataFrame()
    file_amr = glob.glob(filepath)
    for i in file_amr:
        df_i = pd.DataFrame
        print(str(i))
        #df_i = pd.read_csv(i,low_memory=False, converters={'bioproject_acc' : str,'biosample_acc' : str,'scientific_name' : str})
        df_i = pd.read_csv(i, low_memory=False)
        df_amr = pd.concat([df_amr,df_i])
    # Remove duplicate in data
    df_amr.reset_index(drop=True, inplace=True)
    df_amr_old = df_amr.copy()
    df_amr = df_amr.drop_duplicates(keep= 'first', inplace=False)
    
    spp = setting.species
    regex_query = []
    for sp in spp:
        sp = sp.replace(' ', '.')
        sp = sp + '*'
        regex_query.append(sp)
    regex_query = '|'.join(regex_query)
    df_amr= df_amr[df_amr['scientific_name'].str.contains(regex_query, case=False)]
    
    df_amr_filterout = df_amr_old.drop(df_amr.index)
    df_amr.reset_index(drop=True, inplace=True)
    return df_amr, df_amr_filterout

def clean_AMR_data(df_amr):
    df_amr_old = df_amr.copy()

    # # remove row which has data in column error
    # amr_col = list(df_amr.columns)
    # error_col = []
    # for i in range(0,len(amr_col),1):
    #     col = amr_col[i]
    #     if 'Unnamed' in col:
    #         e = str(col)
    #         print(e)
    #         error_col.append(e)
    #     else:
    #         pass
    # ls_er = []
    # for j in error_col:
    #     print(j)
    #     er = df_amr[df_amr[j].notna()]
    #     err = er.index.to_list()
    #     ls_er = ls_er + list(set(err))
    # ls_er = list(set(ls_er))

    # # drop with index
    # df_amr_error = df_amr[df_amr.index.isin(ls_er)]
    # df_amr = df_amr.drop(df_amr_error.index)  
    
    # Check pattern bioproject biosample
    df_amr = df_amr.loc[df_amr['bioproject_acc'].str.contains("PR", case=False)]
    df_amr = df_amr.loc[~df_amr['biosample_acc'].isna()]
    df_amr = df_amr.loc[df_amr['biosample_acc'].str.contains("SA", case=False)]

    # Column with int value
    #convert to numeric with parameter errors='coerce' 
    #the ones cannot be converted will be NaN.
    int_col = ['taxid', 'species_taxid','number_drugs_resistant','number_drugs_intermediate',
               'number_drugs_susceptible','number_drugs_tested', 
               'number_amr_genes', 'number_virulence_genes', 'amrfinder_applied',
               'number_stress_genes']

    # loop convert column value to int and fill NaN if cant
    for i in int_col:
        print('convert ' + str(i) + ' to int' )
        df_amr[i] = pd.to_numeric(df_amr[i], errors='coerce')
        
    for i in int_col:
        df_amr = df_amr.dropna(subset=[i], inplace=False)
    
    df_amr_drop = df_amr_old.drop(df_amr.index)
    #print(df_amr_drop)
    return df_amr, df_amr_drop

def match_ASMacc_to_SRA(df_sra, df_amr_clean):
    start = len(df_sra)
    # ASM
    sra_run = df_sra[['Run', 'BioSample']]
    amr_run_asm = df_amr_clean[['asm_acc','bioproject_acc', 'biosample_acc' ]]
    # amr_run_asm = amr_run_asm.dropna(subset=['asm_acc'], inplace=False)
    # amr_run_asm = amr_run_asm.dropna(subset=['Run'], inplace=False)
    
    # df which data from df and amr has both Run and asm_acc
    run_asm_match = pd.merge(sra_run, amr_run_asm, left_on = ['BioSample'], right_on=['biosample_acc'] ,indicator=False)
    run_asm_match = run_asm_match.drop_duplicates(keep='first', inplace=False)
    
    # Match asm
    df_sra = pd.merge(df_sra, run_asm_match[['BioSample', 'asm_acc']], on = ['BioSample'], how = 'left', indicator=False)
    df_sra = df_sra.drop_duplicates()
       
    # AMR
    amr = df_amr_clean.dropna(subset=['biosample_acc'], inplace=False)
    amr_col_list = ['biosample_acc','AMR_genotypes' ]
    amr       = amr[amr_col_list]
    amr.drop_duplicates(inplace=True)
    
    df_sra = pd.merge(df_sra, amr, left_on=['BioSample'], right_on=['biosample_acc'], how='left', indicator=False)
    df_sra = df_sra.drop_duplicates(subset=['Run'])
    del(df_sra['biosample_acc'])
    df_dup = df_sra.loc[df_sra.duplicated(subset=['Run']), :]
    
    end = len(df_sra)
    
    if start == end:
        return df_sra, df_dup
    else: 
        print('\nAMR data error')
        print('Please check')
        return df_sra, df_dup

def exact_ASMacc_notmatch(df_sra_clean, df_amr_clean):
    ### Old 
    #amr_drug = df_amr_clean.dropna(subset=['AMR_genotypes'], inplace= False)
    
    ### 23/05/2022 kept all asm not just asm with AMR
    amr_drug = df_amr_clean.copy()
    amr_drug_col_list = ['asm_acc','bioproject_acc','biosample_acc',
                        'collected_by','collection_date', 'geo_loc_name',
                        'host','host_disease', 'isolation_source',
                        'lat_lon','scientific_name', 'serovar',
                        'AMR_genotypes', 'Platform' ]
    amr_drug = amr_drug[amr_drug_col_list]
    amr_drug.columns = ['asm_acc','BioProject','BioSample'
                       ,'Center_Name','Collection_date', 'geo_loc_name_country',
                       'host','host_disease', 'isolation_source',
                       'lat_lon','Organism', 'Serovar',
                       'AMR_genotypes', 'Platform' ]
    amr_drug.dropna(subset = ['asm_acc'], inplace=True)
    amr_drug.drop_duplicates(inplace = True)
    
    df_asm_match = df_sra_clean[['asm_acc']].dropna()
    asm_notmatch = amr_drug[~amr_drug['asm_acc'].isin(df_asm_match['asm_acc'])]
    asm_notmatch.dropna(subset=['asm_acc'],inplace=True)
    asm_notmatch.drop_duplicates(subset=['asm_acc'], inplace =True)
    
    return asm_notmatch

def check_sra_missing_data(df_sra_nodup, df_amr_clean):
    run_sra = df_sra_nodup[['Run']]
    run_amr = df_amr_clean[['Run', 'scientific_name']]
    run_sra = run_sra.dropna(subset = ['Run'], inplace = False)
    run_amr = run_amr.dropna(subset = ['Run'], inplace = False)
    
    #
    run_missing =  set(run_amr['Run']) - set(run_sra['Run'])
    print('\nFound SRA missing ' + str(len(run_missing)) + '\n')
    text = ''
    if len(run_missing) > 0:
        text = ' OR '.join(run_missing)
        print('\nPlease search this SRA and save to SRA run info table with SRA run selector\n')
        print(text)
        filepath = str(input('Filepath of SRAinfo missing : '))
        
        # Append to clean data
        df_fix = pd.read_csv(filepath, low_memory=False)
        df_fix = combine_colnames_ignorecase(df_fix)
        df_fix_clean = df_fix.drop_duplicates(subset = ['Run'])
        df_fix_clean, df_dropout = drop_non_interest_species(df_fix_clean)
            
        # Append missing data
        df_sra_clean_full = pd.concat([df_sra_nodup, df_fix_clean], ignore_index = True)
        df_sra_clean_full.reset_index(drop=True, inplace=True)
        return df_sra_clean_full
    else:
        return df_sra_nodup
    
### WGS 
def merge_and_clean_WGS_assembly_data(filepath = setting.assembly_path):
    df = pd.DataFrame()
    file_all = glob.glob(filepath)
    for i in file_all:
        print(str(i))
        df_i = pd.read_table( i, header = 1, low_memory=False) #Skip first row
        df = pd.concat([df, df_i], ignore_index= False)
    
    df = df[['# assembly_accession', 'ftp_path']]
    df.columns = ['asm_acc', 'ftp_path']
    df.reset_index(drop=True, inplace=True)
    
    df_old = df.copy()
    # Check pattern asm_acc and ftp_path
    df = df.drop_duplicates(keep='first', inplace=False) 
    df.reset_index(drop=True, inplace=True)
    df = df.loc[df['asm_acc'].str.contains("GCA", case=False)]
    df = df.loc[df['ftp_path'].str.contains("^.*//ftp.ncbi.nlm.nih.gov/genomes/all/*", case=False)]
    dup =  df.loc[df.duplicated(subset=['asm_acc']), :]
    dup = list(dup['asm_acc'].unique())
    df_dup = pd.DataFrame()
    for asm in dup:
        df_i = df[df['asm_acc'] == str(asm)]
        # df_dup = df_dup.append(df_i)
        df_dup = pd.concat([df_dup, df_i], ignore_index= False)
    if len(df_dup) > 0 :
        print('Found duplicates in WGS data')
        print(df_dup)
        drop = True
        while drop:
            index = input('Remove index no.(stop by type N) :')
            if str(index).upper() != 'N':
                dropindex = int(index)
                df.drop(dropindex, inplace=True)
                print('Drop ' + str(index) + ' From df_sra')
            else:
                drop = False
    else:
        pass

    df.drop_duplicates(inplace= True)
    df_drop = df_old.drop(df.index)
    
    df.reset_index(drop=True, inplace=True)
    return df, df_drop

def add_ftp_path_of_fasta(df_file, df_wgs):
    df_file_old = df_file.copy()
    df_file = pd.merge(df_file, df_wgs,
                       on = ['asm_acc'],
                       how = 'left',
                       indicator=False)
    df_file = df_file.drop_duplicates(inplace = False)
    df_file.reset_index(drop=True, inplace=True)
    
    df_ftp_dup = df_file.drop(df_file_old.index)
    return df_file, df_ftp_dup

# =============================================================================
# Common Func
# =============================================================================
def tsv_to_csv(filepath = setting.metadata):
    file = glob.glob(filepath + '*.tsv')
    for tsv in file:
        df = pd.read_table(tsv, sep='\t')
        csv_filename = tsv.replace('.tsv','.csv')
        df.to_csv(csv_filename, index = False)
        os.remove(tsv)

def retrive_id_from_database():
    dbCollection = util.connect_database()
    #data = from_input_to_dict()
    #data = dbCollection.aggregate([{'$match': data }])
    print('Connecting to CenmigDB and retrive old record id ...')
    # Incase first time run
    try:
        data = dbCollection.find({}, {'Run' : True, 'asm_acc' : True, 'cenmigID' : True})
        data = pd.DataFrame.from_dict(data)
        data = data[['Run', 'cenmigID', 'asm_acc']]
        cenmigID_old = data['cenmigID'].dropna()
        run_old = data['Run'].dropna()
        asmacc_old = data['asm_acc'].dropna()
        print('Retriving completed')
    except:
        print('Cant find Database and retrive old id')
        cenmigID_old = list()
        run_old = list()
        asmacc_old = list()
        
    return list(run_old), list(cenmigID_old), list(asmacc_old)

def cenmigID_assigner(df):
    df_cenmig = df[['Run', 'asm_acc', 'Sample_Name', 'Center_Name']]
    df_cenmig['Inhouse'] = df_cenmig['Sample_Name'] + '@' + df_cenmig['Center_Name'] + '@Submitdate' + datetime.date.today().strftime('%d%m%Y')
    df_cenmig = df_cenmig[['Run', 'asm_acc', 'Inhouse']]
    df_cenmig.fillna('REMOVE', inplace=True)
    ## Retrive index from different criteria
    index_run = df_cenmig[['Run']].loc[df_cenmig['Run'].str.contains('RR\d', case =  True)]
    index_asm = df_cenmig[['asm_acc']].loc[df_cenmig['asm_acc'].str.contains('GCA*', case =  True)]
    # Find intersect index
    dropindex = index_run.index.intersection(index_asm.index)
    index_asm.drop(dropindex, inplace=True)
    # Index for Inhouse
    index_runasm = index_run.index.union(index_asm.index)
    index_inhouse = df_cenmig.index.difference(index_runasm)
    index_inhouse = df_cenmig[['Inhouse']].loc[index_inhouse]
    # Get value & Change colnames for concat df
    cenmig_run = df_cenmig[['Run']].loc[index_run.index]
    cenmig_run.columns = [['cenmigID']]
    cenmig_asm = df_cenmig[['asm_acc']].loc[index_asm.index]
    cenmig_asm.columns = [['cenmigID']]
    cenmig_inhouse = df_cenmig[['Inhouse']].loc[index_inhouse.index]
    cenmig_inhouse.columns = [['cenmigID']]
    # Concat df and sort with index
    cenmigID = pd.concat([cenmig_asm, cenmig_run, cenmig_inhouse]).sort_index()
    # Covert df to series to add with df_file
    cenmigID = cenmigID.squeeze()
    df.insert(0, 'cenmigID', cenmigID)
    return(df)

def split_semicolon(i):
    if type(i) == str:
        if ':' in i:
            txt = i.split(':')[0]
        elif ' :' in i:
            txt = i.split(' :')[0]
        elif ': ' in i:
            txt = i.split(': ')[0]
        elif ' : ' in i:
            txt = i.split(' : ')[0]
        else:
            return i
        return txt
    else:
        return ''
        pass
    
def ungeo_subregion(df_file, ungeo_file = setting.ungeo_file):
    cn_geo = pd.read_csv(ungeo_file)
    cn_geo = cn_geo[['Country','Sub-region Name']]
    cn_geo.columns = ['geo_loc_name_country_fix','sub_region']   
    df_file = (pd.merge(df_file, cn_geo, 
               on='geo_loc_name_country_fix',
               how='left', 
               indicator=False))
    return df_file



# =============================================================================
# Submitted Metadata
# =============================================================================

def convert_fastq_to_sra(csv_in, file_dir = setting.dataport):
    df = pd.read_csv(csv_in)
    
    ## Add Run col as cenmigID if Null
    run_ls = []

    for sample_name, cenmigid, run in zip(df['Sample_Name'],df['cenmigID'], df['Run']):
        rex = file_dir + sample_name + '*' + '[fastq.gz|fg.gz|fq|fastq]'
            
        file = glob.glob(rex)
        if len(file) > 0:
            
            # Add cenmigID to Run col if Run col is na but has fastq file to convert. For facilitate file moving
            
            if isinstance(run, float): ## Float if NA value
                run_ls.append(cenmigid)
            elif isinstance(run, str):
                run_ls.append(run)
            
            
            fastq_file = ' '.join(file)
            tmp_dir = file_dir + 'tmp' + sample_name + '/'
            sra_file = file_dir + cenmigid + '.sra'
            
            # Run latf-load
            print('Convert ' + cenmigid + ' to sra')
            cmd_latf_load = 'latf-load --quality PHRED_33 ' + fastq_file + ' -v -o ' + tmp_dir
            subprocess.run([cmd_latf_load], shell = True)
           
            # Run kar to convert to sra
            cmd_kar = 'kar -c ' + sra_file + ' -d ' + tmp_dir
            subprocess.run([cmd_kar], shell = True)
            
            # Remove tmp file
            shutil.rmtree(tmp_dir)
        
    # Concert run_ls to series
    run_series = pd.Series(run_ls)
    df['Run'] = run_series
    
    ## Writing csv agiain to update Run id
    df.to_csv(csv_in, index = False)

def create_metadata_submit(xlsx_file, file_out):
    # Retrive Run Asm_acc and cenmigID from DB
    columns = ['Run', 'asm_acc', 'Sample_Name', 'BioProject', 'BioSample', 'Experiment', 
               'Organism', 'Serovar', 'AMR_genotypes', 
               'isolation_source', 'host', 'host_disease', 'host_disease_stage', 'host_age', 'host_sex', 
               'Collection_date', 'Center_Name', 
               'geo_loc', 'geo_loc_name_country',  'geo_loc_name_country_continent', 'lat_lon', 
               'Assay_Type', 'LibrarySource', 'LibrarySelection', 'LibraryLayout', 'Platform', 'Instrument', 
               'genome_material', 'other_attributes', 'clinical_data', 'demograhpic_data']
    
    run_old, cenmigID_old, asmacc_old = retrive_id_from_database()
    
    df = pd.DataFrame(columns=columns)
    data = pd.read_excel(xlsx_file, sheet_name = 'Form')
    # df = df.append(data) 
    df = pd.concat([df, data], ignore_index= False)
    
    ## remove Run asm_acc cenmigID if redundant
    df = df.loc[~df['Run'].isin(run_old)]
    df = df.loc[~df['asm_acc'].isin(asmacc_old)]
    
    
    df['geo_loc_name_country'] =  df['geo_loc_name_country'].apply(split_semicolon)
    df['geo_loc_name_country_fix'] = df['geo_loc_name_country'].replace({'\d+': np.nan, 'nan': np.nan, 
                                                                               'USA.*' : 'United states',
                                                                               'United Kingdom.*': 'United Kingdom',
                                                                               'Brazil.*': 'Brazil',
                                                                               'Australia,*' : 'Australia'}, 
                                                                              regex=True)
    
    correct_dict = dict_for_correct_country(countryname_col = df['geo_loc_name_country_fix'])
    
    df['geo_loc_name_country_fix'] = df['geo_loc_name_country_fix'].replace(correct_dict)
    df = ungeo_subregion(df)
    
    # Add column & cenmigID
    df = cenmigID_assigner(df)
    df = df.loc[~df['cenmigID'].isin(cenmigID_old)]
    
    # Assign Run for In-house data for storage file sra
    # df['Run'] = df['cenmigID']
    print('Writing csv file . . .')
    df.to_csv(file_out, index = False)

# =============================================================================
# Resistant DB Metadata
# =============================================================================

def metadata_drug_resistant_Resfinder(path_resfinder_result = '*/ResFinder_results_tab.txt', 
                                      path_std_json = '*/std_format_under_development.json',
                                      filename_out = 'metadata_drug_resistant.csv',
                                      path_resfinder_db = setting.resfinder_db + 'phenotypes.txt') :

    file_res = glob.glob(path_resfinder_result)
    file_std = glob.glob(path_std_json)
    df_out = pd.DataFrame()
    if len(file_res) > 0:

        for res, std in zip(file_res, file_std):
            cenmigID = res.rsplit('/', 2)[1]
            # cenmigID = res.rsplit('\\', 2)[1]
            print(cenmigID)
            df = pd.read_table(res, sep = '\t')
            
            # Insert list of ID to df
            add_col = []
            add_col.append(cenmigID)
            add_col = add_col * len(df)
            df.insert(0, 'cenmigID', add_col)
            
            # Create df for matching depth to gene name
            with open(std) as json_file:
                data = json.load(json_file)
                db_version = pd.DataFrame(data['databases'])
                db_version = db_version.loc['key'][0] # Extract db versuion
                data = data['genes']
            # Found some data with no Resistance gene using Error handling to skip
            try:
                depth_df = pd.DataFrame()
                for key in data.keys():
                    # print(key)
                    d_i = data[key]
                    name = d_i['name']
                    refid = d_i['ref_id']
                    depth = d_i['depth']
                    temp_df = {'Resistance gene' : name,
                               'Resistance gene variant' : refid,
                               'db_version' : db_version,
                               'depth' : depth}
                    temp_df = pd.DataFrame.from_records([temp_df])
                    depth_df = pd.concat([depth_df, temp_df], ignore_index=False)
                    # depth_df = depth_df.append(temp_df)
                # Match depth and gene name
                df = pd.merge(df, depth_df,
                                on = 'Resistance gene',
                                how = 'left',
                                indicator= False)
                
                # df_out = df_out.append(df)
                df_out = pd.concat([df_out, df], ignore_index= False)
            except:
                print('No resistance gene in ' + cenmigID)
                df_no_data = {'cenmigID' : str(cenmigID),
                              'Resistance gene' : 'no data',
                              'db_version' : db_version}
                df_no_data = pd.DataFrame.from_records([df_no_data])
                # df_out = df_out.append(df_no_data)
                df_out = pd.concat([df_out, df_no_data], ignore_index= False)
        
                              # Write file
        df_out = df_out[['cenmigID', 'Resistance gene', 'Resistance gene variant' ,'Identity',
                         'Alignment Length/Gene Length', 'Coverage', 'depth', 'db_version',
                         'Position in reference',
                         'Contig', 'Position in contig', 'Phenotype', 'Accession no.']]
        
        # Reading phenoytypes table and matching Resistance gene variant
        ## Must manual assign colnames to avoid error
        colnames = ['Gene_accession no', 'Class', 'Drug list', 'PMID', 'Mechanism of resistance', 'Notes', 'Required_gene']
        phenotype = pd.read_table(path_resfinder_db, names=colnames, header=None)
    
        df_out = pd.merge(df_out, phenotype,
                          left_on = ['Resistance gene variant'],
                          right_on = ['Gene_accession no'],
                          how = 'left',
                          indicator= False)
        
        df_out = df_out[['cenmigID', 'Resistance gene','Gene_accession no', 
                        'Identity', 'Alignment Length/Gene Length', 'Coverage', 
                        'depth', 'db_version', 'Position in reference', 'Contig', 'Position in contig', 'Phenotype', 
                        'Accession no.', 'Class', 'Drug list', 
                        'Mechanism of resistance', 'Required_gene']]
        # Change colnames
        df_out.columns = ['cenmigID', 'Resistance gene','Gene_accession no', 
                        'Identity', 'Alignment Length/Gene Length', 'Coverage', 
                        'depth', 'db_version', 'Position in reference', 'Contig', 'Position in contig', 'Phenotype', 
                        'Accession no', 'Class', 'Drug list', 
                        'Mechanism of resistance', 'Required_gene']
        
        # df_out['UniqueID'] = np.where(df_out['cenmigID'].duplicated(keep=False), 
        #                   df_out['cenmigID'] + df_out.groupby('cenmigID').cumcount().add(1).astype(str),
        #                   df_out['cenmigID'])
        
        ## np.where(condition, if True out, if False out)
        # group by id then cumlative coount .add(1) to start with 1 not 0
        
        # df_out['UniqueID'] = np.where(df_out['cenmigID'].duplicated(keep=False), 
        #                   df_out['cenmigID'] + '_' + df_out.groupby('cenmigID').cumcount().add(1).astype(str),
        #                   df_out['cenmigID'] + '_1')
        
        time_label = datetime.datetime.now()
        now = time_label.strftime("%d-%m-%Y_%H_%M_%S")
        # add update date at last col
        date_add = time_label.strftime('%d/%m/%Y')
        date_add = [date_add] * len(df_out)
        df_out.insert(len(df.columns)-1, 'Date_added', date_add)
        df_out.to_csv(filename_out , index = False)           

# main()
