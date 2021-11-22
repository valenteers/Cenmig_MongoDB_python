# #!/usr/bin/env python3  

#==================================== Load my packages =================================#

from import_modules import *
# import pymongo
# import setting
# import util

#==================================== Load library =====================================#


# import pandas as pd 
# import json
# import os
# import glob
# import re
# from datetime import datetime
# import ftplib
# import subprocess

# def path(filepath):
#     filepath_sra = filepath + '/SRA_info_table/*/SraRun*'
#     filepath_amr = filepath + '/*_amr.csv'
#     filepath_wgs = filepath + '/assembly*.txt'
#     print(filepath_sra)
#     print(filepath_amr)
#     print(filepath_wgs)
#     return filepath_sra, filepath_amr, filepath_wgs

def prepare_metadata(spp = setting.species, file_out = setting.metadata):
    ''' Download PathogenDB and assembly Automatically'''
    # Delete old file First
    file_remove = glob.glob(file_out + '*assembly_summary.txt') + glob.glob(file_out + '*.metadata.tsv') + glob.glob(file_out + '*.metadata.csv')
    if len(file_remove) > 0:
        for file in file_remove:
            os.remove(file)
    
    for sp in spp:
        print(sp + '\n')
        sp = sp.replace(' ','_')
        if sp != 'Salmonella_enterica':
            if sp != 'Streptococcus_agalactiae': # Edit this line if NCBI PathogenDB has STAG data
                ### STAG no pathogenDB at now 10.11.2021
                FTP_HOST = "ftp.ncbi.nlm.nih.gov"
                FTP_USER = 'anonymous'
                FTP_PASS = '@anonymous'
                ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS, timeout=6000)
                # Pathogen
                path = 'pathogen/Results/' + sp
                ftp.cwd(path) # Change path
                
                patho_ls = pd.Series(ftp.nlst()) # Return list of file
                ftp.quit()
                patho_ls = patho_ls[patho_ls.str.contains('^P.*\d$', case = True, regex =True)]
                lastest_ver = [int(i.rsplit('.', 1)[1]) for i in patho_ls] # Find lastest ver
                lastest_ver = max(lastest_ver) # Select max
                # Pop lastest version filename
                pathogen_version = list(patho_ls[patho_ls.str.contains(str(lastest_ver), case = True, regex =True)]).pop()
                pathogen_version = '/' + pathogen_version
                print(pathogen_version)
            
                # Download PathogenDB
                pathogen_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pathogen/Results/' + sp + pathogen_version + '/Metadata/' + pathogen_version + '.metadata.tsv'
                print(pathogen_ftp)
                cmd_pathogen = "wget " +  pathogen_ftp + ' -P ' + file_out
                download_pathogen = subprocess.run([cmd_pathogen], shell=True )
            else:
                print(sp)
                
            # Download assembly
            assembly_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/' + sp + '/assembly_summary.txt'
            cmd_assembly = "wget " +  assembly_ftp + ' -P ' + file_out
            download_assembly = subprocess.run([cmd_assembly], shell=True )
            os.rename(file_out + 'assembly_summary.txt',file_out + sp + '_assembly_summary.txt')
             
        elif sp == 'Salmonella_enterica':
            sp_sal = 'Salmonella' # In PathogenDB use 'Salmonella' name
            FTP_HOST = "ftp.ncbi.nlm.nih.gov"
            FTP_USER = 'anonymous'
            FTP_PASS = '@anonymous'
            ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS, timeout=6000)
            # Pathogen
            path = 'pathogen/Results/' + sp_sal
            ftp.cwd(path) # Change path
            
            # Retrive lastest version from ftp directories
            patho_ls = pd.Series(ftp.nlst()) # Return list of file
            ftp.quit()
            patho_ls = patho_ls[patho_ls.str.contains('^P.*\d$', case = True, regex =True)]
            lastest_ver = [int(i.rsplit('.', 1)[1]) for i in patho_ls] # Find lastest ver
            lastest_ver = max(lastest_ver) # Select max
            # Pop lastest version filename
            pathogen_version = list(patho_ls[patho_ls.str.contains(str(lastest_ver), case = True, regex =True)]).pop()
            pathogen_version = '/' + pathogen_version
            print(pathogen_version)
            
            # Download PathogenDB
            pathogen_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pathogen/Results/' + sp_sal + pathogen_version + '/Metadata' + pathogen_version + '.metadata.tsv'
            print(pathogen_ftp)
            cmd_pathogen = "wget " +  pathogen_ftp + ' -P ' + file_out
            download_pathogen = subprocess.run([cmd_pathogen], shell=True )
            
            # Download assembly
            assembly_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/' + sp + '/assembly_summary.txt'
            cmd_assembly = "wget " +  assembly_ftp + ' -P ' + file_out
            download_assembly = subprocess.run([cmd_assembly], shell=True )
            os.rename(file_out + 'assembly_summary.txt',file_out + sp + '_assembly_summary.txt')

def tsv_to_csv(filepath = setting.metadata):
    file = glob.glob(filepath + '*.tsv')
    for tsv in file:
        df = pd.read_table(tsv, sep='\t')
        csv_filename = tsv.replace('.tsv','.csv')
        df.to_csv(csv_filename, index = False)
        os.remove(tsv)
        
def merge_srainfotable(filepath = setting.sra_path):
    col_list = setting.df_colnames
    df = pd.DataFrame(columns=(col_list))
    file_all = glob.glob(filepath)
    for i in file_all:
        print(str(i))
        df_i = pd.read_csv(i,low_memory=False)
        # Select column
        df = df.append(df_i)
    # Select column
    df = combine_colnames_ignorecase(df, col_list)
    df.drop_duplicates(keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True) # Reset index before filterout
    return df

def fix_data_column_shift(df):
    df_old = df.copy()
    # some host_sex are wrong due to data column shift
    ## regex find value
    df['host_sex'].fillna('', inplace=True)
    key_sex_list = ['male', 'female', 'not collected', 'not applicable', 
                  'not available', 'missing', 'not determined', '', 'na', 'none']
    regex_sex_list = []
    for i in key_sex_list:
        i = '^' + i + '$'
        i = re.sub(' ', '.', i)
        print(i)
        regex_sex_list.append(i)
    regex_query = '|'.join(regex_sex_list)
    
    df_wrong_data_hostsex = df[~df['host_sex'].str.contains(regex_query, case = False, regex =True)]
    bioproject_1 = df_wrong_data_hostsex['BioProject'].unique()
    
    # some host are wrong due to data column shift
    ## Country name in Column
    df['host_for_filter'] =  df['host'].apply(split_semicolon)
    cn_geo = pd.read_csv('UNGEO.csv')
    cn_geo = cn_geo[['name']]
    df_wrong_data_host = df[df['host_for_filter'].isin(cn_geo['name'])]
    bioproject_2 = df_wrong_data_host['BioProject'].unique()
    
    bioproject = list(bioproject_1) + list(bioproject_2)
    bioproject = set(bioproject)
    
    df_fordrop = pd.DataFrame()
    for bio in bioproject:
        df_i = df[df['BioProject'] == str(bio)]
        df_fordrop = df_fordrop.append(df_i)
    df.drop(df_fordrop.index, inplace=True)
    df_drop_out = df_old.drop(df.index)
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    df_drop_out.to_csv('DATA_shift' + now + '.csv', index = False)

    # Search with bioproject
    query_wrong_data = ' OR '.join(bioproject)
    print(query_wrong_data)
    print('\nPlease searh these bioproject in SRA database\n')
    
    # Append data
    filepath = str(input('Filepath of SRAinfo : '))
    df_fix = merge_srainfotable(filepath)

    # Append missing data
    df = df.append(df_fix, ignore_index = True)
    df.reset_index(drop=True, inplace=True)
    return df, bioproject, df_drop_out

def drop_non_interest_species(df):
    df_old = df.copy()
    spp = setting.species

    regex_spp = []
    for i in spp:
        i = i.replace(' ','.')
        i = '^' + i + '*'
        regex_spp.append(i)
    regex_spp = '|'.join(regex_spp)

    df = df.loc[df['Organism'].str.contains(regex_spp, case=False)]
    df_filter_out = df_old.drop(df.index)
    # Drop dup
    df = df.drop_duplicates(keep='first', inplace=False) 
    df.reset_index(drop=True, inplace=True) # Reset index after filterout
    return df, df_filter_out

def remove_duplicates_sra_data(df_sra):
    # Clean Run id duplicates
    df_sra_clean = df_sra.copy()
    dup =  df_sra.loc[df_sra.duplicated(subset=['Run']), :]
    dup = list(dup['Run'].unique())
    df_dup = pd.DataFrame()
    for run in dup:
        df = df_sra[df_sra['Run'] == str(run)]
        df_dup = df_dup.append(df)
  
    now = datetime.now()
    now = now.strftime("%d-%m-%Y_%H_%M_%S")
    if len(df_dup) > 0 :
        print('\nFound SRA run duplicates\n')
        filename = 'Duplicates_data_' + now + '.csv'
        df_dup.to_csv(filename)
        print('\n Please see index in ' + filename)
        print('\n and select index of redundant data')
        drop = True
        while drop:
            index = input('Remove index no.(stop by type N) :')
            if str(index).upper() != 'N':
                dropindex = int(index)
                df_sra_clean.drop(dropindex, inplace=True)
                print('Drop ' + str(index) + ' From df_sra')
            else:
                drop = False
    else:
        pass
    df_sra_drop = df_sra.drop(df_sra_clean.index)
    df_sra_clean.reset_index(drop=True, inplace=True)
    
    print('\n Finish Removing Duplicated in the SRA dataset \n')
    return df_sra_clean, df_sra_drop

def combine_colnames_ignorecase(df, target_col):
    print('\n Combined column with same name \n')
    regex_list = []
    for i in target_col:
        i = '^' + i + '$'
        i = re.sub('_', '.', i)
        i = re.sub(' ', '.', i)
        regex_list.append(i)
    # Join regex to one str
    df_colname = df.columns
    regex_query = '|'.join(regex_list)
    
    # Convert to series for str.contains method
    col_series = pd.Series(df_colname)
    col_series = col_series[col_series.str.contains(regex_query, case = False, regex =True)]
    df = df[set(col_series)]
    df.fillna('', inplace=True)
     
    df_out = pd.DataFrame()

    for regex in regex_list:
        target = col_series[col_series.str.contains(regex, case = False, regex =True)]
        target = list(target)
        print(target)
        col = regex.replace('$', '')
        col = col.replace('^', '')
        col = col.replace('.', '_')
        df_out[str(col)] = df[target].astype(str).agg(''.join, axis=1) 
    
    df_out = df_out.replace(to_replace= r'\\', value= '', regex=True)
    
    return df_out

def merge_AMR_data(filepath = setting.amr_path):
    df_amr = pd.DataFrame()
    file_amr = glob.glob(filepath)
    for i in file_amr:
        df_i = pd.DataFrame
        print(str(i))
        #df_i = pd.read_csv(i,low_memory=False, converters={'bioproject_acc' : str,'biosample_acc' : str,'scientific_name' : str})
        df_i = pd.read_csv(i, low_memory=False)
        df_amr = df_amr.append(df_i)
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
    sra_run = df_sra[['Run']]
    amr_run_asm = df_amr_clean[['Run', 'asm_acc']]
    amr_run_asm = amr_run_asm.dropna(subset=['asm_acc'], inplace=False)
    amr_run_asm = amr_run_asm.dropna(subset=['Run'], inplace=False)
    
    # df which data from df and amr has both Run and asm_acc
    run_asm_match = pd.merge(sra_run, amr_run_asm, on = ['Run'], indicator=False)
    run_asm_match = run_asm_match.drop_duplicates(keep='first', inplace=False)
    
    # Match asm
    df_sra = pd.merge(df_sra, run_asm_match, on = ['Run'], how = 'left', indicator=False)
       
    # AMR
    amr = df_amr_clean.dropna(subset=['Run'], inplace=False)
    amr_col_list = ['Run','AMR_genotypes' ]
    amr       = amr[amr_col_list]
    amr.drop_duplicates(inplace=True)
    
    df_sra = pd.merge(df_sra, amr, on=['Run'], how='left', indicator=False)
    df_dup = df_sra.loc[df_sra.duplicated(subset=['Run']), :]
    
    end = len(df_sra)
    
    if start == end:
        return df_sra, df_dup
    else: 
        print('\nAMR data error')
        print('Please check')
        return df_sra, df_dup

def exact_ASMacc_notmatch(df_sra_clean, df_amr_clean):
    amr_drug = df_amr_clean.dropna(subset=['AMR_genotypes'], inplace= False)
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
        df_fix = merge_srainfotable(filepath)
        df_fix_clean, notsave = remove_duplicates_sra_data(df_fix)
            
        # Append missing data
        df_sra_clean_full = df_sra_nodup.append(df_fix_clean, ignore_index = True)
        df_sra_clean_full.reset_index(drop=True, inplace=True)
        return df_sra_clean_full
    else:
        return df_sra_nodup
    
def cenmigID_assigner(df):
    df_cenmig = df[['Run', 'asm_acc', 'Sample_Name', 'Center_Name']]
    df_cenmig['Inhouse'] = df_cenmig['Sample_Name'] + '|' + df_cenmig['Center_Name']
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

def merge_and_clean_WGS_assembly_data(filepath = setting.assembly_path):
    df = pd.DataFrame()
    file_all = glob.glob(filepath)
    for i in file_all:
        print(str(i))
        df_i = pd.read_table( i, header = 1, low_memory=False) #Skip first row
        df = df.append(df_i)
    
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
        df_dup = df_dup.append(df_i)
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

def ungeo_subregion(df_file, ungeo_file = setting.ungeo_file):
    cn_geo = pd.read_csv(ungeo_file)
    cn_geo = cn_geo[['name','sub-region']]
    cn_geo.columns = ['geo_loc_name_country','sub_region']   
    df_file = (pd.merge(df_file, cn_geo, 
               on='geo_loc_name_country',
               how='left', 
               indicator=False))
    return df_file

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
    
def retrive_id_from_database():
    dbCollection = util.connect_database()
    #data = from_input_to_dict()
    #data = dbCollection.aggregate([{'$match': data }])
    print('Connecting to CenmigDB and retrive old record id ...')
    data = dbCollection.find({}, {'Run' : True, 'asm_acc' : True, 'cenmigID' : True})
    data = pd.DataFrame.from_dict(data)
    if len(data) > 0 :
        data = data[['Run', 'cenmigID', 'asm_acc']]
        cenmigID_old = data['cenmigID'].dropna()
        run_old = data['Run'].dropna()
        asmacc_old = data['asm_acc'].dropna()
        print('Retriving completed')
        return list(run_old), list(cenmigID_old), list(asmacc_old)
    else:
        print('No data in DB')
        return [], [], [] # Return empty list if no data in DB

def create_metadata_ncbi(file_out = 'cenmigDB_metadata.csv'):
    # Retrive Run Asm_acc and cenmigID from DB
    run_old, cenmigID_old, asmacc_old = retrive_id_from_database()
    
    print('\nStart generate metadata ...\n')
    # Create and Clean SRA data
    df_sra = merge_srainfotable()
    
    ## remove Run which has in DB
    df_sra = df_sra.loc[~df_sra['Run'].isin(run_old)]
    
    df_sra_fixed, bioproject, df_error_data = fix_data_column_shift(df_sra)
    df_sra_species_correct, df_species_out = drop_non_interest_species(df_sra_fixed)
    df_sra_nodup, df_sra_dup = remove_duplicates_sra_data(df_sra_species_correct)
    
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
    df_sra_completed = check_sra_missing_data(df_sra_nodup, df_amr_clean)
    
    # Match asm_acc with SRA id
    df_sra_asm_acc, df_file_dup = match_ASMacc_to_SRA(df_sra_completed, df_amr_clean)
    
    # Exact not match asm_acc but has AMR genotypes data
    df_asm_notmatch = exact_ASMacc_notmatch(df_sra_asm_acc, df_amr_clean)
    
    # Append df_file df_asm_notmatch
    df_file = df_sra_asm_acc.append(df_asm_notmatch, ignore_index=True)
    df_file.reset_index(drop=True, inplace=True)
    
    # Add ftp_path to asm_acc
    df_wgs, df_wgs_waste = merge_and_clean_WGS_assembly_data()
    df_file, df_ftp_drop = add_ftp_path_of_fasta(df_file, df_wgs)
    
    print('\nWGS part successed ...\n')
    
    # Fix country name
    df_file['geo_loc_name_country'] =  df_file['geo_loc_name_country'].apply(split_semicolon)
    
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
                    'geo_loc', 'geo_loc_name_country',  'geo_loc_name_country_continent', 'lat_lon', 'sub_region', 
                    'Assay_Type', 'LibrarySource', 'LibrarySelection', 'LibraryLayout', 'Platform', 'Instrument', 
                    'ftp_path']
    
    # Check data number
    if len(df_sra_asm_acc) + len(df_asm_notmatch) == len(df_file):
        df_file = df_file[ordered_colnames]
        print('\nPrepaare to write metadata csv file ...\n')
        df_file.to_csv(file_out, index= False)
        print('Finished')
    else:
        print('UNGEO match error')

def create_metadata_submit(csv_file, file_out):
    # Retrive Run Asm_acc and cenmigID from DB
    columns = ['Run', 'asm_acc', 'Sample_Name', 'BioProject', 'BioSample', 'Experiment', 
               'Organism', 'Serovar', 'sequence_typing', 'AMR_genotypes', 
               'isolation_source', 'host', 'host_disease', 'host_disease_stage', 'host_age', 'host_sex', 
               'Collection_date', 'Center_Name', 
               'geo_loc', 'geo_loc_name_country',  'geo_loc_name_country_continent', 'lat_lon', 
               'Assay_Type', 'LibrarySource', 'LibrarySelection', 'LibraryLayout', 'Platform', 'Instrument', 
               'genome_material', 'other_attributes', 'clinical_data', 'demograhpic_data']
    
    run_old, cenmigID_old, asmacc_old = retrive_id_from_database()
    
    df = pd.DataFrame(columns=columns)
    data = pd.read_csv(csv_file)
    df = df.append(data) 
    
    ## remove Run asm_acc cenmigID if redundant
    df = df.loc[~df['Run'].isin(run_old)]
    df = df.loc[~df['asm_acc'].isin(asmacc_old)]
    
    df = ungeo_subregion(df)
    
    # Add column & cenmigID
    df = cenmigID_assigner(df)
    df = df.loc[~df['cenmigID'].isin(cenmigID_old)]
    df.to_csv(file_out, index = False)
        
#create_metadata_ncbi(file_out = 'cenmigDB_metadata.csv')
