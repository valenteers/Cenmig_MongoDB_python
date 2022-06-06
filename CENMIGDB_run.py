#! /usr/bin/env python

import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
packagesdir = os.path.join(currentdir, "cenmigDB")
sys.path.append(packagesdir)


# =============================================================================
# Module 
# =============================================================================

import setting
import time


# from metadata_create import download_pathoDB_AssemblyDB_metadata,tsv_to_csv,create_metadata_ncbi,create_metadata_submit
from metadata_create import download_pathoDB_AssemblyDB_metadata,tsv_to_csv,create_metadata_ncbi,create_metadata_submit,convert_fastq_to_sra
from search import searchDB

from download import download_from_ncbi,convert_sra_to_fastq_gunzip,unzip_fa,download_ncbi_SEA

from store_file import store_fileDB,store_fileSEA

from save_file import get_file

from update import update_record,replace_record,update_resistantDB

from delete import del_records_by_csv,del_records_by_query

from export_data import export_all 

from summarize import summarize_data

from analyze_SEA_data import process_SEA_data
# =============================================================================
# Package
# =============================================================================
import argparse
import glob2
# import os

# =============================================================================
#  Args section
# https://gist.github.com/amarao/36327a6f77b86b90c2bca72ba03c9d3a
# =============================================================================


# =============================================================================
# Function
# =============================================================================
def make_db(file_csv_out, mode, xlsx_in):
    if mode.lower() == 'ncbi':
        print('Please check loading module before start since cant load module within python script')
        print('ml SRA-Toolkit/3.0.0-ubuntu64')
        time.sleep(5)
        print('Start Making DB')
       
        #download_pathoDB_AssemblyDB_metadata(file_out = setting.metadata)
        #tsv_to_csv(filepath = setting.metadata)
        
        create_metadata_ncbi(file_csv_out)
        update_record(csv_update = file_csv_out, index_column = setting.index_column, filename_backup = 'Backup')

        # # Download SRA and fasta
        db_metadata = download_ncbi_SEA()
        store_fileSEA(db_metadata)
        
        print('Making DB Finished')
    
    elif mode.lower() == 'submit':
        create_metadata_submit(xlsx_in, file_csv_out)
        convert_fastq_to_sra(file_csv_out)
        update_record(csv_update = file_csv_out, index_column = setting.index_column, filename_backup = 'Backup')
        store_fileDB(csv_file = file_csv_out, db_path = setting.data_sequences, file_in = setting.dataport)

def update_DB(csv_update, filename_backup, mode, index_column, delete_file_state):
    if mode.lower() == 'update':
        update_record(csv_update, index_column, filename_backup)
    
    elif mode.lower() == 'mlst_resfinder_run':
        file_outpath = process_SEA_data(csv_file = csv_update)
        update_record(csv_update = file_outpath + 'mlst_out.csv', index_column = setting.index_column, 
                      filename_backup = filename_backup, upsert = False)
        try:
            update_record(csv_update = file_outpath + 'seqsero2_out.csv', index_column = setting.index_column, 
                          filename_backup = filename_backup, upsert = False)
        except:
            pass
            
        # Update to Resistant DB
        update_resistantDB(csv_update = file_outpath + 'metadata_drug_resistant.csv', 
                            filename_backup = filename_backup)
    
    elif mode.lower() == 'replace':
         replace_record(csv_update, index_column, filename_backup)
        
    # elif mode.lower() == 'edit_inhouse':
    #     edit_data_inhouse(csv_update, filename_backup, index_column = '_id')
    
    elif mode.lower() == 'delete':
         del_records_by_csv(csv_update, index_column , delete_file_state)
    
    elif mode.lower() == 'resistant':
         update_resistantDB(csv_update, filename_backup)
    
    elif mode.lower() == 'download_ncbi_file':
         download_from_ncbi(csv_update, file_out = setting.dataport, db_path = setting.data_sequences)
         ## Unzip fasta file
         unzip_fa(file_path = setting.dataport)
         file_zip = glob2.glob(setting.dataport + '*.gz')
         for file in file_zip:
             os.remove(file)
         if input('Move file to data_sequences folder? : {Y/N)').upper() == 'Y':
            store_fileDB(csv_update, db_path = setting.data_sequences, file_in = setting.dataport)
    
# def delete_DB(file_in, delete_file_state,index_column = setting.index_column):
#     del_records_by_csv(file_in, index_column , delete_file_state)
   
def search_DB(file_in, out_path, result_filename, mode):
    if mode.lower() == 'search':
        query_file = file_in
        searchDB(query_file, out_path, result_filename)
    elif mode.lower() == 'all':
        export_all(out_path, filename = result_filename)

def export_sequences(csv_file, out_path, file_type):
    file_out = out_path
    get_file(csv_file, file_out, file_type, db_path = setting.data_sequences, save_file_state = "save")
    
    
        

# Build top-level parser
desc = 'This program is using for interact with the CenmigDB via command-line '
desc = desc + 'for creating new collection or DB of MongoDB please use mongoimport'
parser = argparse.ArgumentParser(description=(desc))

## Build Sub-parser for each function
subparsers = parser.add_subparsers(dest='command') # dest = 'command' > specify object name command

### Make_DB
make_db_help = 'Please check loading module before start since cant load module within python script with "ml SRA-Toolkit/3.0.0-ubuntu64"'
make = subparsers.add_parser('make_db', help='Download, Reformat, and Update to MongoDB', 
                             description = make_db_help)
make.add_argument("--xlsx_in_filename", "-i", help="Specific xlsx input filename (for mode : submit only)") 
make.add_argument("--csv_out_filename", "-o", help="Specific csv output filename [default : 'cenmigDB_metadata.csv']", default = 'cenmigDB_metadata.csv') 
make.add_argument("--mode", "-m", help="there are 2 modes : ncbi or submit [default : 'ncbi']", default = 'ncbi') 
make.add_help


### Update_DB
mlst_run_help = 'NOTE: Please load module before run "-m mlst_resfinder_run" with "ml SRA-Toolkit/3.0.0-ubuntu64 SPAdes/3.13.2 SeqSero2/1.2.1 and skip -i if run analysis based on setting.analyze_query"'
update = subparsers.add_parser('update_db', help='Update, Replace, Update to ResistantDB or download seq file from ncbi',
                               description = mlst_run_help )
update.add_argument("--csv_in_filename", "-i", help="Specific csv input filename", default= True)
update.add_argument("--csv_out_filename", "-o", help="Specific csv output filename prefix [default : 'Backup']", default = 'Backup') 
update.add_argument("--mode", "-m", help="there are several modes : update ,mlst_resfinder_run, replace, delete, resistant and download_ncbi_file") 
update.add_argument("--index_column", "-index", help="ID which use to matching (Run,asm_acc,cenmigID,_id) [default: cenmigID]", default = setting.index_column) 
update.add_argument("--delete_file", "-del", help="Delete sequence files from db (input YES to delete file)[delete mode only] [default: none]", default = 'none')

### Delete records
# delete = subparsers.add_parser('delete_records', help= 'Delete records')
# delete.add_argument("--csv_in_filename", "-i", help="Specific input csv or query txt file")
# delete.add_argument("--index_column", "-ID", help="ID which use to matching (Run,asm_acc,cenmigID) [default: cenmigID]", default = setting.index_column)
# delete.add_argument("--delete_file", "-del", help="Delete sequence files from db (input y or yes) [default: none]", default = 'none')

### Search
search = subparsers.add_parser('search', help= 'Query metadata based on keyword in txt file or export all metadata')
search.add_argument("--input", "-i", help="Specific input query txt file")
search.add_argument("--out_path", "-O", help="output path [default:/query_result_path/]", default= setting.query_result_path)
search.add_argument("--out_file_prefix", "-o", help="csv output filename prefix [default: Search_result]", default= 'Search_result')
search.add_argument("--mode", "-m", help="there are 2 modes : search (for query data with txt file) and all (for export all metadata) [default : search]", default = 'search')

### Export sequences
export = subparsers.add_parser('export_sequences', help= 'Copy sequence files based on csv file shopping list')
export.add_argument("--csv_in_filename", "-i", help="Specific csv input filename") 
export.add_argument("--out_path", "-O", help="output path [default:/query_result_path/]", default= setting.query_result_path)
export.add_argument("--file_type", "-file", help="there are 3 modes : sra , fasta, all [default : sra]", default = 'sra')

### Summarize data
summarize_help = 'NOTE: Please load module before run with "ml R"'
summarize = subparsers.add_parser('summarize_data', help= 'Summarize data and export result to csv and png',
                                  description = summarize_help)
summarize.add_argument("--out_path", "-O", help="output path [default:/query_result_path/]", default= setting.query_result_path)
summarize.add_argument("--input", "-i", help="Input (can be csv(metadata), txt(query file) or Skip this args if use data from mongoDB", default = 'skip')

args = parser.parse_args()

# =============================================================================
#  Link argument to execute function
# =============================================================================

if args.command == 'make_db':
    file_csv_out = args.csv_out_filename
    mode = args.mode
    xlsx_in = args.xlsx_in_filename
    make_db(file_csv_out, mode, xlsx_in)

elif args.command == 'search':
    file_in = args.input
    out_path = args.out_path
    result_filename = args.out_file_prefix
    mode = args.mode
    search_DB(file_in, out_path, result_filename, mode)

# elif args.command == 'delete':
#     if args.delete_file.upper() in ['Y','YES']:
#         args.delete_file = 'YES'
#     else:
#         args.delete_file = 'none'
    
#     file_in = args.csv_in_filename
#     delete_file_state = args.delete
#     index_column = args.index_column
    
#     delete_DB(file_in, delete_file_state,index_column)

elif args.command == 'update_db':
    csv_update = args.csv_in_filename
    filename_backup = args.csv_out_filename
    mode = args.mode
    index_column = args.index_column
    delete_file_state = args.delete_file
    update_DB(csv_update, filename_backup, mode, index_column, delete_file_state)

elif args.command == 'export_sequences':
    csv_file = args.csv_in_filename
    out_path = args.out_path
    file_type = args.file_type
    export_sequences(csv_file, out_path, file_type)
    
elif args.command == 'summarize_data': 
    file_out = args.out_path
    if args.input == 'skip':
        input_file = True
    else:
        input_file = args.input
        
    summarize_data(file_out, input_file)
