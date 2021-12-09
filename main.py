# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 09:09:33 2021

@author: User
"""

import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.join(currentdir, "cenmigDB")
sys.path.append(parentdir)


# =============================================================================
# Module 
# =============================================================================

import setting


from metadata_create import prepare_metadata,tsv_to_csv,create_metadata_ncbi,create_metadata_submit

from search import searchDB

from download import download_from_ncbi,convert_sra_to_fastq_gunzip,unzip_fa

from store_file import store_fileDB

from save_file import save_data_csv,save_data_query

from update import update_field,replace_record

from delete import del_records_by_csv,del_records_by_query

from backup import backup_all 


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

# Build top-level parser
desc = 'This program is using for interact with the CenmigDB via command-line '
desc = desc + 'for creating new collection or DB of MongoDB please use mongoimport'
parser = argparse.ArgumentParser(description=(desc))

## Build Sub-parser for each function
subparsers = parser.add_subparsers(dest='command') # dest = 'command' > specify object name command

### Sub command

### Metadata sub command
# Download PathogenDB and assemblydata
prepare_data = subparsers.add_parser('prepare_data', help='Download tsv and txt metadata from NCBI PathogenDB and AssemblyDB')
prepare_data.add_argument("--species", "-s", help="Specific species to prepare [default : setting.species]", default = setting.species) 
prepare_data.add_argument("--out_path", "-O", help="output path [default:/raw_metadata/]", default= setting.metadata)

# ncbi
metadata_ncbi = subparsers.add_parser('metadata_ncbi', help='Create and reformat data from ncbi from NCBI to csv file for updating CenmigDB')
metadata_ncbi.add_argument("--output_filename", "-o", help="Specific output filename")  

# submit
metadata_submit = subparsers.add_parser('metadata_submit', help= 'Reformatting submission_file to csv file for updating CenmigDB')  
metadata_submit.add_argument("--input_file", "-i", help="Input csv file")
metadata_submit.add_argument("--output_filename", "-o", help="Specific output filename")

### Search
search = subparsers.add_parser('search', help= 'Query metadata based on keyword in txt file')
search.add_argument("--input", "-i", help="Specific input query txt file")
search.add_argument("--out_path", "-O", help="csv output path [default:/query_result_path/]", default= setting.query_result_path)
search.add_argument("--out_file", "-o", help="csv output filename [default: Search_result]", default= 'Search_result')

### Update
# replace
replace_records = subparsers.add_parser('replace_records', help= 'Update record by replacing entire old record')
replace_records.add_argument("--input", "-i", help="Specific input csv filename", default = setting.index_column)
replace_records.add_argument("--out_file", "-o", help="Specific output backup filename", default = 'BackUp_beforeupdate')

# update field
update_fields = subparsers.add_parser('update_fields', help= 'Add data fields(Columns) to records')
update_fields.add_argument("--input", "-i", help="Specific input csv filename")
update_fields.add_argument("--index_column", "-ID", help="ID which use to matching (Run,asm_acc,cenmigID) [default:cenmigID]", default = setting.index_column)
update_fields.add_argument("--backup_filename", "-o", help="Specific output backup filename [default: BackUp_beforeupdate_$DATETIME] ", default = 'BackUp_beforeupdate')

# delete record and file
delete_records = subparsers.add_parser('delete_records', help= 'Delete records')
delete_records.add_argument("--input", "-i", help="Specific input csv or query txt file")
delete_records.add_argument("--index_column", "-ID", help="ID which use to matching (Run,asm_acc,cenmigID) [default: cenmigID]", default = setting.index_column)
delete_records.add_argument("--delete_file", "-del", help="Delete sequence files from db (input y or yes) [default: None]", default = 'none')

### Backup
backup = subparsers.add_parser('backup', help= 'Backup all metadata in CenmigDB')
backup.add_argument("--out_path", "-O", help="Specific output backup path [default: /backup_metadata/", default = setting.backup_metadata_path)
backup.add_argument("--out_file", "-o", help="Specific back up filename [default: 'cenmig_metadataDB_backup_$DATETIME']", default = 'cenmig_metadataDB_backup')

### Manage sequence file
# download ncbi
download_ncbi = subparsers.add_parser('download_ncbi', help= 'Download file from ncbi based on Run and asm_acc from input csv')
download_ncbi.add_argument("--input", "-i", help="Specific input csv filename for listing SRA and asm_acc for download")
download_ncbi.add_argument("--out_path", "-o", help="Specific output location [default : cenmigDBport_internal/]", default= setting.dataport)
download_ncbi.add_argument("--fq_dump", "-d", help="Convert sra to fq.gz (default:False) (input y or yes only)")
download_ncbi.add_argument("--unzip", "-u", help="unzip fna.gz (default:False) (input y or yes only)")

# copy file from data_sequences to user
get_file = subparsers.add_parser('get_file', help= 'Copy file SRA and fasta to user with Run and asm_acc from csv')
get_file.add_argument("--input", "-i", help="Specific input csv or query txt file")
get_file.add_argument("--out_loc", "-o", help="Specific output location folder [default: query_result_path/]", default = setting.query_result_path)

# store file from data_port to data_sequences
store_seqfile = subparsers.add_parser('store_seqfile', help= 'Moving file to storage location based on spp. and filetype')
store_seqfile.add_argument("--input", "-i", help="Specific input csv filename")
store_seqfile.add_argument("--file_in", "-file", help="folder of file [default: cenmigDBport_internal/]", default = setting.dataport)
store_seqfile.add_argument("--db_path", "-db", help="path of DB [default: data_sequences/", default = setting.data_sequences)


args = parser.parse_args()

# =============================================================================
#  Link argument to execute function
# =============================================================================

if args.command == 'metadata_ncbi':
    create_metadata_ncbi(file_out = args.output_filename)

elif args.command == 'metadata_submit':
    csv_file = args.input
    file_out = args.output_filename
    create_metadata_submit(csv_file = args.input, file_out = args.output_filename)

elif args.command == 'prepare_data':
    prepare_metadata(spp = args.species, file_out = args.out_path)
    tsv_to_csv(filepath = args.out_path)

elif args.command == 'search' :
    searchDB(query_file = args.input, 
             out_path = args.out_path, 
             result_filename = args.out_file)

elif args.command == 'update_fields':
    update_field(csv_update = args.input, index_column = args.index_column, 
                         filename_backup = args.backup_filename)
    
elif args.command == 'replace_record':
    replace_record(csv_update = args.input, 
                   index_column = args.index_column, 
                   filename_backup = args.backup_filename)

elif args.command == 'delete_records':
    if args.delete_file.upper() in ['Y','YES']:
        args.delete_file = 'YES'
    else:
        args.delete_file = 'none'

    if '.csv' in str(args.input):
        del_records_by_csv(csv_filename = args.input, 
                           index_column = args.index_column , 
                           delete_file_state = args.delete_file)
    
    if '.txt' in str(args.input):
        del_records_by_query(query_file = args.input, 
                           index_column = args.index_column , 
                           delete_file_state = args.delete_file)
        
elif args.command == 'backup':
    backup_all(file_out = args.out_path , 
               filename = args.out_file)

elif args.command == 'download_ncbi':
    fastqdump = str(args.fq_dump)
    unzip = str(args.unzip)
    download_from_ncbi(csv = args.input, 
                       file_out = args.out_path)
    if fastqdump.upper() in ['Y','YES']:
        convert_sra_to_fastq_gunzip(filepath = args.out_path)
    if unzip.upper() in ['Y','YES']:
        unzip_fa(file_path = args.out_path)
        file_zip = glob2.glob(args.out_path + '*.gz')
        for file in file_zip:
            os.remove(file)

elif args.command == 'get_file':
    if '.csv' in str(args.input):
        save_data_csv(csv_file = args.input, 
                      file_out = args.out_loc , 
                      db_path = setting.data_sequences, 
                      save_file_state = 'save')
    
    if '.txt' in str(args.input):
        save_data_query(query_file = args.input, 
                        file_out = args.out_loc,
                        db_path = setting.data_sequences, 
                        save_file_state = 'save')

elif args.command == 'store_seqfile':
    store_fileDB(csv_file = args.input, 
                 db_path = args.db_path, file_in = args.file_in)

