#!/usr/bin/env python3  

#==================================== Config Setting =====================================#

#------------------------------------ Database MongoDB -------------------------------------#


database_url = 'mongodb://localhost'
database_port = 27017
database_name = 'cenmigDB' #ชื่อดาต้าเบส
database_collection = 'Bacteria' #ชื่อของเทเบิ้ล
index_column = 'cenmigID' #คอลัมภ์ที่เป็น unique สำหรับใช้ค้นหา

#------------------------------------ Path -------------------------------------------------#
import os, sys
# main = '/data/home/tiravut.per/cenmigDB/'
#main = '/data/home/chaivut.ton/NAScenmig/cenmigDB/cenmigDB/cenmigDB/'
main = os.path.dirname(os.path.realpath(__file__)) + '/'
path = ''

file_no_duplicate_with_database = 'no_duplicate_with_database.csv' #ชื่อไฟล์สำหรับฟังก์ชั่นที่ไว้ใช้เช็คไฟล์
default_path = '/home/liria/Desktop/import/cenmigDB.csv' #โฟลเดอร์สำหรับ import ไฟล์เข้า
request_by_csv = 'request.csv'

# dataport = 'Nascenmig/cenmigDBport_inside/'
dataport = main + 'cenmigDBport_internal/'
data_sequences = main + 'data_sequences/' #path ที่เก็บไฟล์ของ Database

metadata = main + 'raw_metadata/'
metadata_backup = main + 'metadata_backup/'

query_result_path = main + 'query_result_path/'

backup_metadata_path = main + 'backup_metadata/'

# MTB_sra     = 'Mycobacterium_tuberculosis/sra/'
# MTB_fasta   = 'Mycobacterium_tuberculosis/fasta/'
# SAL_sra     = 'Salmonella_enterica/sra/'
# SAL_fasta   = 'Salmonella_enterica/fasta/'
# STAU_sra    = 'Staphylococcus_aureus/sra/'
# STAU_fasta  = 'Staphylococcus_aureus/fasta/'
# STAG_sra    = 'Streptococcus_agalactiae/sra/'
# STAG_fasta  = 'Streptococcus_agalactiae/fasta/'

#------------------------------------ Deley Time -------------------------------------------#

delay_time = 0

#------------------------------------ depthNode -------------------------------------------------#

depth_node = 0

#------------------------------------ Metadata -------------------------------------------------#
sra_path = metadata + 'SRA_info_table/*/SraRun*'
amr_path = metadata + '*metadata.csv'
assembly_path = metadata + '*assembly_summary.txt'
ungeo_file = main + 'UNGEO.csv'
df_colnames = ['Run','Serovar','Organism','Sample_Name','BioProject','BioSample','Experiment',
                    'isolation_source','host','geo_loc_name_country','Collection_date','Center_Name','geo_loc_name_country_continent',
                    'lat_lon','host_disease','host_disease_stage','host_age','host_sex','Assay_Type', 'LibrarySource','LibrarySelection',
                    'LibraryLayout','Platform','Instrument']

species = ['Salmonella enterica', 
           'Mycobacterium tuberculosis', 
           'Staphylococcus aureus', 
           'Streptococcus agalactiae']



#==================================== End Setting ===========================================#