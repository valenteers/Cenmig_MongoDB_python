#!/usr/bin/env python3  

#==================================== Config Setting =====================================#

#------------------------------------ Database MongoDB -------------------------------------#


database_url = 'mongodb://localhost'
database_port = 27017
# database_name = 'cenmigDBtest' #ชื่อดาต้าเบส
database_name = 'cenmigDB' # Testing
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
sra_meta = metadata + 'Sra_info_table/'
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

#------------------------------------ Analyze -------------------------------------------------#

analyze_query = {'sub_region' : 'South-eastern Asia'}

#------------------------------------ Metadata -------------------------------------------------#
search_text = ['((("Mycobacterium tuberculosis"[Organism] OR mycobacterium tuberculosis[All Fields] OR "Mycobacterium tuberculosis complex sp."[orgn]) AND ("biomol dna"[Properties] AND "strategy wgs"[Properties])))' ,
               '((("Salmonella enterica"[Organism] OR Salmonella enterica[All Fields]) AND "Salmonella enterica"[orgn] AND ("biomol dna"[Properties] AND "strategy wgs"[Properties]))) ',
               '((("Staphylococcus aureus"[Organism] OR Staphylococcus aureus[All Fields]) AND "Staphylococcus aureus"[orgn] AND ("biomol dna"[Properties] AND "strategy wgs"[Properties])))',
               '((("Streptococcus agalactiae"[Organism] OR Streptococcus agalactiae[All Fields]) AND "Streptococcus agalactiae"[orgn] AND ("biomol dna"[Properties] AND "strategy wgs"[Properties])))']


sra_path = metadata + 'SRA_info_table/*/SraRun*'
amr_path = metadata + '*metadata.csv'
assembly_path = metadata + '*assembly_summary.txt'
ungeo_file = main + 'UNGEO.csv'
bio_attr_file = 'biosample_attr.tsv'
df_colnames = ['Run','Serovar','Organism','Sample_Name','BioProject','BioSample','Experiment',
                    'isolation_source','host','geo_loc_name_country','Collection_date','Center_Name','geo_loc_name_country_continent',
                    'lat_lon','host_disease','host_disease_stage','host_age','host_sex','Assay_Type', 'LibrarySource','LibrarySelection',
                    'LibraryLayout','Platform','Instrument']

combine_columns_dict = { 'Run'      : ['run'],
                        'Serovar'   : ['serovar'],
                        'Organism'  : ['organism'],
                        'Sample_Name' :['sample_name'],
                        'BioProject'  : ['bioproject'],
                        'BioSample'   : ['biosample'],
                        'Experiment'  : ['experiment'],
                        'isolation_source' : ['isolation_source'],
                        
                        'host'        : ['host','host_(scientific name)', 'host_name', 'host_organism', 'host_scientific_name',	'host_species', 
                                       'host_speciess', 'nat_host', 'nathost', 'specific_host', 'specifichost'],
                        
                        'host_age' : ['host_age'],
                        'host_sex' : ['host_sex', 'host_gender'],
                        'host_disease' : ['host_disease', 'host_disease_stat'],
                        'host_disease_stage' : ['host_disease_stage'],
                        'geo_loc_name_country' : ['geo_loc_name_country','country','geo_loc_name','geographic_location_(country_and/or_sea_region)',
                                                  'geographic_location_(country_and/or_sea)','geographic_location_(country_and/or_sea,_region)','geographic_location_(country_and/or_sea,region)',
                                                  'geographic_location_(country)','geographic_location_(country:region,area)','geographic_location_(locality)','geographic_location_country_and_or_sea',
                                                  'geographic_locations','geographic_origin','geographical_location','geographical_location_(country:region,_location)','geolocname'],
                        'geo_loc_name_country_continent' : ['geo_loc_name_country_continent'],
                        
                        'lat_lon' : ['lat_lon','geographic_location_(latitude_and_longitude)','geographic_location_(latitude,_longitude)',
                                     'geographical_location_(lat_lon)','geographical_location_(longitude_and_longitude)','latlon'],
                        
                        'Collection_date' : ['Collection_date','colection_date','collection_date_(yyyymmdd)','collection_year','collectiondate','date_of_collection',
                                             'date_sample_collected','isolation_year','sample_collection_date','sample_date','sampling_date','time_of_sample_collection','year_isolated'],
                        'Center_Name' : ['center_name'],
                        'Assay_Type' : ['assay_Type'], 
                        'LibrarySource' : ['LibrarySource'],
                        'LibrarySelection' : ['LibrarySelection'],
                        'LibraryLayout' : ['LibraryLayout'],
                        'Platform' : ['Platform'],
                        'Instrument' : ['Instrument']}

species = ['Salmonella enterica', 
           'Mycobacterium tuberculosis', 
           'Staphylococcus aureus', 
           'Streptococcus agalactiae',
           'Burkholderia pseudomallei']

species_ncbi = ['Salmonella enterica', 
           'Mycobacterium tuberculosis', 
           'Staphylococcus aureus', 
           'Streptococcus agalactiae']

#------------------------------------ Metadata -------------------------------------------------#
mlst_db = main + 'mlst_dbs/' 
resfinder_db = main + 'resfinder_db/' 

#------------------------------------ Setup -------------------------------------------------#
package_required = ['pandas', 'numpy', 'glob2', 
                    'pymongo','pycountry', 'argparse', 'stringMLST']

folder_list = [data_sequences, dataport, backup_metadata_path, query_result_path, metadata]

#==================================== End Setting ===========================================#

## Create folder for store data_sequenxes
for file_type in ['/sra/', '/fasta/']:
    for sp in species:
        path = data_sequences + sp.replace(' ', '_') + file_type
        srainfo_path = sra_meta + sp.replace(' ', '_')
        if not os.path.exists(path):
            os.makedirs(path)
        if not os.path.exists(srainfo_path):
            os.makedirs(srainfo_path)
