#! /usr/bin/env R
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  1 13:34:19 2022

@author: Admin
"""

import setting
import util
import pandas as pd
import numpy as np
import subprocess
import os
import shutil

def summarize_data(file_out, input_file):
    print('Please make sure R module is loaded before run')
    print('Load R module by using "ml R"')
    if not os.path.exists(file_out):
        os.makedirs(file_out)
    
    species_txt, species = select_species(file_out)
    data_metadata, data_resist = get_data(input_file, species)
    data_metadata = convert_datetime(data_metadata)
    metadata_out_filename, resist_out_filename = data_for_R(data_metadata, data_resist, file_out)
    run_R(file_out)
    delete_tmp(metadata_out_filename, resist_out_filename, species_txt)
    print('Finished')
    
def select_species(file_out):
    print('Which species to run (more than one use (,) to seperate for Run all type ALL)')
    print('species available')
    print(setting.species)
    species_in = input('Type here :')
    if species_in.upper() == 'ALL':
        species = setting.species
    else:
        species_in = species_in.replace("'", "")
        species_in = species_in.replace(", ", ",")
        species_in = species_in.replace(" ,", ",")
        species_in = species_in.replace(" , ", ",")
        species = species_in.split(',')
    
    # Write species list for R
    species_txt = file_out + 'spp_R_tmp.txt'
    f = open(species_txt, "w")
    f.write((',').join(species))
    f.close()
   
    ## Edit species var for next function
    suffix = '.*'
    species = [sp + suffix for sp in species]
    species = ('|').join(species)
    return species_txt, species

def get_data(input_file, species):
    
    ## Metadata
    if input_file == True:
        dbCollection = util.connect_database()
        data = dbCollection.find({'Organism' : {'$regex' : species}})
        data_metadata = pd.DataFrame.from_dict(data)
        print(species)
    elif '.txt' in input_file:
        #### query
        data_metadata = util.query(input_file)
    elif '.csv' in input_file:
        #### CSV
        data_metadata = pd.read_csv(input_file, low_memory=False)

    # Resistant data
    db_resist = util.connect_database(collection = 'Resistant')
    db_resist = db_resist.find({'cenmigID' : { '$in' : list(data_metadata['cenmigID']) } })
    data_resist = pd.DataFrame.from_dict(db_resist)
    return  data_metadata, data_resist

def convert_datetime(data_metadata):
    # Convert datetime in pandas is more powerful
    data_metadata['Year'] = pd.to_datetime(data_metadata['Collection_date'], errors='coerce', utc=True)
    data_metadata['Year'] = pd.DatetimeIndex(data_metadata['Year']).year
    return(data_metadata)

def data_for_R(data_metadata, data_resist, file_out):
    print('Prepare data for run in R')
    metadata_out_filename = file_out + 'metadata_R_tmp.csv'
    resist_out_filename =  file_out + 'resist_R_tmp.csv'
    
    # Add ISO3 code to data
    ungeo = pd.read_csv(setting.ungeo_file, low_memory=False)
    ungeo = ungeo[['Country','ISO-alpha3 Code']]
    data_metadata = pd.merge(data_metadata, ungeo,
                             how = 'left',
                             left_on='geo_loc_name_country_fix',
                             right_on = 'Country',
                             indicator = False)
    
    data_metadata.to_csv(metadata_out_filename, index = False)
    data_resist.to_csv(resist_out_filename, index = False)
    
    return metadata_out_filename, resist_out_filename

def run_R(file_out):
    print('Running in R')
    cmd_R ='Rscript --vanilla ' + setting.main + 'R_graph.R ' + file_out 
    print(cmd_R)
    subprocess.run([cmd_R], shell = True)

def delete_tmp(metadata_out_filename, resist_out_filename, species_txt):
    file_ls = [metadata_out_filename, resist_out_filename, species_txt]
    for file in file_ls:
        os.remove(file)

#summarize_data(file_out = 'tmp/', input_file = True)

    
    