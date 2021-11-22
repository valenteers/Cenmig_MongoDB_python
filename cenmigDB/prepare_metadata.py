# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 13:44:58 2021

@author: User
"""
from import_modules import *
# import subprocess
# import re
# import setting
# import ftplib
# import os
# import pandas as pd


def prepare_metadata(spp = setting.species, file_out = setting.metadata):
    for sp in spp:
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
                lastest_ver = [int(i.rsplit('.', 1)[1]) for i in patho_ls] # Find lastest ver ## Spilt version and convert to int
                lastest_ver = max(lastest_ver) # Select max
                pathogen_version = list(patho_ls[patho_ls.str.contains(str(lastest_ver), case = True, regex =True)]).pop()
                print(pathogen_version)
            
                # Download PathogenDB
                pathogen_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pathogen/Results/' + sp + pathogen_version + '/Metadata/' + pathogen_version + '.metadata.tsv'
                cmd_pathogen = "wget -q " +  pathogen_ftp + ' -P ' + file_out
                download_pathogen = subprocess.run([cmd_pathogen], shell=True )
            else:
                print(sp)
                
            # Download assembly
            assembly_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/' + sp + '/assembly_summary.txt'
            cmd_assembly = "wget -q " +  assembly_ftp + ' -P ' + file_out
            download_pathogen = subprocess.run([cmd_assembly], shell=True )
            os.rename(file_out + 'assembly_summary.txt', sp + '_assembly_summary.txt' )
        
        elif sp == 'Salmonella_enterica':
            sp_sal = 'Salmonella' # In PathogenDB use 'Salmonella' name
            FTP_HOST = "ftp.ncbi.nlm.nih.gov"
            FTP_USER = 'anonymous'
            FTP_PASS = '@anonymous'
            ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS, timeout=6000)
            # Pathogen
            path = 'pathogen/Results/' + sp
            ftp.cwd(path) # Change path
            
            # Retrive lastest version from ftp directories
            patho_ls = pd.Series(ftp.nlst()) # Return list of file
            ftp.quit()
            patho_ls = patho_ls[patho_ls.str.contains('^P.*\d$', case = True, regex =True)]
            lastest_ver = [int(i.rsplit('.', 1)[1]) for i in patho_ls] # Find lastest ver
            lastest_ver = max(lastest_ver) # Select max
            pathogen_version = list(patho_ls[patho_ls.str.contains(str(lastest_ver), case = True, regex =True)]).pop()
            print(pathogen_version)
            
            # Download PathogenDB
            pathogen_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/pathogen/Results/' + sp_sal + pathogen_version + '/Metadata/' + pathogen_version + '.metadata.tsv'
            cmd_pathogen = "wget -q " +  pathogen_ftp + ' -P ' + file_out
            download_pathogen = subprocess.run([cmd_pathogen], shell=True )
            os.rename(file_out + 'assembly_summary.txt', sp + '_assembly_summary.txt' )
            
            # Download assembly
            assembly_ftp = 'ftp://ftp.ncbi.nlm.nih.gov/genomes/genbank/bacteria/' + sp + '/assembly_summary.txt'
            cmd_assembly = "wget -q " +  assembly_ftp + ' -P ' + file_out
            download_pathogen = subprocess.run([cmd_assembly], shell=True )

