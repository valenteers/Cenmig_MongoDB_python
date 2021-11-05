#!/usr/bin/env python3  


#==================================== load packages =====================================#
# from cenmigDB import config 
# from cenmigDB import text as txt
from os import close
from cenmigDB import shortcut
from packages.mongoDB import connect

import config
import text as txt

#==================================== end load packages =================================#

# from pymongo import MongoClient 
# from tqdm import tqdm 
# from shutil import copy
# from os import error

# import time
# import pandas as pd 
# import json
# import csv
# import os
# import shutil

# example.exam()

#==================================== Setting Variable =====================================#

#------------------------------------ Deley Time -------------------------------------------#

delayTime = config.delayTime

#==================================== Process ===============================================#

print (txt.txtHelp)
connect = connect.DBconnect(config.DBUrl, config.DBPort, config.DBName, config.DBcollection)
closeConnect = connect.closeConnect(config.DBUrl, config.DBPort)

while depthNode == 1:
    print ('\n================================ Top Menu ======================================\n')
    c = input(txt.choose)
#------------------------------------ check file --------------------------------------------#
    if c in shortcut.check:
        # try:
            print ('\n "Step : check same data ---> sameNameInCSV, sameNameInDB, noSameData"\n')
            path = input(txt.txt.pathQuest)
            print (txt.txt.nowProcess)
            check_same_data(path, databaseUrl, databasePort, databaseName, collectionName, pathSaveCSV, pathSaveDB, pathNoSameData, pathSaveDupId)
            depthNode = complete_and_back()
        # except:
        #     print (txt.errorPath)
#------------------------------------ import & update ----------------------------------------#
    elif c in shortcut.update:
        depthNode = depthNode + 1
        while depthNode == 2:
            print (txt.importUpdate)
            c = input(txt.choose)
            if c in shortcut.noreplace:
                try:
                    print ('\n "Step : import & update ---> donot replace same name"\n')
                    path = input(txt.pathQuest)
                    if path == '': #สุดท้ายอาจจะค้องลบออก เพราะไม่มั่นใจว่าจะมี default ได้จริงหรือไม่
                        path = config.defaultPath #สุดท้ายอาจจะค้องลบออก เพราะไม่มั่นใจว่าจะมี default ได้จริงหรือไม่
                    print (txt.txt.nowProcess)
                    import_noreplace(path, databaseUrl, databasePort, databaseName, collectionName)
                    depthNode = complete_and_back()
                except:
                    print(txt.errorPath)
            elif c in shortcut.replace:
                try:
                    print ('\n "Step : import & update ---> replace same name"\n')
                    path = input(txt.pathQuest)
                    if path == '': #สุดท้ายอาจจะค้องลบออก เพราะไม่มั่นใจว่าจะมี default ได้จริงหรือไม่
                        path = config.defaultPath #สุดท้ายอาจจะค้องลบออก เพราะไม่มั่นใจว่าจะมี default ได้จริงหรือไม่
                    print (txt.nowProcess)
                    import_replace(path, databaseUrl, databasePort, databaseName, collectionName)
                    depthNode = complete_and_back()
                except:
                    print(txt.errorPath)
            elif c in shortcut.new :
                try:
                    print ('\n "Step : import & update ---> same name to new document"\n')
                    path = input(txt.pathQuest)
                    if path == '': #สุดท้ายอาจจะค้องลบออก เพราะไม่มั่นใจว่าจะมี default ได้จริงหรือไม่
                        path = config.defaultPath #สุดท้ายอาจจะลบออก ขึ้นกับว่าเซฟที่พาธสครปเลยดีไหม หรือให้ใช้ defaultPath
                    print (txt.nowProcess)
                    import_new(path, config.DBUrl, config.DBPort, config.DBName, config.DBcollection)
                    depthNode = complete_and_back()
                except:
                    print(txt.errorPath)
            else:
                depthNode = system_key(c,depthNode)

#------------------------------------ End import & update --------------------------------------#

#------------------------------------ find document & show to save or delete --------------------------------------#

    elif c in shortcut.find:
        depthNode = depthNode + 1
        while depthNode == 2:
            print (txt.findDoc)
            try:
                condition = {}
                conditionNumber = int(input(txt.numberCondition))     
                print (txt.exam)                 
                for i in range(conditionNumber):        
                    text = input().split()   
                    condition[text[0]] = text[1]       
                show_find_data(condition, databaseUrl, databasePort, databaseName, collectionName)
                depthNode = depthNode + 1
                while depthNode == 3:
                    print (txt.findDocChoice)
                    c = input(txt.choose)
                    if c in shortcut.saveOrDel:
                        print ('\n "Step : find document ---> save document"\n')
                        typeSave = 1 #use save_doc function
                        try:
                            depthNode = depthNode + 1
                            while depthNode == 4:  
                                path = input(txt.pathQuest)
                                depthNode = check_format_path(depthNode,path,typeSave)
                                pass
                        except:
                            print(txt.errorPath)
                            pass 
                    elif c in shortcut.saveOrDelFile:
                        print ('\n "Step : find document ---> save document & file (fastQ, fasta, …)"\n')
                        typeSave = 2 #use save_doc_file function
                        try:
                            depthNode = depthNode + 1
                            while depthNode == 4:  
                                path = input(txt.pathQuest)
                                depthNode = check_format_path(depthNode,path,typeSave)
                                pass
                        except:
                            print(txt.errorPath)
                            pass 
                    elif c in shortcut.delData :
                        print ('\n "Step : find document ---> delete document"\n')
                        c = input(txt.sureToDel)
                        if c == 'yes' :
                            print (txt.nowProcess)
                            del_doc(condition, databaseUrl, databasePort, databaseName, collectionName)
                            depthNode = complete_and_back()
                        else:
                            depthNode = system_key(c,depthNode)
                            # continue
                    elif c in shortcut.delFile :
                        print ('\n "Step : find document ---> delete document & file (fastQ, fasta, …)"\n')
                        c = input(txt.sureToDel)
                        if c == 'yes' :
                            print (txt.nowProcess)
                            del_doc_file()
                            depthNode = complete_and_back()
                        else:
                            depthNode = system_key(c,depthNode)
                            # continue
                    else:
                        depthNode = system_key(c,depthNode)
            except:
                print(txt.errorConditionOrPath)
                pass
            # else:
            #     depthNode = system_key(c,depthNode)

#------------------------------------ End find document & show to save or delete --------------------------------------#

#------------------------------------ save document & files --------------------------------------#

    elif c in shortcut.save:
        depthNode = depthNode + 1
        while depthNode == 2:
            print (txt.saveByCSVorFind)
            c = input(txt.choose)
            if c in shortcut.findCondition:
                depthNode = depthNode + 1
                while depthNode == 3:
                    print (txt.saveDocFile)
                    c = input(txt.choose)
                    if c in shortcut.saveOrDel:
                        print ('\n "Step : save document & files ---> save only document"\n')
                        typeSave = 1
                        try:
                            condition = {}
                            conditionNumber = int(input(txt.numberCondition))                       
                            print (txt.examCondition)
                            for i in range(conditionNumber):        
                                text = input().split()    
                                condition[text[0]] = text[1]
                            depthNode = depthNode + 1
                            while depthNode == 4:  
                                path = input(txt.pathQuest)
                                depthNode = check_format_path(depthNode,path,typeSave)
                                pass
                        except:
                            print(txt.errorConditionOrPath)
                            pass  
                    elif c in shortcut.saveOrDelFile:
                        print ('\n "Step : save document & files ---> save document + files (fastQ, fasta, ...)"\n')
                        typeSave = 2
                        try:
                            condition = {}
                            conditionNumber = int(input(txt.numberCondition))                       
                            print (txt.examCondition)
                            for i in range(conditionNumber):        
                                text = input().split()    
                                condition[text[0]] = text[1]
                            depthNode = depthNode + 1
                            while depthNode == 4:  
                                path = input(txt.pathQuest)
                                depthNode = check_format_path(depthNode,path,typeSave)
                                pass
                        except:
                            print(txt.errorConditionOrPath)
                            pass
                    else:
                        depthNode = system_key(c,depthNode)
            elif c in shortcut.loadByCSV:
                depthNode = depthNode + 1
                while depthNode == 3:
                    print (txt.saveDocFileByCSV)
                    c = input(txt.choose)
                    if c in shortcut.saveOrDel:
                        print ('\n "Step : save document & files ---> save document by csv index"\n')
                        typeSave = 1
                        depthNode = depthNode + 1
                        while depthNode == 4:  
                                path = input(txt.pathQuest)
                                print (txt.nowProcess)
                                depthNode = load_data_by_csv(path, databaseUrl, databasePort, databaseName, collectionName, loadDBfromCSV, typeSave, depthNode)
                                
                    elif c in shortcut.saveOrDelFile:
                        print ('\n "Step : save document & files ---> save document + files (fastQ, fasta, ...) by csv index"\n')
                        typeSave = 2
                        depthNode = depthNode + 1
                        while depthNode == 4:  
                                path = input(txt.pathQuest)
                                print (txt.nowProcess)
                                depthNode = load_data_by_csv(path, databaseUrl, databasePort, databaseName, collectionName, loadDBfromCSV, typeSave, depthNode)
                                
                    else:
                        depthNode = system_key(c,depthNode)
            else:
                depthNode = system_key(c,depthNode)

#------------------------------------ save document & files --------------------------------------#

#------------------------------------ delete document & files --------------------------------------#

    elif c in shortcut.delData:
        depthNode = depthNode + 1
        while depthNode == 2:
            print (txt.deleteDocFile)
            c = input(txt.choose)
            if c in shortcut.saveOrDel:
                print ('\n "Step : delete document & files ---> delete only document"\n')
                try:      
                    condition = {}
                    conditionNumber = int(input(txt.numberCondition))                
                    print (txt.examCondition)
                    for i in range(conditionNumber):        
                        text = input().split()    
                        condition[text[0]] = text[1]
                    c = input(txt.sureToDel)
                    if c == 'yes' :
                        print (txt.nowProcess)
                        del_doc(condition, databaseUrl, databasePort, databaseName, collectionName)
                        depthNode = complete_and_back()
                    else:
                        depthNode = system_key(c,depthNode)
                except:
                    print(txt.errorConditionOrPath)
                    pass
            elif c in shortcut.delFile:
                print ('\n "Step : delete document & files ---> delete document + files (fastQ, fasta, ...)"\n')
                try:      
                    condition = {}
                    conditionNumber = int(input(txt.numberCondition))                
                    print (txt.examCondition)
                    for i in range(conditionNumber):        
                        text = input().split()    
                        condition[text[0]] = text[1]
                    c = input(txt.sureToDel)
                    if c == 'yes' :
                        print (txt.nowProcess)
                        del_doc_file(condition, databaseUrl, databasePort, databaseName, collectionName)
                        depthNode = complete_and_back()
                    else:
                        depthNode = system_key(c,depthNode)
                except:
                    print(txt.errorConditionOrPath)
                    pass
            else:
                depthNode = system_key(c,depthNode)

#------------------------------------ delete document & files --------------------------------------#

    else:
        depthNode = system_key(c,depthNode)

#==================================== End Process ===============================================#