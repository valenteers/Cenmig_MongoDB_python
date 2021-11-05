#!/usr/bin/env python3  

#==================================== Setting Text ==========================================#

#------------------------------------ Text System -------------------------------------------#

choose_anything = '\n * What choice are you need? \n'
what_path = '\n * What "path file" you need? \n'
complete = '\n * Complete !!! \n'
donot_have_this_command = '\n * Cannot use this command \n'
exit_program ='\n * Good Bye \(^-^)/ \n'
go_to_home_menu = '\n * Back to home menu... \n'
now_process = '\n * Begin Process!!! \n'
now_home_menu = '\n * Now !!! This on home menu \n'
request_condition = '\n * Search in condition? \n'
condition_format = '\n * Example "name_column data_you_need". If more then 1 condition default by "and". (Now!! Can search only string)\n'
how_much_condition = '\n * How much condition? (int only and below then 0 will be search all)\n'
default_path = '\n * No anykey --> Use default path... \n'
sure_to_delete = '\n * Are you sure to delete press key "yes" need go back press key "b" and "Enter"\n'
error_format_path = '\n * Please use format path exam "/home/your_user_name/your_file_name.csv"\n'
error_condition_or_path = '\n * Condition or Path is wrong format\n'
empty_folder = '\n Donot have any file in folder\n'


home_menu ='''
\n================================ home menu ===================================\n
    Use "1" or "c" or "check" to check data file.csv --> duplicate.csv
    Use "2" or "up" or "update" to import & update by CSV
    Use "3" or "f" or "search" to search data by "condition"
    Use "4" or "s" or "download" or "export" to download data by "condition"
    Use "5" or "d" or "del" or "delete" to delete data by "condition"
    Use "7" or "e" or "exit" to exit (Except "condition" or "path")
    Use "8" or "h" or "help" to show tutorial (Except "condition" or "path")
    Use "9" or "ex" or "exam" to show example (Except "condition" or "path")
    Use "0" or "m" or "home" or "menu" to "home menu" page (Except "condition" or "path")
    Use "b" or "back" to back one step (Except "condition" or "path")
\n===============================================================================\n
            '''

how_to_use_this_program = '''
\n===============================================================================\n
---------------------------------- Example ------------------------------------
\n===============================================================================\n
 -"Basic Step" you can use step by step with guideline to complete (easy to use)
\n-------------------------- Example import & update ----------------------------\n
 Show how to update by replace data step by step.\n
	-Step:1 use "up" + press "Enter" and get guideline 
		(Choose anything --> donot replace "nore", replace "re" or newdata "new")
	-Step:2 use "re" for replace data + press "Enter" and get guideline 
		(What "path file" you need?) 
	-Step:3 add path "/home/your_user_name/your_file_name.csv" + press "Enter" and wait until show 
		(Complete !!!) and (Back to home menu...)
\n===============================================================================\n
                '''

import_update = '''
\n============================ import & update ==================================\n
            Use "1" or "nore" or "noreplace" to donot replace data by duplicate index column
            Use "2" or "re" or "replace" to replace data by duplicate index column
            Use "3" or "new" or "newdata" with duplicate index column to new row
\n===============================================================================\n
        '''

search_data_and_show = '\n================= search data & show to download or delete =======================\n'
download_or_delete = '''
\n===============================================================================\n
            Use "1" or "d" to download data
            Use "2" or "df" to download data & file (fastQ, fasta, …)
            Use "4" or "deld" to delete data
            Use "5" or "deldf" to delete data & file (fastQ, fasta, …)
\n================================================================================\n
'''
download_choise = '''
\n============================ download data & files =============================\n
            Use "1" or "f" to search & download data or files (fastQ, fasta, ...)
            Use "2" or "l" to download data by index to csv file
\n================================================================================\n
'''
download_data_by_csv = '''
\n============================ download data & files by csv index ================\n
            Use "1" or "d" to download data by csv index column
            Use "2" or "df" to download data + files (fastQ, fasta, ...) by csv index
\n================================================================================\n
'''
download_data_or_file = '''
\n============================ download data & files =============================\n
            Use "1" or "d" to search & download data
            Use "2" or "df" to search & download data + files (fastQ, fasta, ...)
\n================================================================================\n
'''

delete_data_file = '''
\n============================ delete data & files ===========================\n
            Use "1" or "d" to delete only data
            Use "2" or "df" to delete data & file (fastQ, fasta, ...)
\n================================================================================\n
'''

#==================================== End Text ===========================================#