# cenmigDB
This program is using for interact with the cenmigDB

usage: CENMIGDB_run.py [-h] {make_db,update_db,search,export_sequences,summarize_data} ...

This program is using for interact with the CenmigDB via command-line for creating new collection or DB of MongoDB
please use mongoimport

<pre>
positional arguments:
  {make_db,update_db,search,export_sequences,summarize_data}
    make_db             Download, Reformat, and Update to MongoDB
    update_db           Update, Replace, Update to ResistantDB or download seq file from ncbi
    search              Query metadata based on keyword in txt file or export all metadata
    export_sequences    Copy sequence files based on csv file shopping list
    summarize_data      Summarize data and export result to csv and png
    setup_db            Set up folder and required python library

optional arguments:
  -h, --help            show this help message and exit
</pre>

