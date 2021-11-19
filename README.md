# cenmigDB
This program is using for interact with the cenmigDB

**usage: python3 main.py [func]**
           
This program is using for interact with the CenmigDB via command-line \
For creating new collection or DB of MongoDB please use mongoimport 


**positional arguments:**
<pre>
  {prepare_data,metadata_ncbi,metadata_submit,search,replace_records,update_fields,delete_records,backup,download_ncbi,get_file,store_seqfile}
    prepare_data        Download tsv and txt metadata from NCBI PathogenDB and AssemblyDB
    metadata_ncbi       Create and reformat data from ncbi from NCBI to csv file for updating CenmigDB
    metadata_submit     Reformatting submission_file to csv file for updating CenmigDB
    search              Query metadata based on keyword in txt file
    replace_records     Update record by replacing entire old record
    update_fields       Add data fields(Columns) to records
    delete_records      Delete records
    backup              Backup all metadata in CenmigDB
    download_ncbi       Download file from ncbi based on Run and asm_acc from input csv
    get_file            Copy file SRA and fasta to user with Run and asm_acc from csv
    store_seqfile       Moving file to storage location based on spp. and filetype
</pre>

