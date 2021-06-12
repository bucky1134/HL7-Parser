# HL7-Parser

This is hl7 parser based on python2.7 to parse healthcare hl7 standard files to csv format and ingest data in postgres database
------Step to execute hl7parser-------

1.Create virtual enviroment for python 2.7 and install dependencies from requirement.txt file.
2.Add ingestion creds in config.py file
3.Add tableinfo in table.json file 
4. put all the files in a folder .
5. execute below command to parse files in local
    python hl7parser.py 'path to file location'
6. after parsing output folder created and for every segment there is a final csv generated like Final_MSH,Final_PID.
7. to ingest these direct files after parsing remove that output folder and then again run hl7parser with below command.
8. execute below command to parse files in local and directly ingest to db 
    python hl7parser.py 'path to location' --ingestion true
    

---Note:  This parser fails if different encoding characters are present in raw file.-----
