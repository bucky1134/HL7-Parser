import os
import optparse
import pandas as pd
import boto
import boto3
import shutil
from config import *
import time
import re
import logging
import hl7
import csv
import sys
from itertools import izip_longest
import json
import shutil
import requests
import psycopg2
import datetime
from datetime import datetime
class ingestion:
    def __init__(self,foldername):
        self.foldername=foldername+'output'
    def get_connection(self):
        tconnection = psycopg2.connect(user=config['dbuser'],password=config['dbpassword'],host=config['dbhost'],port=config['dbport'],database=config['dbdatabase'])
        return connection
    def executeingestion(self,connection,cursor):
        try:
            #print('Ingestion started->')          
            with open(config['tablefileloc']+'table.json','r') as json_file:
                table=json.load(json_file)
                loc=os.listdir(self.foldername+'/')
                for i in table:
                    for file in loc:
                        if str(file).startswith('Final_'+str(i)):
                            print(file)
                            with open(os.path.join(self.foldername,file), 'r') as f:
                                reader = csv.reader(f,delimiter=',',quotechar='"')
                                cols=reader.next()
                                strcolumn=str(cols).replace('[','').replace(']','').replace("'",'"')
                                cursor.execute("CREATE TABLE IF NOT EXISTS l1."+str(table[i]).lower()+"(ingestion_datetime timestamp);")
                                for column in cols:
                                    cursor.execute("DO $do$ BEGIN if not exists (select * from information_schema.columns where table_name='"+str(table[i]).lower()+"' and column_name='"+str(column)+"' and table_schema='l1') THEN alter table l1."+str(table[i]).lower()+' add "'+str(column)+'" varchar(500); end if; end; $do$')
                                for row in reader:
                                    #print(row)
                                    l=[]
                                    for x in row:
                                        l.append(str(x).replace('"','').replace("'",''))
                                    cursor.execute("insert into l1."+str(table[i]).lower()+"("+strcolumn+",ingestion_datetime)values("+str(l).replace('[','').replace(']','')+",now())")
                connection.commit()
        except Exception as e:
            print('error occured: '+str(e))
            
            raise
class csvwriter:
    def __init__(self, foldername):
        x = datetime.now().strftime('%Y%m%d%H%M%S')
        self.filename = 'output'
        self.foldername = foldername+self.filename
        self.filecounter = 0
    def createdirectory(self):
        try:
            if os.path.exists(self.foldername):
                shutil.rmtree(self.foldername+'/',ignore_errors = False)
            if not os.path.exists(self.foldername):
	        os.makedirs(self.foldername)
        except Exception as e:
            print(e)
            raise
    def combinefile(self,segment):
        print('Combining File..')
	for i in segment:
            if str(i) =='' or str(i)==' ':
                continue
            listfile=os.listdir(self.foldername+'/'+str(i)+'/')
            dff=[]
            for n in listfile:
                if str(n).startswith(str(i)):
                    csv=pd.read_csv(self.foldername+'/'+str(i)+'/'+str(n),delimiter='|')
                    dff.append(csv)
                    os.remove(self.foldername+'/'+str(i)+'/'+str(n))
            mergedff=pd.concat(dff,ignore_index=True,sort=True)
            mergedff.to_csv(self.foldername+'/Final_'+str(i)+'.csv',index=False,quotechar='"')
    def writefile(self, datadict,files):
        try:
            keys = sorted(datadict.keys())
            for key in datadict:
                if not os.path.exists(self.foldername+'/'+str(key)):
                    os.makedirs(self.foldername+'/'+str(key))
                csv_file = self.foldername+'/'+str(key)+'/'+str(key)+'_'+str(files)+'.CSV'
                with open(csv_file, 'ab') as csvfile:
                    writer = csv.writer(csvfile,delimiter='|',quotechar = '"')
                    writer.writerow(datadict[key].keys())
                    writer.writerows(izip_longest(*datadict[key].values()))
        except Exception as e:
            print(e)
            raise
class hl7parse:
    def __init__(self, filelocation,ingestion):
        self.name = 'hl7parser'
        self.ingestion=ingestion
        self.setsegment=set()
        self.filelocation = filelocation
        self.datadict = {}
        self.tempdict = {}
        self.message_control_id = ''
    def getcomponentlist(self, feild):
        data = str(feild).split('^')
        # seprate the components on the basis of standerd seprater(^)
        return data
    def get_longestdictvalue(self):
        max = 0
        for value in self.tempdict.values():
            if len(value) > max:
                max = len(value)
        return max
    def updatedict(self, key):
        if(len(self.datadict[key])) != 0:
            maks = max(self.datadict[key],
                       key=lambda k: len(self.datadict[key][k]))
            maxlen = len(self.datadict[key][maks])
        else:
            maxlen = 0
        for itr, item in self.tempdict.items():
            if itr not in self.datadict[key]:
                self.datadict[key][itr] = []
                for i in range(maxlen):
                    l = [' ']
                    self.datadict[key][itr] = self.datadict[key][itr]+l
                self.datadict[key][itr] = self.datadict[key][itr]+item
            else:
                self.datadict[key][itr] = self.datadict[key][itr]+item
        for itr, item in self.datadict[key].items():
            if itr not in self.tempdict:
                l = [' ']
                self.datadict[key][itr] = self.datadict[key][itr]+l
    def createdictkey(self, dictkey):
        if dictkey not in self.datadict:
            self.datadict[dictkey] = {}
    def parsevalue(self, fileheader, feild, headercounter):
        if len(feild) == 1:
            x = self.getcomponentlist(feild)
            for i in range(len(x)):
                self.tempdict[str(fileheader)+'.' +
                              str(headercounter)+'.'+str(i+1)] = [x[i]]
        else:
            additionaldict = {}
            for component in feild:
                temp = {}
                x = self.getcomponentlist(component)
                matchingdict = {}
                for itr in range(len(x)):
                    header = str(fileheader)+'.' + \
                        str(headercounter)+'.'+str(itr+1)
                    temp[header] = [x[itr]]
                    matchingdict[header] = ''
                    if header not in additionaldict.keys():
                        additionaldict[header] = ''
                    if header not in self.tempdict.keys():
                        self.tempdict[header] = []
                        self.tempdict[header] = self.tempdict[header] + \
                            temp[header]
                    else:
                        self.tempdict[header] = self.tempdict[header] + \
                            temp[header]
                for itr in additionaldict.keys():
                    if itr not in matchingdict.keys():
                        l = [' ']
                        self.tempdict[itr] = self.tempdict[itr]+l
    def extractmessage(self, message, file):
        # parse hl7 message to get data segment wise like MSH, PID, PV!
        message = hl7.parse(message)
        for segment in message:
            self.setsegment.add(str(segment[0]))
            headercounter = 0
            self.tempdict = {}
            self.createdictkey(str(segment[0]))
            for feild in segment:
                self.parsevalue(segment[0], feild, headercounter)
                headercounter = headercounter+1
            if str(segment[0]) == 'MSH':
                self.message_control_id = self.tempdict['MSH.10.1']
            self.tempdict['msg_control_id'] = self.message_control_id
            self.tempdict['filename'] = [str(file)]
            longest = self.get_longestdictvalue()
            if longest > 1:
                for i, r in self.tempdict.items():
                    if len(r) == 1:
                        for itr in range(1, longest):
                            temp = {}
                            temp[i] = [self.tempdict[i][0]]
                            self.tempdict[i] = self.tempdict[i]+temp[i]
            self.updatedict(str(segment[0]))
    def parsemessage(self,connection,cursor):
        loc = os.listdir(self.filelocation)
        obj = csvwriter(self.filelocation)
        if obj:
            obj.createdirectory()
            for files in loc:
                print('Parsing file: '+str(files))
                 
                with open(os.path.join(self.filelocation, files), 'r') as f:
                    x = f.read()
                t = x.replace('\n', '\r')
                self.extractmessage(t, files)
                obj.writefile(self.datadict,files)
                self.datadict={}
            obj.combinefile(self.setsegment)
            if self.ingestion:
                indb=ingestion(self.filelocation)
                indb.executeingestion(connection,cursor)
if __name__ == "__main__":
    try:
        parser=optparse.OptionParser()
        parser.add_option('-q','--ingestion',action="store",dest="ingestion_flag",help="ingestion flag",default=False)
        options,args=parser.parse_args()
        path=args[0]
        ingestion_flag=False
        if options.ingestion_flag=='true':
            ingestion_flag=True
        cursor=None
        print('Job Execution started..')
        print('Connecting to database for ingestion if set to TRUE')
	try:
            connection= psycopg2.connect(user=config['dbuser'], password=config['dbpassword'],
                                          host=config['dbhost'], port=config['dbport'], database=config['dbdatabase'])
            if connection:
                print('Postgres connection established.')
		cursor = connection.cursor()
        except Exception as e:
            message = 'Error while connecting to database'
            print(message)
            raise
        object = hl7parse(path,ingestion_flag)
        object.parsemessage(connection,cursor)
    except Exception as e:
        message = 'Job execution failed due to :' + str(e)
        print(message)
    finally:
        cursor.close()
        connection.close()
        print('Database connection ended..')
        print('Job execution ended..')
