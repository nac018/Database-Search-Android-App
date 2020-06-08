#!/usr/bin/env python
# coding: utf-8

# In[1]:



import mysql.connector
import pandas as pd
import numpy as np
import sys
import re
import requests
import urllib.parse
import json

def tablelist(databasename):
    namelist = list()
    cnx = mysql.connector.connect(user='project', password='inf551', host='127.0.0.1', database=databasename)
    cursor = cnx.cursor()
    query = "show tables;"
    cursor.execute(query)
    for table in cursor:
        #print(type(table[0]))
        namelist.append(table[0])
    cursor.close()
    return namelist


def columnname(tablename,databasename):
    attributes = list()
    cnx = mysql.connector.connect(user='project', password='inf551', host='127.0.0.1', database=databasename)
    cursor = cnx.cursor()
    query = "select * from "+tablename+";"
    cursor.execute(query)
    attributes = [desc[0] for desc in cursor.description]
    #cursor.close()
    #attributes[0] = "# " + attributes[0]
    return attributes

def todf(nameoftable,databasename):
    #databasename = 'world'
    filepath = nameoftable + '.csv'
    a = columnname(nameoftable,databasename)
    cnx = mysql.connector.connect(user='project', password='inf551', host='127.0.0.1', database=databasename)
    cursor = cnx.cursor()
    query = 'select * from '+nameoftable +";"
    cursor.execute(query)
    arrays = list()
    
    for entry in cursor:
        entry = list(entry)
        #print(entry)
        for i in range(len(entry)):
            entry[i] = str(entry[i])
            if len(entry[i])== 0 :
                entry[i] ='None'
                #print(entry[i])
        arrays.append(entry)
    
    df = pd.DataFrame(np.array(arrays),columns=a) 
    #df.to_csv(filepath, index=False,)
    cursor.close()
    return df


def transjson(nameoftable,databasename):
    df = todf(nameoftable,databasename)
    outputpath = (nameoftable+".json")
    if nameoftable == "city":
        nodename = 'city'
        df["PK"] = df["ID"]
        df.reset_index
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    elif nameoftable =="country":
        nodename = 'country'
        df['CountryCode'] = df["Code"]
        df["PK"] = df["Code"]
        #df = df.drop(["Code2"])
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    elif nameoftable =="countrylanguage":
        nodename = 'countrylanguage'
        df['PK'] = df["CL_ID"]
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable == "nbaplayers":
        nodename = 'nbaplayers'
        df["PK"]= df["PLAYER_ID"]
        df["NAME"] = df["PLAYER_NAME"]
        df=df.drop(columns=['PLAYER_NAME'])
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable == "nbasalary":
        nodename = 'nbasalary'
        df["PK"]=df["PlayerID"]
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable == "nbateam":
        nodename = 'nbateam'
        df["PK"]=df["IDT"]
        df["TEAM_ABBREVIATION"] = df["IDT"]
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable =='worldcupmatches':
        nodename = 'worldcupmatches'
        df['PK']=df['IDM']
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable == 'worldcupplayers':
        nodename = 'worldcupplayers'
        df["PK"] = df["IDP"]
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
    elif nameoftable == 'worldcups':
        nodename = 'worldcups'
        df["PK"] = df["Year"]
        df = df.set_index("PK")
        json = df.to_json(orient = 'index')
    
        
    return '{"' + nodename + '": ' + json + '}'


def uploaded(url, data, tips):
    try:
        putresponse = requests.patch(url+'.json', data)
        if putresponse.status_code == 200:  
            print("upload {} completed".format(tips))
            
        else:
            print ("upload failed because:{}".format(putresponse.text))
    except:
        print("upload {} failed".format(tips))

        
def Reset_database(url):
    try:
        delresponse = requests.delete(url + '.json')
        if delresponse.status_code ==200:
            print("Reset firebase database successfully")
        else:
            print("Reset firebase failed and the reason is: {}".format(delresponse.text))
    except:
            print(("Reset firebase failed"))
    return


def inverted_index(tablename,databasename):
    data = todf(tablename,databasename)
    nameset = {}


    for entry in data.values:       
        pair = list(zip(data.columns,entry))
        a = 1
        for column, value in pair:
            if a ==1:
                primarykey = column
                a =a+1
            value = value.replace('.','%2e').lower()
            if value == None:
                continue
            if value == "":
                continue
            
            values = value.split(" ")
            
            if len(values)>1:
                result = {}
                result['Table'] = tablename
                result['ID'] = entry[0]
                result['column'] = column
                result['primarykey'] = primarykey
                nameset[value] = []
                nameset[value].append(result)
                if value in all_set:
                    all_set[value] = all_set[value] + nameset[value]
                else:
                    all_set[value] = nameset[value]
                    
            value = value.replace(" ","-").replace("_","-")
            values= value.split('-')                    
        
            for value1 in values:
                value1 = re.sub('[\$\#\[ \] \/]', '', value1)
                if value1 == None:
                    continue
                if value1 == "":
                    continue
                
                nameset[value1] = []
                result={}
                result['Table'] = tablename
                result['ID'] = entry[0]
                result['column'] = column
                result['primarykey'] = primarykey
            
                nameset[value1].append(result)
            
                if value1 in all_set:
                    all_set[value1] = all_set[value1] + nameset[value1]
                else:
                    all_set[value1] = nameset[value1]
                

                
                
def inverted_index2(tablename,databasename,primarykey):
    data = todf(tablename,databasename)
    nameset = {}
    for entry in data.values:       
        pair = list(zip(data.columns,entry))
        a = 1
        for column, value in pair:
            if a ==1:
                primarykey = column
                a = 2
            value = value.lower()
            try:
                value = value.replace('.','%2e')
            except:
                value = value
            if value == None:
                continue
            if value == " ":
                continue
                
            
            value = re.sub('[\$\#\[\]\/]', '', value)
            values = value.split(" ")
            if len(values)>1:
                result = {}
                result['Table'] = tablename
                result['ID'] = entry[0]
                result['column'] = column
                result['primarykey'] = primarykey
                nameset[value] = []
                nameset[value].append(result)
                if value in all_set:
                    all_set[value] = all_set[value] + nameset[value]
                else:
                    all_set[value] = nameset[value]
            
            value = value.replace(" ","-").replace("_","-")
            values= value.split('-')
            
            for value1 in values:
                value1 = re.sub('[\$\#\[ \] \/]', '', value1)
                if value1 == None:
                    continue
                if value1 == "":
                    continue
               
                nameset[value1] = []
                result={}
                result['Table'] = tablename
                result['Column'] = column
                result['ID'] = entry[0]
                result['primarykey'] = primarykey
                
                nameset[value1].append(result)
                if value1 in all_set:
                    all_set[value1] = all_set[value1] + nameset[value1]
                else:
                    all_set[value1] = nameset[value1]            
            

nodesname = sys.argv[2]
datasetname = sys.argv[1]            

#nodesname = 'Project'
#datasetname = 'project'

myurl = "https://expe-a3855.firebaseio.com/"+nodesname
Reset_database(myurl)

SLASH = "/"
INDEX = "index"

for tables in tablelist(datasetname):
    data= transjson(tables, datasetname)
    uploaded(myurl,data,tables)





url = myurl + "/NBAinverted"
all_set={}
print("Invert and upload index of NBA data... ")
inverted_index('nbaplayers',datasetname)
inverted_index('nbateam',datasetname)
inverted_index('nbasalary',datasetname)
data = json.dumps(all_set)
uploaded(url,data,"NBA Information")


url = myurl + "/WCinverted"
print("Invert and upload index of WorldCup data... ")
all_set={}
inverted_index('worldcupmatches',datasetname)
inverted_index('worldcups',datasetname)
data = json.dumps(all_set)
uploaded(url,data,"World Cup Information")

all_set={}
inverted_index('worldcupplayers',datasetname)
data = json.dumps(all_set)
uploaded(url,data,"World Cup Players Information")



print("Invert and upload index of World data... ")
url = myurl + "/Cinverted"
all_set={}
inverted_index2('city',datasetname,'ID')
inverted_index2('countrylanguage',datasetname,'CL_ID')
inverted_index2('country',datasetname,'Code')
data = json.dumps(all_set)
print("this might take 2 minutes")
uploaded(url,data,"World (Country) Information")


#data = json.dumps(all_set)
#uploaded(url,data,"World (City) Information")


#all_set={}#
#inverted_index2('countrylanguage',datasetname,'CL_ID')
#data = json.dumps(all_set)
#uploaded(url,data,"World (Countrylanguage) Information")


#all_set={}
#inverted_index2('country',datasetname,'Code')
#data = json.dumps(all_set)
#uploaded(url,data,"World (Country) Information")


# In[6]:


#myurl = "https://expe-a3855.firebaseio.com/Project/"
#Reset_database(myurl)

