# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:15:54 2019

@author: H27346
"""

from tkinter import *
from tkinter import filedialog
import pandas as pd
import json
import os
import re
from pandas.api.types import is_string_dtype
import pyodbc
import datetime
            
from xlutils.copy import copy    
from xlrd import open_workbook



fields = {'Feldtrennzeichen':',','Header_ausgeben':True,'Codepage':'utf-8','Fliesszahlenformat':'None','Textqualifizierer':'None','Dezimalzeichen':'.', 'Datumsformat':'%Y-%m-%d %H:%M:%S','Umlautbereinigung':False}

file = open('Params.txt','r')
global p
p=""
global data
try:
    data = json.load(file)
except:
    data={}
        
def UploadAction(event=None):
    global param

    file = open('Params.txt','w')
    if ents['Feldtrennzeichen'].get()=='None':
        sep1=None
    else:
        sep1=ents['Feldtrennzeichen'].get() 
        
    if ents['Header_ausgeben'].get()=='None':
        header1=None
    elif ents['Header_ausgeben'].get().lower()=='false' or ents['Header_ausgeben'].get()=='0':
        header1=False 
    else:
        header1=True 
 
    if ents['Umlautbereinigung'].get()=='None':
        char_transformation=None
    elif ents['Umlautbereinigung'].get().lower()=='false' or ents['Umlautbereinigung'].get()=='0':
        char_transformation=False 
    else:
        char_transformation=True 
        
    if ents['Codepage'].get()=='None':
        encoding1=None
    else:
        encoding1=ents['Codepage'].get()
    
    if ents['Fliesszahlenformat'].get()=='None':
        float_format1=None
    else:
        float_format1=ents['Fliesszahlenformat'].get()


    if ents['Datumsformat'].get()=='None':
        date_format1=None
    else:
        date_format1=ents['Datumsformat'].get()
    
    if ents['Dezimalzeichen'].get()=='None':
        decimal1=None
    else:
        decimal1=ents['Dezimalzeichen'].get()
    
    quotechar1=ents['Textqualifizierer'].get()
    
    parameters={"Feldtrennzeichen":sep1,"Header_ausgeben":header1,"Codepage":encoding1,"Fliesszahlenformat":float_format1,"Textqualifizierer":quotechar1,"Datumsformat":date_format1,"Dezimalzeichen":decimal1,"Umlautbereinigung":char_transformation}

    param_n=str(param.get()).strip()
    param_n=param_n.lower() 
    if(param_n!=""):
        data[param_n]=[]
        data[param_n].append(parameters)

    json.dump(data, file)
    file.close() 
    
    
def show_param():
    window=Tk()
    window.title("Parameters")
    file = open('Params.txt','r')
    try:  
        data = json.load(file)
        key=list(data.keys())
        window.geometry(str(len(key)+4)+"00x210")
        r=1
        for c in fields:
            label_1=Label(window,text=c,bg = "blue", width=15).grid(row=r,column=0)
            j=1
            label_3=Label(window,text="Param Name",bg = "green", width=15).grid(row=0,column=0)
            k=1
            for i in key:
                label_4=Label(window,text=i,bg = "red", width=50).grid(row=0,column=k)
                k=k+3
                    
            for i in key:
                label_2=Label(window,text=data[i][0][c], width=50).grid(row=r,column=j)
                j=j+3
            r=r+3

    except:
        window.geometry("200x200")
        label_1=Label(window,text="No parameters in file",relief="solid",font=("arial", 12,"bold")).place(x=30,y=70)
    window.mainloop()
    
def exportCSV():
 
    global df
    print("Start Conversion:   " + str(datetime.datetime.now()))
    try:
        filename = filedialog.askopenfilename(initialdir = "/Desktop/",title = "Please Choose files2convert file",filetypes =(("Excel","*.xlsx"),("all files","*.*")))
        print('Selected:   ', filename)
        directory = os.path.split(filename)[0]
    
        df=pd.read_excel(filename)
    except FileNotFoundError:
        print("please choose a the convert2csv file")
        return
    
    outputs_list=list(df['OutputFilename'])
    outputs_set=list(set(outputs_list))
    dupli_bool=False
    
    for y in outputs_set:
        list_dupli=[]
        for i,x in enumerate(outputs_list):
            if y==x:
                list_dupli.append(i)

        if(len(list_dupli)>1):
            print("The Output File Named "+ str(y)+" Exists "+ str(len(list_dupli)) +" Times")
            dupli_bool=True
    if(dupli_bool):
        print("Conversion Aborted")
        return



    file = open('Params.txt','r')
    data = json.load(file)
    params_list=data.keys()
    #df4=pd.read_excel(directory+'/Output/snowflake_SQL.xlsx',sheet_name="Sheet1")
    df4=pd.DataFrame({'Tablename' : [],'SQLstringCREATETABLE' : [],'PUTanweisung' : [],'COPYanweisung' : []})
    
    input_files=os.listdir(directory+'/Input/')
    for i in range(df.shape[0]):
        try:
            
            x=df.iloc[i]
            input_files_pattern=list()
            x_param_n=x['param'].lower()
            if(x_param_n not in params_list):
                print("Parameter "+str(x_param_n) +" not found")
                continue

            for y in input_files:
                if(y.startswith(x['InputFilename'])):
                   input_files_pattern.append(y)
            
            if(len(input_files_pattern)==0):
                print("File Starts With "+x['InputFilename']+ " Not Found")
                continue
            elif len(input_files_pattern)>1:
                print("For Than One File Starts With "+x['InputFilename']+" Was Found")
                continue

            if input_files_pattern[0].endswith('.xlsx'):
                df1=pd.read_excel(directory+'/Input/'+input_files_pattern[0],sheet_name=x['TableSheetName'])
            
            elif input_files_pattern[0].endswith('.parquet'):
                #print("hello")
                df1 = pd.read_parquet(directory+'/Input/'+input_files_pattern[0], engine='pyarrow')   
                        
            elif input_files_pattern[0].endswith('.txt'):
                df1=pd.read_csv(directory+'/Input/'+input_files_pattern[0])
                
            elif y.endswith('.json'):
                df1=pd.read_json(directory+'/Input/'+input_files_pattern[0])

                
            elif input_files_pattern[0].endswith('.accdb'):
                conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+directory+'/Input/'+input_files_pattern[0]+';')
                SQL_Query = pd.read_sql_query('''select * from  '''+x['TableSheetName'], conn)
                df1 = pd.DataFrame(SQL_Query)
            print(directory+'/Input/'+input_files_pattern[0])

            cols=list(df1.columns)
            if(data[x_param_n][0]['Umlautbereinigung']):
                for col in cols:
                    if(type(df1[col].iloc[0])==type("string")):
                        df1[col]=df1[col].apply(lambda x:str(x).replace('ö', 'oe'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('Ö', 'Oe'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('ü', 'ue'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('Ü', 'Ue'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('ä', 'ae'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('Ä', 'Ae'))
                        df1[col]=df1[col].apply(lambda x:str(x).replace('ß', 'ss'))
                                
                            
                cols= list(map(lambda x: str(x).replace('ö', 'oe'), cols))
                cols= list(map(lambda x: str(x).replace('Ö', 'Oe'), cols))
                cols= list(map(lambda x: str(x).replace('ü', 'ue'), cols))
                cols= list(map(lambda x: str(x).replace('Ü', 'Ue'), cols))
                cols= list(map(lambda x: str(x).replace('ä', 'ae'), cols))
                cols= list(map(lambda x: str(x).replace('Ä', 'Ae'), cols))
                cols= list(map(lambda x: str(x).replace('ß', 'ss'), cols))
                cols= list(map(lambda x: str(x).replace(' ', '_'), cols))
                
                df1.columns=cols

            col_var=""
            for col1 in cols:
                col_var=col_var+str(col1)+" VARCHAR(255) ,"
            col_var=col_var[:-2]
                        

            output_n=str(x['OutputFilename']).strip()

            if(output_n=='nan'):
                
                col_var="CREATE OR REPLACE TABLE KMT_STAGE."+str(input_files_pattern[0])+'('+str(col_var)+');'
                if(data[x_param_n][0]['Textqualifizierer']=="None"):
                    df1.to_csv(directory+'/Output/'+str(input_files_pattern[0])+'.csv', sep = data[x_param_n][0]['Feldtrennzeichen'], header=data[x_param_n][0]['Header_ausgeben'],encoding=data[x_param_n][0]['Codepage'],index=False,date_format=data[x_param_n][0]['Datumsformat'],float_format=data[x_param_n][0]['Fliesszahlenformat'],decimal=data[x_param_n][0]['Dezimalzeichen'])
                else:
                    df1.to_csv(directory+'/Output/'+str(input_files_pattern[0])+'.csv', sep = data[x_param_n][0]['Feldtrennzeichen'], header=data[x_param_n][0]['Header_ausgeben'],encoding=data[x_param_n][0]['Codepage'],quotechar=data[x_param_n][0]['Textqualifizierer'],quoting=1,index=False,date_format=data[x_param_n][0]['Datumsformat'],float_format=data[x_param_n][0]['Fliesszahlenformat'],decimal=data[x_param_n][0]['Dezimalzeichen'])

                df4=df4.append({'Tablename' :str(input_files_pattern[0]),'SQLstringCREATETABLE' :col_var,'PUTanweisung':"put file://C:\\"+"Users\\"+"Public\Public_CSV4Snowflake\SnowLoadingZone\\"+str(input_files_pattern[0])+".csv @KMT_PRE_STAGE;",'COPYanweisung':"COPY INTO KMT_STAGE."+str(input_files_pattern[0])+" from @KMT_PRE_STAGE file_format = 'KONTENTOOLCSVFORMAT' pattern = '.*"+str(input_files_pattern[0])+".csv.gz';"} , ignore_index=True)

            else:
                col_var="CREATE OR REPLACE TABLE KMT_STAGE."+x['OutputFilename']+'('+col_var+');'
                if(data[x_param_n][0]['Textqualifizierer']=="None"):
                    df1.to_csv(directory+'/Output/'+str(output_n)+'.csv', sep = data[x_param_n][0]['Feldtrennzeichen'], header=data[x_param_n][0]['Header_ausgeben'],encoding=data[x_param_n][0]['Codepage'],index=False,date_format=data[x_param_n][0]['Datumsformat'],float_format=data[x_param_n][0]['Fliesszahlenformat'],decimal=data[x_param_n][0]['Dezimalzeichen'])
                else:
                    df1.to_csv(directory+'/Output/'+str(output_n)+'.csv', sep = data[x_param_n][0]['Feldtrennzeichen'], header=data[x_param_n][0]['Header_ausgeben'],encoding=data[x_param_n][0]['Codepage'],quotechar=data[x_param_n][0]['Textqualifizierer'],quoting=1,index=False,date_format=data[x_param_n][0]['Datumsformat'],float_format=data[x_param_n][0]['Fliesszahlenformat'],decimal=data[x_param_n][0]['Dezimalzeichen'])

            df4=df4.append({'Tablename' :str(output_n),'SQLstringCREATETABLE' :col_var,'PUTanweisung':"put file://C:\\"+"Users\\"+"Public\Public_CSV4Snowflake\SnowLoadingZone\\"+str(output_n)+".csv @KMT_PRE_STAGE;",'COPYanweisung':"COPY INTO KMT_STAGE."+str(output_n)+" from @KMT_PRE_STAGE file_format = 'KONTENTOOLCSVFORMAT' pattern = '.*"+str(output_n)+".csv.gz';"} , ignore_index=True)

            df4.to_excel(directory+'/Output/snowflake_SQL.xlsx',index=False)
        except Exception as e:
            print(e)
            pass
    print("End Conversion:   " + str(datetime.datetime.now()))

        #df10 = pd.read_parquet(directory+'/Input/holiday.parquet', engine='pyarrow')
       
def makeform(root, fields):
   entries = {}
   for field in fields:
      row = Frame(root)
      lab = Label(row, width=22, text=field+": ", anchor='w')
      ent = Entry(row)
      ent.insert(0,fields[field])
      row.pack(side = TOP, fill = X, padx = 5 , pady = 5)
      lab.pack(side = LEFT)
      ent.pack(side = RIGHT, expand = YES, fill = X)
      entries[field] = ent
   return entries


if __name__ == '__main__':
   root = Tk()
   root.title("Outputfiles Parameters")

   root.resizable(0, 0) # this prevents from resizing the window

   ents = makeform(root, fields)
   
   param = Entry(root)
   param.pack(side = LEFT, padx = 5, pady = 5)
   
   

   b1 = Button(root, text = 'Save Param',command=UploadAction)
   b1.pack(side = LEFT, padx = 5, pady = 5)
  
   
   b3 = Button(root, text = 'show Param',command=show_param)
   b3.pack(side = LEFT, padx = 5, pady = 5)
   
   b2 = Button(root, text='Convert to CSV',command=exportCSV)
   b2.pack(side = LEFT, padx = 5, pady = 5)

  
   root.mainloop()
