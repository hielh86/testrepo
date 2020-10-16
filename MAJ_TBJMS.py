# -*- coding: utf-8 -*-
"""
Created on Wed May  6 15:18:29 2020

@author: HIELH9
"""



from arcgis.features import SpatialDataFrame
import os, tempfile, re, arcpy, arcgis, datetime, logging, fnmatch, subprocess, ssl, numpy,xlrd, openpyxl
from arcgis.gis import GIS

import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, datetime
#from datetime import datetime
from pandas import DataFrame
import time
import numpy

Workspace=arcpy.GetParameterAsText (0)
Datas=arcpy.GetParameterAsText (1)

os.getcwd()
os.chdir(Workspace)
Datas = Datas.replace('\\','/')
wb=xlrd.open_workbook(Datas.rsplit('/', 1)[0])
feuille_4 = wb.sheet_by_name(Datas.rsplit('/', 1)[1].replace('$',''))
#feuille_4 = wb.sheet_by_index(5)
#one_day=datetime.timedelta(days=1)
rows=feuille_4.nrows-3
#cols=feuille_4.ncols-3
lstoflst = []
lst2=[]

for r in range(8, rows):
    lstoflst=[]
    for c in range(0,19) :
        textType = feuille_4.cell(rowx=r, colx=c).ctype #Get the type of the cell
        if textType == 5 or textType==0:
           lst = 0
        else:
          lst = feuille_4.cell_value(rowx=r, colx=c)
        lstoflst.append(lst)
    lst2.append(lstoflst)
    # print(lst2)
df0=pd.DataFrame(lst2, columns= ['CCS','NomRA','CodeRA','AppTot','AppCOV','AffecTot','AffecCOV','TransCOV','TransReg','TransTot','TauxCOV','Trans19','InterReg','InterCOV','InterTot','Trans19_inter','VolTrans','TotTran19','ContTrans'])

df0['ContTrans']=df0['ContTrans']*100
df0['TauxCOV']=df0['TauxCOV']*100

print(df0)

lstoflst = []
lst2=[]
for r in range(8, rows):
    lstoflst=[]
    for c in range(19,27) :
        textType = feuille_4.cell(rowx=r, colx=c).ctype #Get the type of the cell
        if textType == 5 or textType==0:
           lst = 0
        else:
          lst = feuille_4.cell_value(rowx=r, colx=c)
        lstoflst.append(lst)
    lst2.append(lstoflst)
    #print(lst2)
df1=pd.DataFrame(lst2, columns= ['DelaiH3','DelaiH5','DelaiP0','DelaiP1','','','','DurCHCOV']) 
#df1=pd.DataFrame(lstoflst_time, columns= ['DelaiH3','DelaiH5','DelaiP0','DelaiP1','','','','DurCHCOV'])            
df1=df1.drop(['','',''], axis=1)
df1['DelaiH3'] = pd.to_numeric(df1['DelaiH3'], errors='coerce')
df1['DelaiH3']=round((df1['DelaiH3']*86400/60),2)
df1['DelaiH5'] = pd.to_numeric(df1['DelaiH5'], errors='coerce')
df1['DelaiH5']=round((df1['DelaiH5']*86400/60),2)
df1['DelaiP0'] = pd.to_numeric(df1['DelaiP0'], errors='coerce')
df1['DelaiP0']=round((df1['DelaiP0']*86400/60),2)
df1['DelaiP1'] = pd.to_numeric(df1['DelaiP1'], errors='coerce')
df1['DelaiP1']=round((df1['DelaiP1']*86400/60),2)
df1['DurCHCOV'] = pd.to_numeric(df1['DurCHCOV'], errors='coerce')
df1['DurCHCOV']=round((df1['DurCHCOV']*86400/60),2)
print(df1)
def convert_excel_time(t):
        # Time = t*86400
        return time.strftime('%H:%M:%S', time.gmtime(round(t*86400)))

lst_time_val=[]
for r in range(8,rows):
    
    time_value = feuille_4.cell_value(rowx=r, colx=27)
    lst_time_val.append(time_value)

df2=pd.DataFrame(lst_time_val, columns= ['Heure'])            

print(df2.dtypes)
for i in range(0,16):
    
    try :  
        df2.loc[i,'Heure']=convert_excel_time(df2.loc[i,'Heure'])

        res = True
    except : 
        print("Not a float")
        df2.loc[i,'Heure']='NULL'
        res = False


Source=[]
for r in range(8,rows):
    source=feuille_4.cell_value(rowx=r, colx=28)
    Source.append(source)
    
df3=pd.DataFrame(Source, columns= ['Source']) 
print(df3)


date_feuille = xlrd.xldate_as_tuple(feuille_4.cell_value(rowx=3, colx=6), wb.datemode)
Date_= date(date_feuille[0], date_feuille[1], date_feuille[2])
Date_ = datetime.combine(Date_, datetime.min.time())
print(Date_)
df5=pd.concat([df0, df1, df2, df3], axis = 1)
print(df5)
Jointure=[1,9,12,2,3,11,4,5,6,7,8,10,13,14,15,16]
df5['Id_join']= Jointure

df5['TauxCOV'] = pd.to_numeric(df5['TauxCOV'], errors='coerce')
df5['ContTrans'] = pd.to_numeric(df5['ContTrans'], errors='coerce')
df5['TauxCOV']=round(df5['TauxCOV'],2)
df5['ContTrans']=round(df5['ContTrans'],2)

def recode_empty_cells(dataframe, list_of_columns):

    for column in list_of_columns:
        
        dataframe[column]=dataframe[column].astype('int64')
        

    return dataframe

recode_empty_cells(df5, ['AppTot','AppCOV','AffecTot','AffecCOV','TransCOV','TransReg','TransTot','Trans19','InterReg','InterCOV','InterTot','Trans19_inter','VolTrans','TotTran19'])

if arcpy.Exists(r".\Mytable_SPU"):
   arcpy.management.Delete(r".\Mytable_SPU")

x_SPU = np.array(np.rec.fromrecords(df5.values))
names_SPU = df5.dtypes.index.tolist()
x_SPU.dtype.names = tuple(names_SPU)
arcpy.da.NumPyArrayToTable(x_SPU, r".\Mytable_SPU")

arcpy.management.AddField(".\Mytable_SPU", "Date", "DATE", None, None, None, '', "NULLABLE", "NON_REQUIRED", '')
with arcpy.da.UpdateCursor(r".\Mytable_SPU", ['Date']) as rows:
    for row in rows:
        rows.updateRow([Date_])

SPU_joined_Mytable = arcpy.AddJoin_management(r".\SPU", 'Id_join', r".\Mytable_SPU", 'Id_join')

# Copy the layer to a new permanent feature class
arcpy.CopyFeatures_management(SPU_joined_Mytable, r".\MyTable_SPU_Geom")
shp_SPU=r".\Dashboard_SPU"
field_names = [f.name for f in arcpy.ListFields(shp_SPU)]
with arcpy.da.UpdateCursor(shp_SPU, ["Date"]) as cursor:
    for row in cursor:
        if row[0] == Date_:
            cursor.deleteRow()
with arcpy.da.InsertCursor(shp_SPU, ["SHAPE@",'Nom','Id_join','CCS','NomRA','CodeRA','Date','AppTot','AppCOV','AffecTot','AffecCOV','TransCOV','TransReg','TransTot','TauxCOV','Trans19','InterReg','InterCOV','InterTot','Trans19_inter','VolTrans','TotTran19','ContTrans', 'DelaiH3','DelaiH5','DelaiP0','DelaiP1','DurCHCOV','Heure','Source']) as iCursor:
    with arcpy.da.SearchCursor(r".\MyTable_SPU_Geom", ["SHAPE@",'SPU_Nom','SPU_Id_join','Mytable_SPU_CCS','Mytable_SPU_NomRA','Mytable_SPU_CodeRA','Mytable_SPU_Date','Mytable_SPU_AppTot','Mytable_SPU_AppCOV','Mytable_SPU_AffecTot','Mytable_SPU_AffecCOV','Mytable_SPU_TransCOV','Mytable_SPU_TransReg','Mytable_SPU_TransTot','Mytable_SPU_TauxCOV','Mytable_SPU_Trans19','Mytable_SPU_InterReg','Mytable_SPU_InterCOV','Mytable_SPU_InterTot','Mytable_SPU_Trans19_inter','Mytable_SPU_VolTrans','Mytable_SPU_TotTran19','Mytable_SPU_ContTrans', 'Mytable_SPU_DelaiH3','Mytable_SPU_DelaiH5','Mytable_SPU_DelaiP0','Mytable_SPU_DelaiP1','Mytable_SPU_DurCHCOV','Mytable_SPU_Heure','Mytable_SPU_Source' ]) as sCursor:
        for row in sCursor:  
            iCursor.insertRow(row)
