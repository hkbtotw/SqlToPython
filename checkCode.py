import pandas as pd
import pandasql as ps
from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta
import math
import numpy as np
import pyodbc

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\SqlFromDataFrame\\'
file_name='testcode.xlsx'

cvt={'AGENT_CODE':str}
df_original=pd.read_excel(file_path+file_name, sheet_name='original', converters=cvt)

cvt={'AGENT_CODE':str}
df_code=pd.read_excel(file_path+file_name, sheet_name='code', converters=cvt)

originalList=list(df_original['AGENT_CODE'].unique())
codeList=list(df_code['AGENT_CODE'].unique())

print(len(originalList), ' --- len ----- ',len(codeList))

def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

# Find common ID between wfhList and  TB_EMP list
resultList=intersection(originalList,codeList)
print(' == result == ',len(resultList),' ----  ',len(originalList))

# Find ID from wfhList not in common List
notExistIDList = [x for x in originalList  if x not in resultList]

dfList=[]
for nid in notExistIDList:
    dfDummy=df_original[df_original['AGENT_CODE']==nid].copy()
    dfList.append(dfDummy)
    

mainDf=pd.DataFrame(columns=['YYYYMM','AGENT_CODE','AGENT_NM','CUST_TYPE','PRD_CATG','PRD_BRAND','REGION','PROVINCE','TM_CAL_PERIOD','AG_SALE_OUT_OTHER_FLG','AG_SALE_OUT_FLG','SALE_OUT_AGENT','SALE_OUT_AGENT_OTHER','BEG_STOCK_AGENT','END_STOCK_AGENT','BUY_IN','collected_at'])
for nd in dfList:
    mainDf=mainDf.append(nd)

mainDf=mainDf.reset_index(drop=True)

print(mainDf)

mainDf.to_csv(file_path+'checkMissingData.csv')