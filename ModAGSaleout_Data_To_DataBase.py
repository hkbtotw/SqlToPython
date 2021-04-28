# Code use with Python 3.7.4 32 bit with line-bot-sdk installed
from datetime import datetime
from datetime import date
import pyodbc
import numpy as np
import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

#cwd = os.getcwd()
#print(cwd)

def Write_data_to_database(df_input):
    print('------------- Start WriteDB -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)

    df_write=df_input
    print(' col : ',df_write.columns)
   
	## ODBC Driver 17 for SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                        'Server=SBNDCBIPBST02;'
                        'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')

    #- Delete all records from the table    
    #sql="""SELECT * FROM TSR_ADHOC.dbo.S_FCT_AG_SALEOUT"""
    sql="""delete FROM [TSR_ADHOC].[dbo].[TEST_AG_SALEOUT] where YYYYMM = CONVERT(VARCHAR(6), DATEADD(month, -1, GETDATE()), 112)"""
    
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()


    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[TEST_AG_SALEOUT](	
       [YYYYMM]    
      ,[AGENT_CODE]
      ,[AGENT_NM]
      ,[CUST_TYPE]
      ,[PRD_CATG]
      ,[PRD_BRAND]
      ,[REGION]
      ,[PROVINCE]
      ,[TM_CAL_PERIOD]
      ,[AG_SALE_OUT_OTHER_FLG]
      ,[AG_SALE_OUT_FLG]
      ,[SALE_OUT_AGENT]
      ,[SALE_OUT_AGENT_OTHER]
      ,[BEG_STOCK_AGENT]
      ,[END_STOCK_AGENT]
      ,[BUY_IN]
      ,[collected_at]
	)     
    values(?,?,?,?,
    ?,?,?,?,
    ?,?,?,?,
    ?,?,?,?,?)""", 
    row['YYYYMM']
    ,row['AGENT_CODE']
      ,row['AGENT_NM']
      ,row['CUST_TYPE']
      ,row['PRD_CATG']
      ,row['PRD_BRAND']
      ,row['REGION']
      ,row['PROVINCE']
      ,row['TM_CAL_PERIOD']
      ,row['AG_SALE_OUT_OTHER_FLG']
      ,row['AG_SALE_OUT_FLG']
      ,row['SALE_OUT_AGENT']
      ,row['SALE_OUT_AGENT_OTHER']
      ,row['BEG_STOCK_AGENT']
      ,row['END_STOCK_AGENT']
      ,row['BUY_IN']
      ,row['collected_at']	   
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')

filepath=r'C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\SqlFromDataFrame\\'
filename='test_saleout_202102.xlsx'

# Specify columns to be mapped to String in order to make sure that the ID will be considered as categorical var
cvt={'AGENT_CODE':str}
df_input=pd.read_excel(filepath+filename, converters=cvt) 

# Convert NaN or Null to None before writing to database otherwise code cannot write to database
df_input=df_input.replace({np.nan:None})
#print(' ==> ',df_input)


Write_data_to_database(df_input)