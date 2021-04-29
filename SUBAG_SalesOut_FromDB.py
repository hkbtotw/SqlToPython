import pandas as pd
import pandasql as ps
from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta
import math
import numpy as np
import pyodbc

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

twoMonthBeforeStr=(date.today()-relativedelta(months=2)).strftime('%Y%m')
threeMonthBeforeStr=(date.today()-relativedelta(months=3)).strftime('%Y%m')

print(twoMonthBeforeStr, ' ==two three==  ',threeMonthBeforeStr)

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\SqlFromDataFrame\\'

def ReadDIM_PRODUCT():
    print('------------- Start ReadDB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST01;'
                            'Database=SalesSupport_ETL;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

            SELECT DISTINCT [PRD_PARENT_LABEL] as SKU_LABEL
            ,case  PRD_CATEGORY when 'BEER' then 'Beer'
                when 'NonAlcohol' then 'NAB'
                when 'Spirits' then 'Spirits'
                else PRD_CATEGORY end as PROD_CATG
            ,PRD_GROUP	
            ,PRD_BRAND
            --INTO #DIM_PRODUCT
        FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS]

    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def ReadBUYIN_AGENT():
    print('------------- Start ReadDB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST01;'
                            'Database=SalesSupport_ETL;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
    
    --------------- Step 2: Buy-in Agent / direct sub-agent FILTER ONLY BEER ---------------------------

    SELECT A.[SALE_MONTH]
        ,A.[REGION_SALE_REGION]
        ,A.[REGION_SALE_PRV] 
        ,case  A.PRD_CATEGORY when 'BEER' then 'Beer'
                when 'NonAlcohol' then 'NAB'
                when 'Spirits' then 'Spirits'
                else A.PRD_CATEGORY  end as PROD_CATG
        ,P.[PRD_BRAND]
        ,A.[SOLDTO_LABEL]
        ,CASE WHEN A.[SOLDTO_LABEL]  = 'AGENT' THEN A.[SOLDTO_LABEL] ELSE REPLACE(STR(A.[SOLDTO_LABEL],10), ' ','0') END AS AGENT_CODE
        ,SUM(A.BGT_CASE) AS BG_BEER_CASE
        ,SUM(A.[ACT_CASE]) as ACTL_BUYIN_CASE
        ,SUM(CASE WHEN RIGHT(A.[DATE],2) <= 10 THEN A.[ACT_CASE] ELSE 0 END) AS T1_BUYIN_CASE
        ,SUM(CASE WHEN RIGHT(A.[DATE],2) BETWEEN 11 AND 20 THEN A.[ACT_CASE] ELSE 0 END) AS T2_BUYIN_CASE
        ,SUM(CASE WHEN RIGHT(A.[DATE],2) > 20 THEN A.[ACT_CASE] ELSE 0 END) AS T3_BUYIN_CASE
        ,MAX(A.DATE) AS MAX_DATE
    FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS] A
    LEFT JOIN (SELECT DISTINCT [PRD_PARENT_LABEL] as SKU_LABEL
        ,case  PRD_CATEGORY when 'BEER' then 'Beer'
            when 'NonAlcohol' then 'NAB'
            when 'Spirits' then 'Spirits'
            else PRD_CATEGORY end as PROD_CATG
        ,PRD_GROUP	
        ,PRD_BRAND	
    FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS]) P on A.PRD_PARENT_LABEL = P.SKU_LABEL
    WHERE A.[SOLDTO_CUS_CATEGORY_GRP] = 'Agent / Sub Agent'  -- filter only TT Channel
    --AND P.PROD_CATG in ('Beer','Spirits') --.PRD_CATEGORY = 'BEER' -- filter only Beer
    AND A.SALE_MONTH >= 201809
    --**AND A.SALE_MONTH >= 202102
    -- AND A.[SOLDTO_LABEL] IN ('1002979', '1003175','1007344') TEST FOR หจก.สมพงษ์การสุรา, บจก.นีโอ เอส กรุ๊ป, ร้านโชคดี 4545
    GROUP BY A.[SALE_MONTH]
        ,A.[REGION_SALE_REGION]
        ,A.[REGION_SALE_PRV]
        ,A.[SOLDTO_LABEL]
        ,A.PRD_CATEGORY
        ,P.[PRD_BRAND]
    ORDER BY A.[SOLDTO_LABEL],A.[SALE_MONTH]


    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def ReadStockDetail():
    print('------------- Start ReadDB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST01;'
                            'Database=SalesSupport_ETL;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
        SELECT (S.[Year]*100)+S.[Month] AS YYYYMM
            ,S.CUS_CODE AS CUST_CODE
            ,S.CUS_NM AS CUST_NAME
            ,S.CUS_STS_NM AS CUST_TYPE
            ,P.PROD_CATG
            ,P.PRD_BRAND
            ,SUM(CASE WHEN RIGHT(S.[DATE],2) <= 10 THEN S.[ACT_CASE] ELSE 0 END) AS T1_STOCK_CASE
	        ,SUM(CASE WHEN RIGHT(S.[DATE],2) BETWEEN 11 AND 20 THEN S.[ACT_CASE] ELSE 0 END) AS T2_STOCK_CASE
	        ,SUM(CASE WHEN RIGHT(S.[DATE],2) > 20 THEN S.[ACT_CASE] ELSE 0 END) AS T3_STOCK_CASE
            ,MAX(S.DATE) AS MAX_STOCK_DATE
            --INTO #STOCK_DETAIL
        FROM (SELECT * FROM  [SalesSupport_ETL].[dbo].[Temp_ETL_Check_Stock] 
            WHERE 1=1             
            AND ( --*[Year]*100)+[Month] >= 201809 -1
                   [Year]*100)+[Month] >= 201809 -1  )  S
        LEFT JOIN (   SELECT DISTINCT [PRD_PARENT_LABEL] as SKU_LABEL
                    ,case  PRD_CATEGORY when 'BEER' then 'Beer'
                    when 'NonAlcohol' then 'NAB'
                    when 'Spirits' then 'Spirits'
                    else PRD_CATEGORY end as PROD_CATG
                    ,PRD_GROUP	
                    ,PRD_BRAND                    
                    FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS] ) P ON S.PRD_PARENT_LABEL = P.SKU_LABEL
            WHERE 1=1            
            AND (S.[Year]*100)+S.[Month] >= 201809 -1             
            --**S.[Year]*100)+S.[Month] >= 201809 -1             
        GROUP BY (S.[Year]*100)+S.[Month]
            ,S.CUS_CODE 
            ,S.CUS_NM 
            ,S.CUS_STS_NM
            ,P.PROD_CATG
            ,P.PRD_BRAND
        ORDER BY S.CUS_CODE, YYYYMM
    
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def ReadBuyIn_Sub():
    print('------------- Start ReadDB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST01;'
                            'Database=SalesSupport_ETL;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""
    SELECT format(A.[DATE],'yyyyMM') AS YYYYMM
            ,P.PROD_CATG
            ,P.PRD_BRAND
            ,A.CUS_CODE AS CUST_CODE
            ,A.CUS_NM AS CUST_NAME 
            ,A.CUS_STS_NM AS CUST_TYPE
            ,CASE WHEN A.AGENT_CODE < 10 THEN 'NA'ELSE A.AGENT_CODE END AS AGENT_CODE
            ,CASE WHEN A.AGENT_CODE < 10 THEN 'NA'ELSE A.AGENT_NM END AS AGENT_NAME
            ,SUM(ACT_CASE) AS BUYIN_SUB_CASE
            ,SUM(CASE WHEN RIGHT(A.[DATE],2) <= 10 THEN A.[ACT_CASE] ELSE 0 END) AS T1_BUYIN_SUB_CASE
            ,SUM(CASE WHEN RIGHT(A.[DATE],2) BETWEEN 11 AND 20 THEN A.[ACT_CASE] ELSE 0 END) AS T2_BUYIN_SUB_CASE
            ,SUM(CASE WHEN RIGHT(A.[DATE],2) > 20 THEN A.[ACT_CASE] ELSE 0 END) AS T3_BUYIN_SUB_CASE
            ,MAX(A.DATE) AS MAX_DATE
        FROM [SalesSupport_ETL].[dbo].[Temp_ETL_SubAgentSales] A
        LEFT JOIN (SELECT DISTINCT [PRD_PARENT_LABEL] as SKU_LABEL
            ,case  PRD_CATEGORY when 'BEER' then 'Beer'
                when 'NonAlcohol' then 'NAB'
                when 'Spirits' then 'Spirits'
                else PRD_CATEGORY end as PROD_CATG
            ,PRD_GROUP	
            ,PRD_BRAND	
            FROM [SalesSupport_ETL].[dbo].[FCT_SALE_SIS]) P ON A.PRD_PARENT_LABEL = P.SKU_LABEL
        WHERE 1=1 --(A.SURVEY_TYPE = 2 OR P.PROD_CATG = 'Beer') -- Filter only Beer
            --AND P.PROD_CATG in ('Beer','Spirits')
            --**AND format(A.[DATE],'yyyyMM') >= '202102'
            AND format(A.[DATE],'yyyyMM') >= '201809'
        GROUP BY format(A.[DATE],'yyyyMM')
            ,A.CUS_CODE
            ,A.CUS_NM
            ,A.CUS_STS_NM
            ,P.PROD_CATG
            ,P.PRD_BRAND
            ,CASE WHEN A.AGENT_CODE < 10 THEN 'NA'ELSE A.AGENT_CODE END 
            ,CASE WHEN A.AGENT_CODE < 10 THEN 'NA'ELSE A.AGENT_NM END 
        ORDER BY A.CUS_CODE, YYYYMM
  
    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')


    return dfout

def Write_data_to_database(df_input):
    print('------------- Start WriteDB -------------')
    #df_write=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                        'Server=SBNDCBIPBST02;'
                        'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')
    

    #- View all records from the table
    
    sql="""delete from [TSR_ADHOC].[dbo].[TEST_SUB_SALEOUT] where YYYYMM = CONVERT(VARCHAR(6), DATEADD(month, -1, GETDATE()), 112)"""
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[TEST_SUB_SALEOUT](		
       [YYYYMM]
      ,[PRD_CATG]
      ,[PRD_BRAND]
      ,[CUST_CODE]
      ,[CUST_NAME]
      ,[AGENT_CODE]
      ,[CAL_PERIOD]
      ,[SALE_OUT_FLG]
      ,[BUY_IN]
      ,[BEG_STOCK]
      ,[END_STOCK]
      ,[SALE_OUT]
      ,[collected_at]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?
    )""", 
       row['YYYYMM']
      ,row['PRD_CATG']
      ,row['PRD_BRAND']
      ,row['CUST_CODE']
      ,row['CUST_NAME']
      ,row['AGENT_CODE']
      ,row['CAL_PERIOD']
      ,row['SALE_OUT_FLG']
      ,row['BUY_IN']
      ,row['BEG_STOCK']
      ,row['END_STOCK']
      ,row['SALE_OUT']
      ,row['collected_at']
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')


DIM_PRODUCT=ReadDIM_PRODUCT()
print(DIM_PRODUCT.columns,' ====  ',len(DIM_PRODUCT), ' ----- ',DIM_PRODUCT.tail(10))

STOCK_DETAIL=ReadStockDetail()
print(' ===> ',STOCK_DETAIL)

BUYIN_SUB=ReadBuyIn_Sub()
print(' ===> ',BUYIN_SUB)

BUYIN_AGENT=ReadBUYIN_AGENT()
print( ' BIA ===>', BUYIN_AGENT)


q1 = """

        SELECT A.CUST_CODE, A.CUST_NAME, A.CUST_TYPE, B.AGENT_CODE
        FROM
            (select YYYYMM, CUST_CODE, CUST_NAME
            ,case when CUST_TYPE not in ('Agent','Super Sub-agent') then 'Sub-agent' else CUST_TYPE end as CUST_TYPE
            ,row_number() over (partition by cust_code,cust_type order by yyyymm desc) row_id
            FROM STOCK_DETAIL
            WHERE CUST_TYPE <> 'Inactive'
            ) A
        LEFT JOIN
            (SELECT CUST_CODE, AGENT_CODE, AGENT_NAME
            ,row_number() over (partition by CUST_CODE, AGENT_CODE order by yyyymm desc) row_id
            from BUYIN_SUB 
            where AGENT_CODE <> 'NA' 
            )B ON A.CUST_CODE = B.CUST_CODE AND B.ROW_ID = 1
        WHERE A.row_id = 1  
        
    """

DIM_CUSTOMER=ps.sqldf(q1, locals())
print(DIM_CUSTOMER.columns, ' ----- ',len(DIM_CUSTOMER),' :::  ',DIM_CUSTOMER.tail(10))
# check1=DIM_CUSTOMER.copy()
# check1.to_csv(file_path+'check1.csv')

def STOCK_DETAIL_Temp_1(x):
    extractedMonth=str(x)[4:7]
    if(extractedMonth==12):
        return int(x)+89
    else:
        return int(x)+1

STOCK_DETAIL['REF_MNTH']=STOCK_DETAIL.apply(lambda x: STOCK_DETAIL_Temp_1(x['YYYYMM']),axis=1 )

q1 = """
             SELECT YYYYMM
                     ,CUST_CODE
                     ,REF_MNTH
                     ,T1_STOCK_CASE
                     ,T2_STOCK_CASE
                     ,T3_STOCK_CASE AS BEG_STOCK
                     ,PROD_CATG
					 ,PRD_BRAND
                     FROM STOCK_DETAIL
        """
BEG_STOCK_temp=ps.sqldf(q1, locals())
print(BEG_STOCK_temp.columns, ' --BEG STOCK--- ',len(BEG_STOCK_temp),' :::  ',BEG_STOCK_temp.tail(10))
# check1=BEG_STOCK_temp.copy()
# check1.to_csv(file_path+'check_stock.csv')


def CreateCAL_PERIOD(x):
    if(int(x[8:len(x)])<=10):
        return 'T1'
    elif((int(x[8:len(x)])>=11) and (int(x[8:len(x)])<=20)) :
        return 'T2'
    elif((int(x[8:len(x)])>=21) and (int(x[8:len(x)])<=31)) :
        return 'T3'
    else:
        return 'NA'






q1 = """
        SELECT C.CUST_CODE
        --, concat(S.YYYYMM,'_',C.CUST_CODE)  as REF_Location
        , C.CUST_NAME
        ,C.CUST_TYPE
        , S.YYYYMM
        , S.PROD_CATG
        , S.PRD_BRAND
        --, C.CUST_TYPE AS CUST_TYPE_PROFILE
        --, CASE WHEN I.AGENT_CODE IS NOT NULL AND I.AGENT_CODE <> 'NA' THEN I.AGENT_CODE ELSE C.AGENT_CODE END AS AGENT_CODE
        ,I.AGENT_CODE as AGENT_CODE_I
        ,C.AGENT_CODE as AGENT_CODE_C
        , I.T1_BUYIN_SUB_CASE AS T1_BUYIN
        , I.T2_BUYIN_SUB_CASE AS T2_BUYIN
        , I.T3_BUYIN_SUB_CASE AS T3_BUYIN
        , BEG.BEG_STOCK AS BEG_STOCK
        , S.T1_STOCK_CASE AS T1_STOCK
        , S.T2_STOCK_CASE AS T2_STOCK
        , S.T3_STOCK_CASE AS T3_STOCK
        , S.MAX_STOCK_DATE
        FROM STOCK_DETAIL S 
        LEFT JOIN BUYIN_SUB I ON I.CUST_CODE = S.CUST_CODE AND I.YYYYMM = S.YYYYMM and S.CUST_TYPE <> 'Agent' AND I.PROD_CATG = S.PROD_CATG AND I.PRD_BRAND = S.PRD_BRAND
        LEFT JOIN DIM_CUSTOMER C ON COALESCE(S.CUST_CODE ,I.CUST_CODE) = C.CUST_CODE -- AND COALESCE(S.CUST_TYPE ,I.CUST_TYPE) = C.CUST_TYPE
        LEFT JOIN BEG_STOCK_temp BEG ON BEG.CUST_CODE = C.CUST_CODE AND BEG.REF_MNTH = S.YYYYMM AND BEG.PROD_CATG=S.PROD_CATG AND BEG.PRD_BRAND=S.PRD_BRAND
        WHERE C.CUST_TYPE <> 'Agent'
        AND S.YYYYMM >= 201809
        --**AND S.YYYYMM >= 202102
        --AND C.CUST_CODE IN ('0001008201','0001007344', '0642000124', '0001003175')
        GROUP BY  C.CUST_CODE
        , C.CUST_NAME
        , S.YYYYMM
        , C.CUST_TYPE
        , S.PROD_CATG
		, S.PRD_BRAND
        , I.T1_BUYIN_SUB_CASE 
        , I.T2_BUYIN_SUB_CASE 
        , I.T3_BUYIN_SUB_CASE 
        , BEG.BEG_STOCK 
        , S.T1_STOCK_CASE 
        , S.T2_STOCK_CASE 
        , S.T3_STOCK_CASE 
        --, CASE WHEN I.AGENT_CODE IS NOT NULL AND I.AGENT_CODE <> 'NA' THEN I.AGENT_CODE ELSE C.AGENT_CODE END
        ,I.AGENT_CODE
        ,C.AGENT_CODE
        
        
        
    """

SUB_AGENT_DETL_Temp=ps.sqldf(q1, locals())
print(SUB_AGENT_DETL_Temp.columns, ' ---SUB AG DT Temp-- ',len(SUB_AGENT_DETL_Temp),' :::  ',SUB_AGENT_DETL_Temp.tail(10))
check1=SUB_AGENT_DETL_Temp.copy()
check1.to_csv(file_path+'check.csv')

del DIM_PRODUCT, BUYIN_AGENT, STOCK_DETAIL, BUYIN_SUB, BEG_STOCK_temp,  DIM_CUSTOMER


def GetAgentCode(code1, code2):
    if(code1 is None):
        return code2
    elif(code1==np.nan):
        return code2
    elif(code1=='NA'):
        return code2
    else:
        return code1

SUB_AGENT_DETL_Temp['CAL_PERIOD']=SUB_AGENT_DETL_Temp.apply(lambda x: CreateCAL_PERIOD(x['MAX_STOCK_DATE']),axis=1 )
SUB_AGENT_DETL_Temp['REF_Location']=SUB_AGENT_DETL_Temp['YYYYMM'].astype(str)+'_'+SUB_AGENT_DETL_Temp['CUST_CODE'].astype(str)
SUB_AGENT_DETL_Temp['AGENT_CODE']=SUB_AGENT_DETL_Temp.apply(lambda x: GetAgentCode(x['AGENT_CODE_I'], x['AGENT_CODE_C']),axis=1)

q1 = """
        SELECT CUST_CODE
        ,REF_Location
        ,CUST_NAME
        ,YYYYMM
        ,CAL_PERIOD
        ,PROD_CATG
        ,PRD_BRAND
        ,AGENT_CODE
        ,T1_BUYIN
        ,T2_BUYIN
        ,T3_BUYIN
        ,BEG_STOCK
        ,T1_STOCK
        ,T2_STOCK
        ,T3_STOCK
        ,MAX(MAX_STOCK_DATE) AS MAX_STOCK			  
        FROM SUB_AGENT_DETL_Temp        
        GROUP BY  CUST_CODE
        ,CUST_NAME
        ,YYYYMM
        ,CUST_TYPE
        ,PROD_CATG
		,PRD_BRAND
        ,T1_BUYIN
        ,T2_BUYIN
        ,T3_BUYIN
        ,BEG_STOCK 
        ,T1_STOCK
        ,T2_STOCK
        ,T3_STOCK
        ,AGENT_CODE
        ,CAL_PERIOD
        
        
    """

SUB_AGENT_DETL_Temp_1=ps.sqldf(q1, locals())
print(SUB_AGENT_DETL_Temp_1.columns, ' ---SUB AG DT Temp 1-- ',len(SUB_AGENT_DETL_Temp_1),' :::  ',SUB_AGENT_DETL_Temp_1.tail(10))

check1=SUB_AGENT_DETL_Temp.copy()
check1.to_csv(file_path+'check1.csv')

del SUB_AGENT_DETL_Temp

def checknull(tin):
    if(tin is None):
        return 0
    else:
        return tin
def CreateAG_BUY_IN(x,t1,t2,t3):
    #print(x,' ::  ',type(x))
    if(x=='T3'):
        return checknull(t1)+checknull(t2)+checknull(t3)
    elif(x=='T2'):
        return checknull(t1)+checknull(t2)
    elif(x=='T1'):
        return checknull(t1)
    else:
        return -99
def CreateAG_SALE_OUT(x,stock,t1,t2,t3,s1,s2,s3):
    def checknull(tin):
        if(tin is None):
            return 0
        elif(tin==np.nan):
            return 0
        else:
            return tin

    if(x=='T3'):
        return checknull(stock)+checknull(t1)+checknull(t2)+checknull(t3)-checknull(s3)
    elif(x=='T2'):
        return checknull(stock)+checknull(t1)+checknull(t2)-checknull(s2)
    elif(x=='T1'):
        return checknull(stock)+checknull(t1)-checknull(s1)
    else:
        return -99
def CreateAG_SALE_OUT_FLG(x,stock,t1,t2,t3,s1,s2,s3):
    def checknull(tin):
        if(tin is None):
            return 0
        else:
            return tin
    if(stock is None or stock==np.nan):
        return 'ERR'
    elif(x=='T3' and stock+checknull(t1)+checknull(t2)+checknull(t3)-s3<0):
        return 'ERR'
    elif(x=='T3' and math.isnan(stock+checknull(t1)+checknull(t2)+checknull(t3)-s3)==True):
        return 'ERR'
    elif(x=='T2' and stock+checknull(t1)+checknull(t2)-s2<0):
        return 'ERR'
    elif(x=='T2' and math.isnan(stock+checknull(t1)+checknull(t2)-s2)==True):
        return 'ERR'
    elif(x=='T1' and stock+checknull(t1)-s1<0):
        return 'ERR'
    elif(x=='T1' and math.isnan(stock+checknull(t1)-s1)==True):
        return 'ERR'
    else:
        return '-'
def CreateAG_END_STOCK(x,s1,s2,s3):
    if(x=='T3'):
        return s3
    elif(x=='T2'):
        return s2
    elif(x=='T1'):
        return s1
    else:
        return -99

SUB_AGENT_DETL_Temp_1['BUY_IN']=SUB_AGENT_DETL_Temp_1.apply(lambda x: CreateAG_BUY_IN(x['CAL_PERIOD'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN']),axis=1 )
SUB_AGENT_DETL_Temp_1['SALE_OUT']=SUB_AGENT_DETL_Temp_1.apply(lambda x: CreateAG_SALE_OUT(x['CAL_PERIOD'],x['BEG_STOCK'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )
SUB_AGENT_DETL_Temp_1['SALE_OUT_FLG']=SUB_AGENT_DETL_Temp_1.apply(lambda x: CreateAG_SALE_OUT_FLG(x['CAL_PERIOD'],x['BEG_STOCK'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )
SUB_AGENT_DETL_Temp_1['END_STOCK']=SUB_AGENT_DETL_Temp_1.apply(lambda x: CreateAG_END_STOCK(x['CAL_PERIOD'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )

q1 = """
    SELECT CUST_CODE
        ,REF_Location
        ,CUST_NAME
        ,YYYYMM
        ,CAL_PERIOD
        ,PROD_CATG
        ,PRD_BRAND
        ,AGENT_CODE
        ,T1_BUYIN
        ,T2_BUYIN
        ,T3_BUYIN
        ,BEG_STOCK AS BEG_STOCK
        ,T1_STOCK
        ,T2_STOCK
        ,T3_STOCK
        ,BUY_IN
        ,SALE_OUT
        ,SALE_OUT_FLG
        ,END_STOCK
    
    FROM SUB_AGENT_DETL_Temp_1
        
    """

SUB_AGENT_DETL=ps.sqldf(q1, locals())
print(SUB_AGENT_DETL.columns, ' ---AG DT-- ',len(SUB_AGENT_DETL),' :::  ',SUB_AGENT_DETL.tail(10))

del SUB_AGENT_DETL_Temp_1 

# AGENT_DETL.reset_index().T.drop_duplicates().T
# print(AGENT_DETL.columns, ' ---AG DT - 2 -- ',len(AGENT_DETL),' :::  ',AGENT_DETL.tail(10))
# check1=AGENT_DETL.copy()
# check1.to_csv(file_path+'check_AGDETL.csv')


#------------------------- Final Select Output ------------------------



SUB_AGENT_DETL['SALEOUT_by']='SUBAGENT'
SUB_AGENT_DETL['collected_at']=nowStr

monthStr=(date.today()-relativedelta(months=1)).strftime('%Y%m')
datenow=monthStr
print(' ==> ',datenow)

q1 = """select   S.SALEOUT_by
   ,S.YYYYMM
   ,S.PROD_CATG as PRD_CATG
   ,S.PRD_BRAND
   ,S.CUST_CODE
   ,S.CUST_NAME
   ,S.AGENT_CODE
   ,S.CAL_PERIOD
   ,S.SALE_OUT_FLG
   ,sum(S.BUY_IN) as BUY_IN
   ,sum(S.BEG_STOCK) as BEG_STOCK
   ,sum(S.END_STOCK) as END_STOCK
   ,sum(S.SALE_OUT) as SALE_OUT
   --,sum(D.HAS_QTY_BEER) as TARGET_BEER
   --,sum(D.HAS_QTY_SPIRITS) as TARGET_SPIRITS
   ,S.collected_at
FROM SUB_AGENT_DETL S
where S.YYYYMM='"""+str(datenow)+"""'    -->=201809 --and S.CAL_PERIOD ='T3'
GROUP BY  S.YYYYMM
   ,S.SALE_OUT_FLG
   ,S.AGENT_CODE
   ,S.PROD_CATG
   ,S.PRD_BRAND
   ,S.CUST_CODE
   ,S.CUST_NAME
   ,S.CAL_PERIOD  
        
   """

dfResult=ps.sqldf(q1, locals())
print(dfResult.columns, ' ---Result-- ',len(dfResult),' :::  ',dfResult.tail(10))

del SUB_AGENT_DETL

check1=dfResult.copy()
check1.to_csv(file_path+'Sub_check.csv')

Write_data_to_database(dfResult)

del  dfResult

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

##-----------------------------------------------------------------------
## Write log file
activityLog=' SUBSG Saleout to DB Successful at '+nowStr+ ' ::  Time_use : '+str(round(DIFFTIMEMIN,2))+ ' Seconds ******** \n'

log_file="SUBAG_SaleoutToDB_"+todayStr
f = open(file_path+'\\log\\'+log_file, "a")
f.write(activityLog)
f.close()
