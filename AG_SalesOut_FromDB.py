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
    -- AND A.[SOLDTO_LABEL] IN ('1002979', '1003175','1007344') TEST FOR ?????????.???????????????????????????????????????, ?????????.???????????? ????????? ???????????????, ??????????????????????????? 4545
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
            AND ([Year]*100)+[Month] >= 201809 -1  )  S
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
    
    sql="""delete from [TSR_ADHOC].[dbo].[TEST_AG_SALEOUT] where YYYYMM = CONVERT(VARCHAR(6), DATEADD(month, -1, GETDATE()), 112)"""
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
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,?,
    ?,?
    )""", 
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
        SELECT CUST_CODE
            ,PRD_BRAND
            --,YYYYMM
            ,REF_MNTH
            ,T3_STOCK_CASE AS BEG_STOCK
            FROM STOCK_DETAIL
        """
BEG_STOCK_temp=ps.sqldf(q1, locals())
print(BEG_STOCK_temp.columns, ' --BEG STOCK--- ',len(BEG_STOCK_temp),' :::  ',BEG_STOCK_temp.tail(10))
# check1=BEG_STOCK_temp.copy()
# check1.to_csv(file_path+'check_stock.csv')

q1 = """
        select YYYYMM, AGENT_CODE, PROD_CATG, PRD_BRAND, sum(BUYIN_SUB_CASE) SUB_BUY_IN
 				from BUYIN_SUB where AGENT_CODE is not null
 				group by YYYYMM, AGENT_CODE, PROD_CATG, PRD_BRAND
        """
BUYIN_SUB_Temp=ps.sqldf(q1, locals())
print(BUYIN_SUB_Temp.columns, ' --BI SUB Temp--- ',len(BUYIN_SUB_Temp),' :::  ',BUYIN_SUB_Temp.tail(10))

def CreateCAL_PERIOD(x):
    if(int(x[8:len(x)])<=10):
        return 'T1'
    elif((int(x[8:len(x)])>=11) and (int(x[8:len(x)])<=20)) :
        return 'T2'
    elif((int(x[8:len(x)])>=21) and (int(x[8:len(x)])<=31)) :
        return 'T3'
    else:
        return 'NA'

STOCK_DETAIL['CAL_PERIOD']=STOCK_DETAIL.apply(lambda x: CreateCAL_PERIOD(x['MAX_STOCK_DATE']),axis=1 )

q1 = """

        SELECT I.AGENT_CODE
		, C.CUST_NAME
		--, C.CUST_TYPE AS CUST_TYPE_PROFILE
		, COALESCE(I.SALE_MONTH,S.YYYYMM) AS YYYYMM
		, I.PRD_BRAND
		, I.PROD_CATG
		, I.REGION_SALE_REGION
		, I.REGION_SALE_PRV
		, S.CAL_PERIOD
		, SUM(I.T1_BUYIN_CASE) AS T1_BUYIN
		, SUM(I.T2_BUYIN_CASE) AS T2_BUYIN
		, SUM(I.T3_BUYIN_CASE) AS T3_BUYIN
		, SUM(BEG.BEG_STOCK) AS BEG_STOCK
		, SUM(S.T1_STOCK_CASE) AS T1_STOCK
		, SUM(S.T2_STOCK_CASE) AS T2_STOCK
		, SUM(S.T3_STOCK_CASE) AS T3_STOCK
		, SUM(INSUB.SUB_BUY_IN) AS SUB_BUY_IN
		, MAX(S.MAX_STOCK_DATE) AS MAX_STOCK_DATE
		
		FROM BUYIN_AGENT I
		-- select CUST_CODE, count(CUST_TYPE) from (select distinct CUST_CODE, CUST_NAME ,CUST_TYPE from DIM_CUSTOMER) A group by CUST_CODE
		LEFT JOIN STOCK_DETAIL S ON I.AGENT_CODE = S.CUST_CODE AND I.SALE_MONTH = S.YYYYMM AND I.PRD_BRAND = S.PRD_BRAND
		LEFT JOIN (select distinct CUST_CODE, CUST_NAME, CUST_TYPE from  DIM_CUSTOMER) C ON COALESCE(I.AGENT_CODE, S.CUST_CODE) = C.CUST_CODE
		LEFT JOIN BEG_STOCK_temp BEG ON BEG.CUST_CODE = S.CUST_CODE AND BEG.REF_MNTH = S.YYYYMM AND BEG.PRD_BRAND = S.PRD_BRAND
		LEFT JOIN BUYIN_SUB_Temp INSUB ON I.AGENT_CODE = INSUB.AGENT_CODE AND I.SALE_MONTH = INSUB.YYYYMM AND I.PRD_BRAND = INSUB.PRD_BRAND 
		
		WHERE 1=1  --C.CUST_TYPE in ('Agent','Super Sub-agent')
				--AND I.SALE_MONTH >=  """+str(twoMonthBeforeStr)+"""  -- 201809
                AND I.SALE_MONTH >=  201809
				AND I.ACTL_BUYIN_CASE > 0
				AND I.AGENT_CODE is not null
		GROUP BY  I.AGENT_CODE
				, C.CUST_NAME
				, COALESCE(I.SALE_MONTH,S.YYYYMM)
				, I.PRD_BRAND
				, I.PROD_CATG
				, I.REGION_SALE_REGION
				, I.REGION_SALE_PRV
				--, C.CUST_TYPE 
				, S.CAL_PERIOD  
        
    """

AGENT_DETL_Temp=ps.sqldf(q1, locals())
print(AGENT_DETL_Temp.columns, ' ---AG DT Temp-- ',len(AGENT_DETL_Temp),' :::  ',AGENT_DETL_Temp.tail(10))
# check1=AGENT_DETL_Temp.copy()
# check1.to_csv(file_path+'check.csv')

del DIM_PRODUCT, BUYIN_AGENT, STOCK_DETAIL, BUYIN_SUB, BEG_STOCK_temp, BUYIN_SUB_Temp

def checknull(tin):
    if(tin is None):
        return 0
    else:
        return tin
def CreateAG_BUY_IN(x,t1,t2,t3):
    

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

AGENT_DETL_Temp['AG_BUY_IN']=AGENT_DETL_Temp.apply(lambda x: CreateAG_BUY_IN(x['CAL_PERIOD'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN']),axis=1 )
AGENT_DETL_Temp['AG_SALE_OUT']=AGENT_DETL_Temp.apply(lambda x: CreateAG_SALE_OUT(x['CAL_PERIOD'],x['BEG_STOCK'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )
AGENT_DETL_Temp['AG_SALE_OUT_FLG']=AGENT_DETL_Temp.apply(lambda x: CreateAG_SALE_OUT_FLG(x['CAL_PERIOD'],x['BEG_STOCK'],x['T1_BUYIN'],x['T2_BUYIN'],x['T3_BUYIN'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )
AGENT_DETL_Temp['AG_END_STOCK']=AGENT_DETL_Temp.apply(lambda x: CreateAG_END_STOCK(x['CAL_PERIOD'],x['T1_STOCK'],x['T2_STOCK'],x['T3_STOCK']),axis=1 )

q1 = """
    SELECT AG.*
    ,AG.BEG_STOCK AS AG_BEGIN_STOCK
    --, AG.AG_BUY_IN
    --, AG.AG_SALE_OUT
    --, AG.AG_SALE_OUT_FLG
    --, AG.AG_END_STOCK
    -- select * from #AGENT_DETL where AGENT_CODE = '0001007240' and PRD_BRAND = 'Chang25th ColdBrew' order by YYYYMM
    --INTO #AGENT_DETL
    FROM AGENT_DETL_Temp AG
    ORDER BY AG.AGENT_CODE, AG.YYYYMM      
        
    """

AGENT_DETL=ps.sqldf(q1, locals())
print(AGENT_DETL.columns, ' ---AG DT-- ',len(AGENT_DETL),' :::  ',AGENT_DETL.tail(10))

del AGENT_DETL_Temp

# AGENT_DETL.reset_index().T.drop_duplicates().T
# print(AGENT_DETL.columns, ' ---AG DT - 2 -- ',len(AGENT_DETL),' :::  ',AGENT_DETL.tail(10))
# check1=AGENT_DETL.copy()
# check1.to_csv(file_path+'check_AGDETL.csv')


#------------------------- Final Select Output ------------------------
def ParseProvinceName(x):
    if(x=='Phrachuap Khiri Khan'):
        return 'Prachuap Khiri Khan'
    else:
        return x
def CreateSale_Out_Agent_Other(x, y):    
    if( x-y < 0):
        return 0
    else:
        return x-y
def CreateSale_Out_Agent_Other_Flg(x, y):
    #print(' check x-y', x, ' : ',y ,' ==> ',x-y)
    if( ((x-y) is None) or (x-y)==np.nan ):
        return 'ERR'
    elif(math.isnan(x-y)==True):
        return 'ERR'
    elif (x-y < 0):
        return 'ERR'        
    else:
        return '-'

AGENT_DETL['PROVINCE']=AGENT_DETL.apply(lambda x: ParseProvinceName(x['REGION_SALE_PRV']),axis=1 )
AGENT_DETL['SALE_OUT_AGENT_OTHER']=AGENT_DETL.apply(lambda x: CreateSale_Out_Agent_Other(x['AG_SALE_OUT'],x['SUB_BUY_IN']),axis=1 )
AGENT_DETL['AG_SALE_OUT_OTHER_FLG']=AGENT_DETL.apply(lambda x: CreateSale_Out_Agent_Other_Flg(x['AG_SALE_OUT'],x['SUB_BUY_IN']),axis=1 )



q1 = """
    SELECT A.YYYYMM AS YEAR_MONTH
        ,A.PROD_CATG
        ,A.PRD_BRAND
        ,A.AGENT_CODE
        ,A.CUST_NAME AS AGENT_NM
        ,A.REGION_SALE_REGION AS REGION
        ,A.PROVINCE
        --,A.CUST_TYPE_PROFILE AS AG_TYPE
        ,A.CAL_PERIOD		AS TM_CAL_PERIOD
        ,A.AG_BEGIN_STOCK	AS [BEG_STOCK_AGENT]
        ,A.AG_END_STOCK		AS [END_STOCK_AGENT]
        ,A.AG_BUY_IN		AS [BUY_IN]
        ,A.AG_SALE_OUT		AS [SALE_OUT_AGENT]
        ,A.AG_SALE_OUT_FLG  -- LIST ERROR TO P'TANOM OR IT FOR FIXING ???
        ,A.[SALE_OUT_AGENT_OTHER]
        ,A.AG_SALE_OUT_OTHER_FLG
        --,A.AG_BUY_IN as [BUY_IN_AGENT]
    --into #Output
    FROM AGENT_DETL A
    --WHERE A.CUST_TYPE_PROFILE IN ('Agent','Super Sub-agent')
    ORDER BY A.AGENT_CODE, A.YYYYMM    
        
    """

Output=ps.sqldf(q1, locals())
print(Output.columns, ' ---Output-- ',len(Output),' :::  ',Output.tail(10))

Output['collected_at']=nowStr

# check1=Output.copy()
# check1.to_csv(file_path+'check1.csv')
monthStr=(date.today()-relativedelta(months=1)).strftime('%Y%m')
datenow=monthStr
print(' ==> ',datenow)
#datenow='201809'

q1 = """     

     SELECT 
        O.YEAR_MONTH as [YYYYMM]
        ,O.AGENT_CODE ,O.AGENT_NM ,MTAG.CUST_TYPE
        ,O.PROD_CATG as PRD_CATG
        ,O.PRD_BRAND 
        ,O.REGION ,O.PROVINCE
        ,O.TM_CAL_PERIOD
        ,O.AG_SALE_OUT_OTHER_FLG ,O.AG_SALE_OUT_FLG 
        ,O.SALE_OUT_AGENT ,O.SALE_OUT_AGENT_OTHER
        ,O.BEG_STOCK_AGENT  
        ,O.END_STOCK_AGENT  
        ,O.BUY_IN
        ,O.collected_at
        --,'AGENT' as SALEOUT_by
    FROM Output O 
    LEFT JOIN (select distinct CUST_CODE, CUST_NAME ,CUST_TYPE from DIM_CUSTOMER WHERE CUST_TYPE in  ('Agent','Super Sub-agent')) MTAG on MTAG.CUST_CODE = O.AGENT_CODE
    WHERE MTAG.CUST_CODE is not null
    --and O.YEAR_MONTH=CONVERT(VARCHAR(6), DATEADD(month, -1, GETDATE()), 112)
    and O.YEAR_MONTH='"""+str(datenow)+"""'
    order by O.YEAR_MONTH desc ,O.AGENT_CODE desc
        
    """

dfResult=ps.sqldf(q1, locals())
print(dfResult.columns, ' ---Result-- ',len(dfResult),' :::  ',dfResult.tail(10))

check1=dfResult.copy()
check1.to_csv(file_path+'check1.csv')

Write_data_to_database(dfResult)

del AGENT_DETL, Output, dfResult, DIM_CUSTOMER

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

##-----------------------------------------------------------------------
## Write log file
activityLog=' Saleout to DB Successful at '+nowStr+ ' ::  Time_use : '+str(round(DIFFTIMEMIN,2))+ ' Seconds ******** \n'

log_file="SaleoutToDB_"+todayStr
f = open(file_path+'\\log\\'+log_file, "a")
f.write(activityLog)
f.close()
