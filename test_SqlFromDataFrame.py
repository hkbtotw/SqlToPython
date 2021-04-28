import pandas as pd
import pandasql as ps
import pyodbc

def ReadSV():
    print('------------- Start ReadDB -------------')
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST02;'
                            'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

    declare @LY_F nvarchar(6) = format(EOMONTH(getdate(),0),'yyyyMM');
    

    ---------------- SV ล่าสุดของแต่ละร้านค้า ----------------------------

    select * 

    from ( SELECT *
        ,DENSE_RANK() over(partition by CUST_CODE, [YearMonth] order by YearMonth desc) as last_row_no
        FROM [TSR_ADHOC].[dbo].S_CVM_SV_DIST_BEER_DATE
        where [YearMonth] >=  @LY_F
        ) SV where last_row_no = 1

    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    #dfout.columns=['EmployeeId','UserLat','UserLong','DateTimeStamp']
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

df=ReadSV()

print(df.columns,' ====  ',len(df), ' ----- ',df.tail(10))


def ConvertToyyyyMM(x):
    stringMM=x[5:7]
    stringYY=x[0:4]    
    return stringYY+stringMM


df['REC_DATE_2']=df.apply(lambda x: ConvertToyyyyMM(x['REC_DATE']),axis=1)


q1 = """
select YearMonth ,p_name_t ,a_name_t ,t_name_t
    ,sum( case when [BRAND] = 'เบียร์ช้าง25ปี' and [BRAND_GRP] = 'Chang' then QTY_BT else 0 end) as QTY_BT_CHANG_25TH
	  ,sum( case when [BRAND] != 'เบียร์ช้าง25ปี' and [BRAND_GRP] = 'Chang' then QTY_BT else 0 end) as QTY_BT_CHANG_CC
	  ,sum( case when [BRAND_GRP] = 'Leo' then QTY_BT else 0 end) as QTY_BT_LEO
	  ,sum( case when [BRAND_GRP] = 'Signha' then QTY_BT else 0 end) as QTY_BT_Signha
	  ,sum(QTY_BT) as QTY_BT
--into #Volumn_Bt_by_tambon
FROM df
--where [YearMonth] >=  @LY_F
group by YearMonth,p_name_t,a_name_t,t_name_t

"""

df2=ps.sqldf(q1, locals())
print(df2.columns, ' ----- ',len(df2),' :::  ',df2.tail(10))

q1= """
    select  [YearMonth],[p_name_t],[a_name_t],[t_name_t]
	,count(distinct([CUST_CODE_12DIGI]))  CNT_CUST
	,count(distinct(case when [BRAND] = 'เบียร์ช้าง25ปี' and [BRAND_GRP] = 'Chang' then [CUST_CODE_12DIGI] end)) as CNT_CHANG_25TH
	,count(distinct(case when [BRAND] != 'เบียร์ช้าง25ปี' and [BRAND_GRP] = 'Chang' then [CUST_CODE_12DIGI] end)) as CNT_CHANG_CC
	,count(distinct(case when [BRAND_GRP] = 'Leo' then [CUST_CODE_12DIGI] end)) as CNT_LEO
	,count(distinct(case when [BRAND_GRP] = 'Signha' then [CUST_CODE_12DIGI] end)) as CNT_SIGNHA
	-- focus 5 sku -- (N'เบียร์ช้าง 25 ปี',N'เบียร์ช้าง 25 ปี 490 CAN',N'คลาสสิก 320 CAN',N'คลาสสิก 490 CAN',N'คลาสสิก 620')
	,count(distinct(case when PRODUCT = 'เบียร์ช้าง 25 ปี'			then [CUST_CODE_12DIGI] end)) as CNT_PRD_CH25_620_BT
	,count(distinct(case when PRODUCT = 'เบียร์ช้าง 25 ปี 490 CAN'	then [CUST_CODE_12DIGI] end)) as CNT_PRD_CH25_490_BT
	,count(distinct(case when PRODUCT = 'คลาสสิก 320 CAN'		then [CUST_CODE_12DIGI] end)) as CNT_PRD_CC_320_CAN
	,count(distinct(case when PRODUCT = 'คลาสสิก 490 CAN'		then [CUST_CODE_12DIGI] end)) as CNT_PRD_CC_490_CAN
	,count(distinct(case when PRODUCT = 'คลาสสิก 620'			then [CUST_CODE_12DIGI] end)) as CNT_PRD_CC_620_BT
	-----------------
--into #ND_CNT_by_tambon
from df
--where [YearMonth] >=  @LY_F
group by [YearMonth],[p_name_t],[a_name_t],[t_name_t]

"""
df3=ps.sqldf(q1, locals())
print(df3.columns, ' - df3---- ',len(df3),' :::  ',df3.tail(10))

# pandasql has no format function and no N in front of Thai word
q1= """
   select REC_DATE_2 as YearMonth
	,REC_DATE,CUST_CODE,PRODUCT,p_name_t,a_name_t,t_name_t
	,case when PRODUCT in ('เบียร์ช้าง 25 ปี','เบียร์ช้าง 25 ปี 490 CAN','คลาสสิก 320 CAN','คลาสสิก 490 CAN','คลาสสิก 620') then 1 else 0 end as IS_FOCUS_SKU
	from df

"""
df4=ps.sqldf(q1, locals())
print(df4.columns, ' --df4--- ',len(df4),' :::  ',df4.tail(10))




q1= """
   select A.YearMonth,A.CUST_CODE,A.p_name_t, A.a_name_t, A.t_name_t
	,sum(A.IS_FOCUS_SKU) cnt5SKU
	,case when sum(A.IS_FOCUS_SKU) = 5 then 1 else 0 end as IS_FULLY
--into #skufully
from df4 A
group by A.YearMonth,A.CUST_CODE,A.p_name_t, A.a_name_t, A.t_name_t
having sum(A.IS_FOCUS_SKU) = 5 

"""
df5=ps.sqldf(q1, locals())
print(df5.columns, ' --df5--- ',len(df5),' :::  ',df5.tail(10))

df3['key']=df3['YEARMONTH']+df3['p_name_t']+df3['a_name_t']+df3['t_name_t']
df2['key']=df2['YEARMONTH']+df2['p_name_t']+df2['a_name_t']+df2['t_name_t']

q1= """
   select YearMonth, p_name_t, a_name_t, t_name_t ,count(distinct(CUST_CODE)) as CNT_SHOP_5SKU
	from df5 group by YearMonth, p_name_t, a_name_t, t_name_t	
	

"""
df6=ps.sqldf(q1, locals())
print(df6.columns, ' --df6--- ',len(df6),' :::  ',df6.tail(10))
df6['key']=df6['YearMonth']+df6['p_name_t']+df6['a_name_t']+df6['t_name_t']

# pandasql has no concat
q1= """
    select A.*
	,C.CNT_SHOP_5SKU
    ,B.QTY_BT
	,B.QTY_BT_CHANG_25TH
	,B.QTY_BT_CHANG_CC
	,B.QTY_BT_LEO
	,B.QTY_BT_Signha 
from df3 A
left join df2 B on A.key = B.key
left join df6 C on A.key = C.key

"""
df7=ps.sqldf(q1, locals())
print(df7.columns, ' -df7---- ',len(df7),' :::  ',df7.tail(10))