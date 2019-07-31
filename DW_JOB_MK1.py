#This is the .py Script to build TI-DW


######## REQUIRED LIBRARIES ###################
import pandas as pd
import glob
import numpy as np
from datetime import date, timedelta
import datetime
import DW_BUILD_PATH
import pymssql
import os

        
BASEPATH = DW_BUILD_PATH.BASEPATH
BASEPATHEXT = DW_BUILD_PATH.BASEPATHEXT


def appendcsvs(path):
    allFiles = glob.glob(path + "/*.csv")

    list_ = []

    for file_ in allFiles:
        
        df = pd.read_csv(file_,index_col=None, header=0 ,encoding = 'ISO-8859-1',error_bad_lines=False,low_memory = False)
        list_.append(df)

    df = pd.concat(list_, axis = 0, ignore_index = True )

    return df

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_ASSIGNMENT")


def appendcsvsfn(path):
    allFiles = glob.glob(path + "/*.csv")

    list_ = []

    for file_ in allFiles:
        
        df = pd.concat([pd.read_csv(file_,index_col=None, header=0 ,encoding = 'ISO-8859-1',error_bad_lines=False,low_memory = False).assign(New=os.path.basename(file_)) for fp in file_])
        list_.append(df)

    df = pd.concat(list_, axis = 0, ignore_index = True )

    return df

##########################################DIM_ASSIGNMENT############################################################

path = BASEPATH +'/sisenseassignment' 
df4 = appendcsvs(path)


df4.drop('ORDER#', axis=1, inplace=True)
df4.drop('CLIENT ID', axis=1, inplace=True)
df4.drop('JOB#', axis=1, inplace=True)
df4.drop('EMP ID', axis=1, inplace=True)
df4.drop('PAY RATE', axis=1, inplace=True)
df4.drop('BILL RATE', axis=1, inplace=True)
df4.drop('ENTERED BY USER', axis=1, inplace=True)
df4.drop('445 DISPATCH DATE', axis=1, inplace=True)
df4.drop('POSITION FILLED BY OFFICE', axis=1, inplace=True)
df4.drop('Unnamed: 12', axis=1, inplace=True)
df4['DISPATCH DATE'] = pd.to_datetime(df4['DISPATCH DATE'])
df4.columns= ['ASSIGNMENT_ID','ASSIGNMENT_DISPATCH_DATE','ASSIGNMENT_PSG_RECRUIT_FLAG',]
df4['ASSIGNMENT_PSG_RECRUIT_FLAG'] = df4['ASSIGNMENT_PSG_RECRUIT_FLAG'].fillna("N").astype(str)
df4 = df4[pd.to_numeric(df4['ASSIGNMENT_ID'], errors='coerce').notnull()]
df4 = df4.drop_duplicates(subset='ASSIGNMENT_ID', keep='last', inplace=False)
df4.to_csv(BASEPATHEXT + '/DIM_ASSIGNMENT.csv', sep=',', encoding='utf-8', index = False)


df4 = None


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_EMPLOYEE")

#############################DIM_EMPLOYEE###########################################################################

df8=pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')


df8.drop('MIDDLE', axis=1, inplace=True)
df8.drop('ADDR1', axis=1, inplace=True)
df8.drop('ADDR2', axis=1, inplace=True)
df8.drop('CITY', axis=1, inplace=True)
df8.drop('STATE', axis=1, inplace=True)
df8.drop('HIRED BY EMP ID', axis=1, inplace=True)
df8.drop('HIRE DATE', axis=1, inplace=True)
df8.drop('445 HIRE DATE', axis=1, inplace=True)
df8.drop('LAST WORKED', axis=1, inplace=True)
df8.drop('TI-CLC MUTUAL', axis=1, inplace=True)
df8.drop('PAY RATE', axis=1, inplace=True)
df8.drop('Unnamed: 25', axis=1, inplace=True)
df8['ZIP'] = df8['ZIP'].str[:5]
df8['EMP ID'] = df8['EMP ID'].fillna(0).astype(str)
df8['HOME OFFICE#'] = df8['HOME OFFICE#'] .map(lambda x: str(x)[:-2])
df8.columns = ['EMPLOYEE_ID', 'EMPLOYEE_LAST_NAME','EMPLOYEE_FIRST_NAME','EMPLOYEE_ZIP','EMPLOYEE_OFFICE_ID','EMPLOYEE_STATUS','EMPLOYEE_POSITION',
               'EMPLOYEE_CLC_LEGACY','EMPLOYEE_LAST_WORKED_DATE','EMPLOYEE_OSHA_DATE','EMPLOYEE_ACC_HOURS','ICIMS_ID', 'MARINE_OSHA_DATE', 'MSHA_DATE']
df8['EMPLOYEE_OSHA_DATE'] = pd.to_datetime(df8['EMPLOYEE_OSHA_DATE'],errors = 'coerce')
df8['MARINE_OSHA_DATE'] = pd.to_datetime(df8['MARINE_OSHA_DATE'],errors = 'coerce')
df8['MSHA_DATE'] = pd.to_datetime(df8['MSHA_DATE'],errors = 'coerce')
df8['EMPLOYEE_LAST_WORKED_DATE'] = pd.to_datetime(df8['EMPLOYEE_LAST_WORKED_DATE'],errors = 'coerce')
df8.drop('EMPLOYEE_ACC_HOURS', axis=1, inplace=True)
df8['EMPLOYEE_OSHA_CERTIFIED_FLAG'] = np.where(df8['EMPLOYEE_OSHA_DATE'].between('1992-08-08',df8['EMPLOYEE_LAST_WORKED_DATE']),"Y","N")
df8['EMPLOYEE_MARINE_OSHA_CERTIFIED_FLAG'] = np.where(df8['MARINE_OSHA_DATE'].between('1992-08-08',df8['EMPLOYEE_LAST_WORKED_DATE']),"Y","N")
df8['EMPLOYEE_MSHA_CERTIFIED_FLAG'] = np.where(df8['MSHA_DATE'].between('1992-08-08',df8['EMPLOYEE_LAST_WORKED_DATE']),"Y","N")
df8.drop('EMPLOYEE_OSHA_DATE', axis=1, inplace=True)
df8.drop('MARINE_OSHA_DATE', axis=1, inplace=True)
df8.drop('MSHA_DATE', axis=1, inplace=True)
df8.drop('EMPLOYEE_LAST_WORKED_DATE', axis=1, inplace=True)
df8['EMPLOYEE_CLC_LEGACY'] = df8['EMPLOYEE_CLC_LEGACY'].fillna('N')



df5 = pd.read_csv(BASEPATHEXT + '/wf_TRADES/TRADES.csv',encoding='windows-1252')
df5.columns=['EMPLOYEE_POSITION','EMPLOYEE_TRADE','EMPLOYEE_PRIMARY_GROUP','EMPLOYEE_BLS_CODE','EMPLOYEE_BLS_GROUPING']

df6 = df8.merge(df5,on=["EMPLOYEE_POSITION"], how = "left")
df6.drop('EMPLOYEE_POSITION', axis=1, inplace=True)

df5=None
df8=None

df7=pd.read_csv(BASEPATHEXT +'/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df7['ZIP'] = df7['ZIP'].astype(str)
df7['ZIP'] = df7['ZIP'].astype(str).str.zfill(5)
df7.drop('MSA_NUM', axis=1, inplace=True)
df7.columns = ['EMPLOYEE_ZIP','EMPLOYEE_STATE','EMPLOYEE_GEO_REGION','EMPLOYEE_GEO_DIVISION','EMPLOYEE_MSA','EMPLOYEE_COUNTY', 'EMPLOYEE_CITY']

df9 = df6.merge(df7,on=["EMPLOYEE_ZIP"], how = "left")

df7 = None
df6 = None

df9['EMPLOYEE_FULL_NAME'] = df9['EMPLOYEE_LAST_NAME']+", "+df9['EMPLOYEE_FIRST_NAME']
df9['EMPLOYEE_MAP_POINT'] = df9['EMPLOYEE_CITY']

df9 = df9.drop_duplicates(subset='EMPLOYEE_ID', keep='last', inplace=False)


path = BASEPATH + '/sisenserehire' 
df14 = appendcsvs(path)

df14.columns = [c.replace(' ', '_') for c in df14.columns]
df14.columns = [c.replace('-', '_') for c in df14.columns]
df14 = df14[['EMP_ID']]
df14.columns=['EMPLOYEE_ID']
df14['REHIRE'] = 'Y'
df14['EMPLOYEE_ID'] = df14['EMPLOYEE_ID'].astype(str)
df14 = df14.drop_duplicates()


df9 = df9.merge(df14,on=["EMPLOYEE_ID"], how = "left")

df9.to_csv(BASEPATHEXT + '/DIM_EMPLOYEE4.csv', sep=',', encoding='utf-8',index = False)


df9 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_JOB")

#############################DIM_JOB###########################################################################

path = BASEPATH + '/sisensejob' 
df = appendcsvs(path)


df.drop('ORDER#', axis=1, inplace=True)
df.drop('CLIENT ID', axis=1, inplace=True)
df.drop('CITY', axis=1, inplace=True)
df.drop('STATE', axis=1, inplace=True)
df.drop('POSITION CODE', axis=1, inplace=True)
df.drop('Unnamed: 10', axis=1, inplace=True)
df.drop('Unnamed: 11', axis=1, inplace=True)
df['PW JOB (Y)'] = df['PW JOB (Y)'].fillna('N').astype(str)
df['OCIP (Y)'] = df['OCIP (Y)'].fillna('N').astype(str)
df['ZIP'] = df['ZIP'].astype(str)
df['ZIP'] = df['ZIP'].str[:5]
df.columns = ['JOB_INDUSTRY','JOB_ID','OCIP_FLAG','JOB_PROJECT_NAME','JOB_PW_FLAG','JOB_ZIP']

df9 = pd.read_csv(BASEPATHEXT + '/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df9['ZIP'] = df9['ZIP'].astype(str)
df9['ZIP'] = df9['ZIP'].astype(str).str.zfill(5)
df9.drop('MSA_NUM', axis=1, inplace=True)
df9.columns = ['JOB_ZIP','JOB_STATE','JOB_GEO_REGION','JOB_GEO_DIVISION','JOB_MSA','JOB_COUNTY', 'JOB_CITY']

df10 = df.merge(df9,on=["JOB_ZIP"], how = "left")
df10['JOB_MAP_POINT'] = df10['JOB_CITY']

df10 = df10.drop_duplicates(subset='JOB_ID', keep='last', inplace=False)


df10.to_csv(BASEPATHEXT + '/DIM_JOB.csv', sep=',', encoding='utf-8', index = False)

df = None
df9 = None
df10 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_OFFICE_EMPLOYEE")


#############################DIM_OFFICE_EMPLOYEE###########################################################################

df8=pd.read_csv(BASEPATH + '/sisenseofficeemployee/officeemployees.csv',encoding='utf-8')
df8.drop('MIDDLE', axis=1, inplace=True)
df8.drop('Unnamed: 21', axis=1, inplace=True)

df8['ZIP'] = df8['ZIP'].str[:5]
df8['EMP ID'] = df8['EMP ID'].fillna(0).astype(str)
df8['OFFICE#'] = df8['OFFICE#'] .map(lambda x: str(x)[:-2])
df8['DEPARTMENT CODE'] = df8['DEPARTMENT CODE'] .map(lambda x: str(x)[:-2])
df8.columns = ['OFFICE_EMPLOYEE_ID', 'OFFICE_EMPLOYEE_LAST_NAME','OFFICE_EMPLOYEE_FIRST_NAME','OFFICE_EMPLOYEE_GENDER','OFFICE_EMPLOYEE_ZIP',
               'OFFICE_EMPLOYEE_POSITION_CODE','OFFICE_EMPLOYEE_POSITION_DESC','OFFICE_EMPLOYEE_DEPARTMENT_CODE',
               'OFFICE_EMPLOYEE_DEPARTMENT_DESC','OFFICE_EMPLOYEE_OFFICE_ID','OFFICE_EMPLOYEE_OFFICE_NAME','OFFICE_EMPLOYEE_DOB',
               'OFFICE_EMPLOYEE_DOH','OFFICE_EMPLOYEE_DLW','OFFICE_EMPLOYEE_DRH','OFFICE_EMPLOYEE_TERMED','ICIMS_ID','TERM_REASON','RECRUITED_BY','HIRING_SOURCE']
df8['OFFICE_EMPLOYEE_DOB'] = pd.to_datetime(df8['OFFICE_EMPLOYEE_DOB'],errors = 'coerce')
df8['OFFICE_EMPLOYEE_DOH'] = pd.to_datetime(df8['OFFICE_EMPLOYEE_DOH'],errors = 'coerce')
df8['OFFICE_EMPLOYEE_DLW'] = pd.to_datetime(df8['OFFICE_EMPLOYEE_DLW'],errors = 'coerce')
df8['OFFICE_EMPLOYEE_DRH'] = pd.to_datetime(df8['OFFICE_EMPLOYEE_DRH'],errors = 'coerce')
df8['OFFICE_EMPLOYEE_TERMED'] = pd.to_datetime(df8['OFFICE_EMPLOYEE_TERMED'],errors = 'coerce')

df7=pd.read_csv(BASEPATHEXT +'/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df7['ZIP'] = df7['ZIP'].astype(str)
df7['ZIP'] = df7['ZIP'].astype(str).str.zfill(5)
df7.drop('MSA_NUM', axis=1, inplace=True)
df7.columns = ['OFFICE_EMPLOYEE_ZIP','OFFICE_EMPLOYEE_STATE','OFFICE_EMPLOYEE_GEO_REGION','OFFICE_EMPLOYEE_GEO_DIVISION','OFFICE_EMPLOYEE_MSA','OFFICE_EMPLOYEE_COUNTY','OFFICE_EMPLOYEE_CITY']

df9 = df8.merge(df7,on=["OFFICE_EMPLOYEE_ZIP"], how = "left")
df9.drop('OFFICE_EMPLOYEE_ZIP', axis=1, inplace=True)
df9['OFFICE_EMPLOYEE_FULL_NAME'] = df9['OFFICE_EMPLOYEE_LAST_NAME']+", "+df9['OFFICE_EMPLOYEE_FIRST_NAME']

df9 = df9.drop_duplicates(subset='OFFICE_EMPLOYEE_ID', keep='last', inplace=False)


FSCTitles = ['SUPPORT CENTER ASSOCIATE','SUPPORT CENTER MANAGER','SUPPORT CENTER SUPERVISOR','SUPPORT  CENTER ASSOCIATE','SOURCING COORDINATOR','VP OPERATIONS','INTERIM SUPPORT CENTER MANAGER'
             ,'FIELD SUPPORT CENTER MANAGER','PROJECT MANAGER']
PerDiemNames = ['STEWART, HOLLY' , 'EARLY, RACHEL' , 'COLE, AUSTIN']

df9['FSC_FLAG'] = np.where(np.isin(df9['OFFICE_EMPLOYEE_POSITION_DESC'],FSCTitles),'FSC','FIELD')
df9['FSC_FLAG'] = np.where(np.isin(df9['OFFICE_EMPLOYEE_FULL_NAME'],PerDiemNames),'FSC - PD',df9['FSC_FLAG'])


df9['USER_ID'] = df9['OFFICE_EMPLOYEE_FIRST_NAME']+"."+df9['OFFICE_EMPLOYEE_LAST_NAME']
df9['OFFICE_EMPLOYEE_MAP_POINT'] = df9['OFFICE_EMPLOYEE_CITY']

df9['PC_TYPE'] =  np.where(df9['OFFICE_EMPLOYEE_OFFICE_ID'] == '99', 'CENTRAL','FIELD')


df10 = pd.read_csv(BASEPATHEXT + '/wf_HR/OFFICE_EMPLOYEE_DUAL_USER_ADDENDUM.csv',encoding='windows-1252')
df11 = df9.append(df10)

df12 = pd.read_csv(BASEPATHEXT + '/wf_HR/GENERATION.csv',encoding='utf-8')
df12['YEAR'] = df12['YEAR'].astype(str)
df13 = pd.read_csv(BASEPATHEXT + '/wf_HR/JOB_TYPE.csv',encoding='utf-8')

df11['OFFICE_EMPLOYEE_DOB'] = df11['OFFICE_EMPLOYEE_DOB'].astype(str)
df11['YEAR'] = df11['OFFICE_EMPLOYEE_DOB'].str[:4]


df14 = df11.merge(df12,on=["YEAR"], how = "left")
df14.drop('YEAR', axis=1, inplace=True)

df15 = df14.merge(df13,on=["OFFICE_EMPLOYEE_DEPARTMENT_CODE"], how = "left")
df15['OFFICE_EMPLOYEE_DOH'] = pd.to_datetime(df15['OFFICE_EMPLOYEE_DOH'],errors = 'coerce')
df15['OFFICE_EMPLOYEE_TERMED'] = pd.to_datetime(df15['OFFICE_EMPLOYEE_TERMED'],errors = 'coerce')
df15 = df15.dropna(axis=0, subset=['OFFICE_EMPLOYEE_DOH'])

df15['OFFICE_EMPLOYEE_TENURE'] = np.where(pd.isnull(df15['OFFICE_EMPLOYEE_TERMED']),
                                 ((pd.Timestamp(date.today()) - df15['OFFICE_EMPLOYEE_DOH']).dt.days).astype(float)/30,
                                 ((df15['OFFICE_EMPLOYEE_TERMED'] - df15['OFFICE_EMPLOYEE_DOH']).dt.days).astype(float)/30)

df15['OFFICE_EMPLOYEE_TENURE_GROUP'] = np.where(df15['OFFICE_EMPLOYEE_TENURE'].between(0, 3), "1. 0 - 2 MONTHS",
                                       np.where(df15['OFFICE_EMPLOYEE_TENURE'].between(3.01, 6), "2. 3 - 6 MONTHS",   
                                       np.where(df15['OFFICE_EMPLOYEE_TENURE'].between(6.01, 12), "3. 7 - 12 MONTHS",
                                       np.where(df15['OFFICE_EMPLOYEE_TENURE'].between(12.01, 24), "4. 1 - 2 YEARS","5. OVER 2 YEARS"))))

df15['HIRING_PROCESS'] = np.where(pd.to_datetime(df15['OFFICE_EMPLOYEE_DOH'],errors = 'coerce') < pd.to_datetime('2018-10-01') , 'LEGACY', np.where(np.logical_and(pd.to_datetime(df15['OFFICE_EMPLOYEE_DOH'],errors = 'coerce') >= pd.to_datetime('2018-10-01') , df15['RECRUITED_BY'].isin(['MACHAEL.NORRIS','ANDREA.HERBSTHAUSER','KYLIE.STITELER','AMY.POLICE','AISHA.HOLT', 'ALYSSA.PUSA','JENA.SCHNEIDER','ALLISON.BOCKMULLER'])), 'NEW HIRING PROCESS','OLD HIRING PROCESS'))
df15['EMAIL']=  df15['USER_ID'].str.lower() + '@tradesmeninternational.com'

dfbh = pd.read_csv(BASEPATHEXT +'/wf_HR/BH_MOBILE.csv',encoding='windows-1252')
dfbh['OFFICE_EMPLOYEE_ID'] = dfbh['OFFICE_EMPLOYEE_ID'].astype(str)
df16 = df15.merge(dfbh,on=["OFFICE_EMPLOYEE_ID"], how = "left")
df16['BILLHORN_MOBILE'] = df16['BILLHORN_MOBILE'].fillna('N')



df16.to_csv(BASEPATHEXT +'/DIM_OFFICE_EMPLOYEE7.csv', sep=',', encoding='utf-8',index = False)

df7 = None
df8 = None
df9 = None
df10 = None


df11 = df11[['FSC_FLAG','OFFICE_EMPLOYEE_CITY','OFFICE_EMPLOYEE_DEPARTMENT_DESC','OFFICE_EMPLOYEE_DOH','OFFICE_EMPLOYEE_FULL_NAME',
             'OFFICE_EMPLOYEE_POSITION_CODE','OFFICE_EMPLOYEE_TERMED','USER_ID']]

df11.to_csv(BASEPATHEXT +'/DIM_ENTRY_EMPLOYEE.csv', sep=',', encoding='utf-8',index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_OFFICE")

#############################DIM_OFFICE#######################################################################################

df4=pd.read_csv(BASEPATH + '/sisenseoffice2/office2.csv',encoding='windows-1252')
df4.drop('AREA#', axis=1, inplace=True)
df4.drop('REGION#', axis=1, inplace=True)
df4.drop('OPEN DATE MONTHYEAR', axis=1, inplace=True)
df4.drop('Unnamed: 9', axis=1, inplace=True)
df4['FIRST SALE'] = df4['FIRST SALE'].str[-4:]
df4.columns = ['OFFICE_ID','OFFICE_NAME','OFFICE_AREA_NAME','OFFICE_REGION_NAME','OFFICE_STATUS','OFFICE_VINTAGE']

df5 = pd.read_csv(BASEPATHEXT +'/wf_OFFICE/TRADESMEN_INTERNATIONAL_OFFICE_DIMENSION.csv',encoding='windows-1252')
df5.drop('OFFICE_STATE', axis=1, inplace=True)
df5.drop('OFFICE_MSA', axis=1, inplace=True)

df6 = df4.merge(df5,on=["OFFICE_ID"], how = "left")
df6['OFFICE_ZIP'] = df6['OFFICE_ZIP'].astype(str)
df6['OFFICE_ZIP'] = df6['OFFICE_ZIP'].astype(str).str.zfill(5)

df4 = None
df5 = None

df7=pd.read_csv(BASEPATHEXT + '/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df7['ZIP'] = df7['ZIP'].astype(str)
df7['ZIP'] = df7['ZIP'].astype(str).str.zfill(5)
df7.drop('MSA_NUM', axis=1, inplace=True)
df7.columns = ['OFFICE_ZIP','OFFICE_STATE','OFFICE_GEO_REGION','OFFICE_GEO_DIVISION','OFFICE_MSA','OFFICE_COUNTY', 'OFFICE_CITY']

df8 = df6.merge(df7,on=["OFFICE_ZIP"], how = "left")
df8['OFFICE_MAP_POINT'] = df8['OFFICE_CITY']
df8['CLASS'] = np.where(df8['OFFICE_REGION_NAME'] == 'Marine' , 'MARINE' , np.where(df8['OFFICE_REGION_NAME'] == 'Major Accounts' , 'STRATEGIC' , 'SMB'))


df6 = None
df7 = None

df8 = df8.drop_duplicates(subset='OFFICE_ID', keep='last', inplace=False)

df8.to_csv(BASEPATHEXT + '/DIM_OFFICE.csv', sep=',', encoding='utf-8',index = False)

df8 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_ORDER")

#############################DIM_ORDER#######################################################################################

df4=pd.read_csv(BASEPATH + '/sisenseorder/order.csv',encoding='windows-1252')



df4.drop('ORDER OFFICE#', axis=1, inplace=True)
df4.drop('ORDER DATE', axis=1, inplace=True)
df4.drop('START DATE', axis=1, inplace=True)
df4.drop('TRADE CODE', axis=1, inplace=True)
df4.drop('ORDERED', axis=1, inplace=True)
df4.drop('FILLED', axis=1, inplace=True)
df4.drop('UNFILLED', axis=1, inplace=True)
df4.drop('REP GENERATED ID', axis=1, inplace=True)
df4.drop('ORDER CLOSED DATE', axis=1, inplace=True)
df4.drop('ORDER LENGTH DAYS', axis=1, inplace=True)
df4.drop('Unnamed: 20', axis=1, inplace=True)
df4.columns= ['ORDER_ID','CLIENT_ID','ORDER_STATUS','ORDER_CLOSED_REASON','ORDER_PSG_TRAVELERS','ORDER_DATE', 'ORDER_START_DATE',
             'ORDER_CLOSED_DATE','ORDER_FIRST_TIMECARD_DATE','ORDER_LAST_TIMECARD_DATE']
df4['ORDER_DATE'] = pd.to_datetime(df4['ORDER_DATE'])
df4['ORDER_START_DATE'] = pd.to_datetime(df4['ORDER_START_DATE'])
df4['ORDER_CLOSED_DATE'] = pd.to_datetime(df4['ORDER_CLOSED_DATE'])
df4['ORDER_FIRST_TIMECARD_DATE'] = pd.to_datetime(df4['ORDER_FIRST_TIMECARD_DATE'], errors = 'coerce')
df4['ORDER_LAST_TIMECARD_DATE'] = pd.to_datetime(df4['ORDER_LAST_TIMECARD_DATE'], errors = 'coerce')
df4['ORDER_CLOSED_REASON'] = df4['ORDER_CLOSED_REASON'].fillna('FILLED')
df4['ORDER_PSG_TRAVELERS'] = df4['ORDER_PSG_TRAVELERS'].fillna('N')
df4 = df4[pd.to_numeric(df4['ORDER_ID'], errors='coerce').notnull()]

df4 = df4.drop_duplicates(subset='ORDER_ID', keep='last', inplace=False)


df4.to_csv(BASEPATHEXT + '/DIM_ORDER.csv', sep=',', encoding='utf-8', index = False)


df4 = None


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_POSITION")

#############################DIM_POSITION#######################################################################################

df4=pd.read_csv(BASEPATH + '/sisensepositioncode/position.csv',encoding='windows-1252')
df4.drop('Unnamed: 5', axis=1, inplace=True)

df5 = pd.read_csv(BASEPATHEXT + '/wf_TRADES/WC RATES.csv',encoding='windows-1252')
df5.columns = ['POSITION CODE', 'WCPCT','WCPCT_BIN']

df6 = df4.merge(df5,on=["POSITION CODE"], how = "left")
df6.drop('OCC CODE', axis=1, inplace=True)
df6.columns = ['INVOICE_POSITION','INVOICE_POSIION_DESC','WC_CODE','OCCUPATION_DESC','WC_PCT','WC_PCT_BIN']
df6['INVOICE_POSITION'] =  df6['INVOICE_POSITION'].str.upper()

df4 = None
df5 = None

df7=pd.read_csv(BASEPATHEXT + '/wf_TRADES/TRADE_GROUP.csv',encoding='windows-1252')
df7.columns = ['OCCUPATION_DESC', 'OCCUPATION_GROUP']

df8 = df6.merge(df7,on=["OCCUPATION_DESC"], how = "left")

df6 = None
df7 = None

df9=pd.read_csv(BASEPATHEXT + '/wf_TRADES/POSITION_PRICE_CHANGE_BIN.csv',encoding='windows-1252')
df9.columns = ['INVOICE_POSITION', 'PRICE_CHANGE']

df10 = df8.merge(df9,on=["INVOICE_POSITION"], how = "left")


df10 = df10.drop_duplicates(subset='INVOICE_POSITION', keep='last', inplace=False)

df10.to_csv(BASEPATHEXT + '/DIM_POSITION.csv', sep=',', encoding='utf-8',index = False)

df10 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_CLIENT")

#############################DIM_CLIENT###########################################################################

df4=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')

df4['CLIENT ID']= df4['CLIENT ID'].str.upper()



df4.drop('ADDR1', axis=1, inplace=True)
df4.drop('ADDR2', axis=1, inplace=True)
df4.drop('CITY', axis=1, inplace=True)
df4.drop('STATE', axis=1, inplace=True)
df4.drop('REGISTRATION REP ID', axis=1, inplace=True)
df4.drop('Unnamed: 20', axis=1, inplace=True)
df4.drop('LAST SALE', axis=1, inplace=True)
df4.drop('445 REGISTRATION DATE', axis=1, inplace=True)

df4['ZIP'] = df4['ZIP'].str[:5]
df4['CLIENT ID'] = df4['CLIENT ID'].astype(str)
df4['HOME OFFICE#'] = df4['HOME OFFICE#'] .map(lambda x: str(x)[:-2])
df4.columns = ['CLIENT_ID', 'CLIENT_NAME','CLIENT_ZIP','OFFICE_ID','CLIENT_TERRITORY',
               'CLIENT_REP_ID','CLIENT_TSI_FLAG','CLIENT_REG_DATE','CLIENT_CLC_LEGACY_FLAG',
               'CLIENT_TI_CLC_MUTUAL_FLAG','CLIENT_STATUS','CLIENT_LAST_SALE_DATE','CLIENT_FIRST_SALE_DATE']
df4['CLIENT_REG_DATE'] = pd.to_datetime(df4['CLIENT_REG_DATE'])
df4['CLIENT_LAST_SALE_DATE'] = pd.to_datetime(df4['CLIENT_LAST_SALE_DATE'])
df4['CLIENT_FIRST_SALE_DATE'] = pd.to_datetime(df4['CLIENT_FIRST_SALE_DATE'])
df4['CLIENT_REP_ID'] = df4['CLIENT_REP_ID'].astype(str)
df4['CLIENT_REP_ID'] = df4['CLIENT_REP_ID'].map(lambda x: str(x)[:-2])
df4['CLIENT_REP_ID'] = df4['CLIENT_REP_ID'].fillna('OPEN')


df5 = pd.read_csv(BASEPATHEXT + '/wf_CLIENT/ABC_MEMBERS.csv',encoding='windows-1252')

df6 = df4.merge(df5,on=["CLIENT_ID"], how = "left")
df6['ABC_MEMBER'] = np.where(df6['ABC_MEMBER']=="Y",1,0)
df6['CLIENT_TSI_FLAG'] = df6['CLIENT_TSI_FLAG'].fillna("N").astype(str)
df6['CLIENT_CLC_LEGACY_FLAG'] = df6['CLIENT_CLC_LEGACY_FLAG'].fillna("N").astype(str)
df6['CLIENT_TI_CLC_MUTUAL_FLAG'] = df6['CLIENT_TI_CLC_MUTUAL_FLAG'].fillna("N").astype(str)

df7=pd.read_csv(BASEPATH + '/sisenseoffice2/office2.csv',encoding='windows-1252')
df7.drop('STATUS', axis=1, inplace=True)
df7.drop('OPEN DATE MONTHYEAR', axis=1, inplace=True)
df7.drop('FIRST SALE', axis=1, inplace=True)
df7.drop('Unnamed: 9', axis=1, inplace=True)
df7.drop('AREA#', axis=1, inplace=True)
df7.drop('REGION#', axis=1, inplace=True)
df7.columns = ['OFFICE_ID','CLIENT_OFFICE_NAME','CLIENT_AREA_NAME','CLIENT_REGION_NAME']
df7['OFFICE_ID'] = df7['OFFICE_ID'].astype(str)

df8 = df6.merge(df7,on=["OFFICE_ID"], how = "left")

df9 = pd.read_csv(BASEPATHEXT + '/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df9['ZIP'] = df9['ZIP'].astype(str)
df9['ZIP'] = df9['ZIP'].astype(str).str.zfill(5)
df9.columns = ['CLIENT_ZIP','CLIENT_STATE','CLIENT_GEO_REGION','CLIENT_GEO_DIVISION','CLIENT_MSA_NUM','CLIENT_MSA','CLIENT_COUNTY', 'CLIENT_CITY']


df10 = df8.merge(df9,on=["CLIENT_ZIP"], how = "left")
df10['LPI_FLAG'] =  np.where(df10['CLIENT_ID'] == '166995', "Y","N")
df10['CLIENT_VINTAGE'] = df10['CLIENT_REG_DATE'].dt.year
df10['CLIENT_VINTAGE'] = df10['CLIENT_VINTAGE'].fillna(0).astype(int)
df10['CLIENT_BEFORE_DATASET'] = np.where(df10['CLIENT_VINTAGE'] < 2016,'Y','N')
df10['CLIENT_MAP_POINT'] = df10['CLIENT_CITY']

df11 = pd.read_csv(BASEPATHEXT + '/wf_CLIENT/DUNS_CLIENT.csv',encoding='windows-1252')
df11.columns = ['CLIENT_ID','CLIENT_DUNS','CLIENT_SITE_TYPE','CLIENT_INDUSTRY', 'CLIENT_REVENUE_LEVEL','CLIENT_EMPLOYEE_TOTAL',	'CLIENT_FOUNDED']
df11['CLIENT_ID'] = df11['CLIENT_ID'].astype(str)

df12 = df10.merge(df11,on=["CLIENT_ID"], how = "left")

df13 = pd.read_csv(BASEPATHEXT + '/DIM_OFFICE_EMPLOYEE7.csv',encoding='windows-1252')
df13 = df13[['OFFICE_EMPLOYEE_ID','OFFICE_EMPLOYEE_FULL_NAME']]
df13.columns = ['CLIENT_REP_ID' , 'SALES_REP']
df13['CLIENT_REP_ID'] = df13['CLIENT_REP_ID'].astype(str)

df14 = df12.merge(df13,on=["CLIENT_REP_ID"], how = "left")
df14['SALES_REP'] = df14['SALES_REP'].fillna('NO REP ASSIGNED')

df14 = df14.drop_duplicates(subset='CLIENT_ID', keep='first', inplace=False)

df14.to_csv(BASEPATHEXT +'/DIM_CLIENT.csv', sep=',', encoding='utf-8', index = False)


df4 = None
df5 = None
df6 = None
df7 = None
df8 = None
df9 = None
df10 = None
df11 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_TIMECARD")

#############################DIM_TIMECARD#######################################################################################

path = BASEPATH + '/sisenseproduction' 
df1 = appendcsvs(path)


df1 = df1[['TIMECARD#','POSITION', 'HOURS']]
df1['TIMECARD#'] =  df1['TIMECARD#'].str[5:]
df1.columns = ['TIMECARD_ID','POSITION_ID','ACCUTERM_HOURS']

df1 = df1.groupby(by = ['TIMECARD_ID','POSITION_ID']).agg({'ACCUTERM_HOURS' : 'sum'}).reset_index()


df1.to_csv(BASEPATHEXT + '/DIM_TIMECARD.csv', sep=',', encoding='utf-8', index = False)

df1 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_PRODUCTION2")

#############################FACT_PRODUCTION2#######################################################################################

path =BASEPATH + '/sisenseproduction' 
df1 = appendcsvs(path)


df1.drop('445 INVOICE DATE', axis=1, inplace=True)
df1.drop('Unnamed: 21', axis=1, inplace=True)

df1['INVOICE DATE'] = pd.to_datetime(df1['INVOICE DATE'])

df1['YEAR'] = df1['INVOICE DATE'].dt.year
df1['MONTH'] = df1['INVOICE DATE'].dt.month
df1['YEAR'] = df1['YEAR'].fillna(0).astype(int)
df1['MONTH'] = df1['MONTH'].fillna(0).astype(int)
df1['EMP ID'] = df1['EMP ID'].fillna(0).astype(str)
df1['ASSIGNMENT#'] = df1['ASSIGNMENT#'].fillna(0).astype(str)
df1['CLIENT ID'] = df1['CLIENT ID'].fillna(0).astype(str)
df1['JOB#'] = df1['JOB#'].fillna(0).astype(str)
df1['ORDER#'] = df1['ORDER#'].fillna(0).astype(str)
df1['POSITION'] = df1['POSITION'].str.upper()

df1['BILLID']=df1['YEAR'].astype(str)+df1['MONTH'].astype(str)+df1['POSITION']+df1['PAY RATE'].astype(str)

df1.columns = ['EMPLOYEE_ID','CLIENT_ID','INVOICE_POSITION','TIME_CARD_ID','TXN_TYPE','INVOICE_HOURS','BILL_RATE',
                'SALES','INVOICE_PAY_RATE','INVOICE_WAGES','COST_OF_SALES','INVOICE_GP','INVOICE_DATE','INVOICE_ID','OFFICE_ID','TERRITORY_REP_ID','ORDER_ID',
                'JOB_ID','ASSIGNMENT_ID','INDUSTRY_TYPE','INVOICE_YEAR','INVOICE_MONTH','BILL_OUT_ID']


path = BASEPATHEXT + '/wf_BILL RATE/BILLRATETABLES' 
df2 = appendcsvs(path)
df2['BILLOUT CODE'] =  df2['BILLOUT CODE'].str.upper()


df2.drop('BILLID', axis=1, inplace=True)

df2['BILLID']=df2['YEAR'].astype(str)+df2['MONTH'].astype(str)+df2['BILLOUT CODE']+df2['PAY RATE'].astype(str)
df2.drop('BILLOUT CODE', axis=1, inplace=True)
df2.drop('OCC Code', axis=1, inplace=True)
df2.drop('PAY RATE', axis=1, inplace=True)
df2.drop('YEAR', axis=1, inplace=True)
df2.drop('MONTH', axis=1, inplace=True)

df2.columns = ['SUG_BILL_RATE','BILL_OUT_ID']

df3 = df1.merge(df2,on=["BILL_OUT_ID"], how = "left")

df1 = None
df2 = None


df3.drop('BILL_OUT_ID', axis=1, inplace=True)
df3.drop('INDUSTRY_TYPE', axis=1, inplace=True)

df3['SUG_BILL_RATE'] = df3['SUG_BILL_RATE'].fillna(0).astype(float)

df3['SUG_BILL_RATE'] = np.where(df3['SUG_BILL_RATE'] != 0.0, df3['SUG_BILL_RATE'],
                       np.where(df3['INVOICE_PAY_RATE'].between(6, 25), (.0013*df3['INVOICE_PAY_RATE']**2 - .0667*df3['INVOICE_PAY_RATE'] + 2.5743)*df3['INVOICE_PAY_RATE'],
                       np.where(df3['INVOICE_PAY_RATE'].between(25.01, 40), (.0003*df3['INVOICE_PAY_RATE']**2 - .0204*df3['INVOICE_PAY_RATE'] + 2.0562)*df3['INVOICE_PAY_RATE'],
                       np.where(df3['INVOICE_PAY_RATE'].between(40.01, 50), (.00006*df3['INVOICE_PAY_RATE']**2 - .0063*df3['INVOICE_PAY_RATE'] + 1.8376)*df3['INVOICE_PAY_RATE'],df3['INVOICE_PAY_RATE']*1.55))))
                       
df3['PAYRATE_BIN'] =   np.where(df3['INVOICE_WAGES']/df3['INVOICE_HOURS'] < 6, "Under $6",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(6, 12), "$6 - $12",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(12.01, 17), "$12.01 - $17",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(17.01, 22), "$17.01 - $22",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(22.01, 27), "$22.01 - $27",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(27.01, 32), "$27.01 - $32",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(32.01, 37), "$32.01 - $37",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(37.01, 42), "$37.01 - $42",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(42.01, 50), "$42.01 - $50","Over $50")))))))))

df3['PAYRATE_BIN_CRITICAL'] =   np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(20, 20.99), "$20",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(21, 21.99), "$21",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(22, 22.99), "$22",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(23, 23.99), "$23",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(24, 24.99), "$24",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(25, 25.99), "$25",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(26, 26.99), "$26",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(27, 27.99), "$27",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(28, 28.99), "$28",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(29, 29.99), "$29",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(30, 30.99), "$30",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(31, 31.99), "$31",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(32, 32.99), "$32",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(33, 33.99), "$33",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(34, 34.99), "$34",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(35, 35.99), "$35",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(36, 36.99), "$36",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(37, 37.99), "$37",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(38, 38.99), "$38",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(39, 39.99), "$39",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(40, 40.99), "$40",
                       np.where((df3['INVOICE_WAGES']/df3['INVOICE_HOURS']).between(41, 41.99), "$41","Not"))))))))))))))))))))))
                       
df3['BILL_GRAPH_FLAG'] = np.where(df3['INVOICE_PAY_RATE'] % .5 == 0,1,0)

df3['SUG_SALES']= np.where(df3['INVOICE_HOURS'] == 0,0,df3['SUG_BILL_RATE'] * df3['INVOICE_HOURS'])

df14 = pd.read_csv(BASEPATHEXT + '/wf_TRADES/WC RATES.csv',encoding='windows-1252')
df14.columns = ['INVOICE_POSITION', 'WC_PCT', "OTHER"]

df15 = df3.merge(df14,on=["INVOICE_POSITION"], how = "left")

df3 = None
df14 = None


df15['WC_PCT'] = df15['WC_PCT']/100
df15['WC_COST'] = np.where(df15['INVOICE_HOURS'] == 0,0,df15['WC_PCT'] * df15['INVOICE_WAGES'])
df15['PYL_TAX_HLTH_OTHER'] = df15['COST_OF_SALES'] - df15['WC_COST']
df15.drop('OTHER', axis=1, inplace=True)
df15.drop('WC_PCT', axis=1, inplace=True)

df16 = pd.read_csv(BASEPATHEXT + '/wf_PRODUCTION/TXN_TYPE.csv',encoding='windows-1252')

df17 = df15.merge(df16,on=["TXN_TYPE"], how = "left")

df15 = None
df16 = None

df22 = df17[['TIME_CARD_ID','INVOICE_HOURS']]
df22 = df22.groupby('TIME_CARD_ID').sum()['INVOICE_HOURS']
df22 = pd.DataFrame(data=df22)
df22['OVERTIME_FLAG'] = np.where(df22['INVOICE_HOURS'] > 40,1,0)
df22.drop('INVOICE_HOURS', axis=1, inplace=True)

df23 = df17.merge(df22,on=["TIME_CARD_ID"], how = "left")

df17 = None
df22 = None

df23['TIME_CARD_ID'] =  df23['TIME_CARD_ID'].str[5:]

df26 = np.unique(df23[['INVOICE_ID', 'OFFICE_ID']], axis = 0)
df26 = pd.DataFrame(data=df26)
df26.columns = ['INVOICE_ID', 'OFFICE_ID']
df26 = df26.groupby('INVOICE_ID').OFFICE_ID.nunique()
df26 = pd.DataFrame(data=df26)
df26['SPLIT_INVOICE'] = np.where(df26['OFFICE_ID'] > 1,1,0)
df26.drop('OFFICE_ID', axis=1, inplace=True)

df27 = df23.merge(df26,on=["INVOICE_ID"], how = "left")

df23 = None
df26 = None




df27['DISPATCH_ID'] = df27['EMPLOYEE_ID'].astype(str) + df27['ORDER_ID'].astype(str)




df27.to_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv', sep=',', encoding='windows-1252', index = False)



df27 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_COMPANY_FINANCIALS")

#############################FACT_COMPANY_FINANCIALS#######################################################################################


path = BASEPATH + '/sisensebudgetactual' 
df1 = appendcsvs(path)


df1['YEARMONTH'] = df1['YEARMONTH'].astype(str)
df1['YEARMONTH'] = df1['YEARMONTH'] +"01"
df1['YEARMONTH'] = pd.to_datetime(df1['YEARMONTH'])

df1.drop('Unnamed: 38', axis=1, inplace=True)

df1.columns = [c.replace(' ', '_') for c in df1.columns]

df1.drop('BUDGET_SALES', axis=1, inplace=True)
df1.drop('ACTUAL_SALES', axis=1, inplace=True)
df1.drop('BUDGET_GP', axis=1, inplace=True)
df1.drop('ACTUAL_GP', axis=1, inplace=True)
df1.drop('BUDGET_OE', axis=1, inplace=True)
df1.drop('ACTUAL_OE', axis=1, inplace=True)
df1.drop('BUDGET_OI', axis=1, inplace=True)
df1.drop('ACTUAL_OI', axis=1, inplace=True)
df1.drop('BUDGET_WAGES', axis=1, inplace=True)
df1.drop('ACTUAL_WAGES', axis=1, inplace=True)
df1.drop('BUDGET_SALES_EXP', axis=1, inplace=True)
df1.drop('ACTUAL_SALES_EXP', axis=1, inplace=True)
df1.drop('BUDGET_EMP_PROMO', axis=1, inplace=True)
df1.drop('ACTUAL_EMP_PROMO', axis=1, inplace=True)
df1.drop('BUDGET_TRAVEL', axis=1, inplace=True)
df1.drop('ACTUAL_TRAVEL', axis=1, inplace=True)
df1.drop('BUDGET_BAD_DEBT', axis=1, inplace=True)
df1.drop('ACTUAL_BAD_DEBT', axis=1, inplace=True)

df1.fillna(0)

df1['OFFICE#']= df1['OFFICE#'].astype(str)

df1['OFFICE#'] = df1['OFFICE#'].apply(lambda x: x.zfill(3))

df1.to_csv(BASEPATHEXT + '/wf_FINANCIAL/FACT_OFFICE_FINANCIAL_WF.csv', sep=',', encoding='windows-1252', index = False)

df1 = None

path = BASEPATH + '/sisenseincomestatement' 
df1 = appendcsvs(path)


df1['YEARMONTH'] = df1['YEARMONTH'].astype(str)
df1['YEARMONTH'] = df1['YEARMONTH'] +"01"
df1['YEARMONTH'] = pd.to_datetime(df1['YEARMONTH'])

df1.drop('Unnamed: 76', axis=1, inplace=True)

df1.columns = [c.replace(' ', '_') for c in df1.columns]

df1 = df1.replace(np.nan, 0)

df1['ACTUAL_SGNA'] = df1['ACTUAL_WAGES']+df1['ACTUAL_BONUSES']+df1['ACTUAL_COMMISSIONS']+df1['ACTUAL_PAYROLL_TAXES']+df1['ACTUAL_MARKETING_EXPENSE']+df1['ACTUAL_RECRUITMENT']+df1['ACTUAL_AUTO_EXPENSE']+df1['ACTUAL_CASUAL_LABOR']+df1['ACTUAL_COMPUTER_EXPENSE']+df1['ACTUAL_COPIER_AND_FAX']+df1['ACTUAL_DUES_AND_SUBS']+df1['ACTUAL_OFFICE_SUPPLIES']+df1['ACTUAL_POSTAGE_AND_DELIVERY']+df1['ACTUAL_SALES_EXPENSE']+df1['ACTUAL_TRAVEL_AND_ENTERTAINMENT']+df1['ACTUAL_TRAINING']+df1['ACTUAL_MISCELLANEOUS']+df1['ACTUAL_EMPLOYEE_PROMOTION']+df1['ACTUAL_401K_EXPENSE']+df1['ACTUAL_HEALTH_INSURANCE']+df1['ACTUAL_RENTS']+df1['ACTUAL_REPAIRS_AND_MAINTENANCE']+df1['ACTUAL_TELEPHONE']+df1['ACTUAL_UTILITIES']+df1['ACTUAL_PROFESSIONAL_FEES']+df1['ACTUAL_SAFETY_EXPENSE']+df1['ACTUAL_BAD_DEBTS']+df1['ACTUAL_BANK_FEES']+df1['ACTUAL_FEES/LICENSE/PERMITS']+df1['ACTUAL_DEPRECIATION']

df1['BUDGET_SGNA'] = df1['BUDGET_WAGES']+df1['BUDGET_BONUSES']+df1['BUDGET_COMMISSIONS']+df1['BUDGET_PAYROLL_TAXES']+df1['BUDGET_MARKETING_EXPENSE']+df1['BUDGET_RECRUITMENT']+df1['BUDGET_AUTO_EXPENSE']+df1['BUDGET_CASUAL_LABOR']+df1['BUDGET_COMPUTER_EXPENSE']+df1['BUDGET_COPIER_AND_FAX']+df1['BUDGET_DUES_AND_SUBS']+df1['BUDGET_OFFICE_SUPPLIES']+df1['BUDGET_POSTAGE_AND_DELIVERY']+df1['BUDGET_SALES_EXPENSE']+df1['BUDGET_TRAVEL_AND_ENTERTAINMENT']+df1['BUDGET_TRAINING']+df1['BUDGET_MISCELLANEOUS']+df1['BUDGET_EMPLOYEE_PROMOTION']+df1['BUDGET_401K_EXPENSE']+df1['BUDGET_HEALTH_INSURANCE']+df1['BUDGET_RENTS']+df1['BUDGET_REPAIRS_AND_MAINTENANCE']+df1['BUDGET_TELEPHONE']+df1['BUDGET_UTILITIES']+df1['BUDGET_PROFESSIONAL_FEES']+df1['BUDGET_SAFETY_EXPENSE']+df1['BUDGET_BAD_DEBTS']+df1['BUDGET_BANK_FEES']+df1['BUDGET_FEES/LICENSE/PERMITS']+df1['BUDGET_DEPRECIATION']



df2=pd.read_csv(BASEPATHEXT + '/wf_FINANCIAL/CLC ADDENDUM.csv',encoding='windows-1252')
df2['YEARMONTH'] = pd.to_datetime(df2['YEARMONTH'])


df3 = df1.append(df2, sort=False)

df1 = None
df2 = None


df3['OFFICE#'] = df3['OFFICE#'].astype(str)
df3['OFFICE#'] = df3['OFFICE#'].apply(lambda x: x.zfill(3))
df3['YEARMONTH'] = pd.to_datetime(df3['YEARMONTH'])
df3['YEARMONTH'] = df3['YEARMONTH'].astype(str)
df3['KEY'] = df3["OFFICE#"]+df3["YEARMONTH"]


df4 = pd.read_csv(BASEPATHEXT + '/wf_FINANCIAL/FACT_OFFICE_FINANCIAL_WF.csv',encoding='windows-1252')
df4['OFFICE#'] = df4['OFFICE#'].astype(str)
df4['OFFICE#'] = df4['OFFICE#'].apply(lambda x: x.zfill(3))
df4['YEARMONTH'] = pd.to_datetime(df4['YEARMONTH'])
df4['YEARMONTH'] = df4['YEARMONTH'].astype(str)
df4['KEY'] =  df4["OFFICE#"]+df4["YEARMONTH"]
df4.drop('OFFICE#', axis=1, inplace=True)
df4.drop('YEARMONTH', axis=1, inplace=True)

df5 = df3.merge(df4,on=['KEY'], how = "left")

df3 = None
df4 = None


df5.drop('KEY', axis=1, inplace=True)

df5.to_csv(BASEPATHEXT + '/FACT_COMPANY_FINANCIALS.csv', sep=',', encoding='utf-8', index = False)

df5 = None


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_INVOICE_ADJ")


#############################FACT_INVOICE_ADJ######################################################################################

path = BASEPATH + '/sisensearadj' 
df1 = appendcsvs(path)

df1['INVOICE DATE'] = pd.to_datetime(df1['INVOICE DATE'])
df1['ADJUSTMENT DATE'] = pd.to_datetime(df1['ADJUSTMENT DATE'])

df1.drop('Unnamed: 10', axis=1, inplace=True)
df1.drop('Unnamed: 11', axis=1, inplace=True)
df1.drop('445 INVOICE DATE YEARMONTH', axis=1, inplace=True)
df1.drop('445 ADJUSTMENT DATE YEARMONTH', axis=1, inplace=True)
df1.drop('ADJUSTMENT CODE DESC', axis=1, inplace=True)

df1.columns = ['ADJUSTMENT_AMOUNT','ADJUSTMENT_ID','ADJUSTMENT_CODE','ADJUSTMENT_DATE','CLIENT_ID','INVOICE_DATE','INVOICE_ID','OFFICE_ID']
df1['ADJUSTMENT_LAG_DAYS'] = ((df1['ADJUSTMENT_DATE'] - df1['INVOICE_DATE']).dt.days).astype(float)
df1['ADJUSTMENT_AMOUNT'] = df1['ADJUSTMENT_AMOUNT']

df18 = pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')
df18.columns = [c.replace(' ', '_') for c in df18.columns]
df18.columns = [c.replace('-', '_') for c in df18.columns]
df18 = df18[['CLIENT_ID','TERRITORY_REP_EMP_ID']]
df18.columns=['CLIENT_ID','SALES_REP_ID']
df18['SALES_REP_ID'] = df18['SALES_REP_ID'].map(lambda x: str(x)[:-2])
df18['CLIENT_ID'] = df18['CLIENT_ID'].str.upper()



df3 = df1.merge(df18,on=["CLIENT_ID"], how = "left")
df3 = df3[df3['OFFICE_ID'].notnull()]

df1 = None
df18 = None

df3.to_csv(BASEPATHEXT + '/FACT_INVOICE_ADJUSTMENTS.csv', sep=',', encoding='windows-1252', index = False)

df3 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ORDER")

#############################FACT_ORDER######################################################################################

df4 = pd.read_csv(BASEPATH + '/sisenseorder/order.csv',encoding='windows-1252')

df4 = df4.drop(df4[df4.ORDERED == 0].index)


df4.drop('STATUS', axis=1, inplace=True)
df4.drop('445 ORDER DATE', axis=1, inplace=True)
df4.drop('445 START DATE', axis=1, inplace=True)
df4.drop('445 ORDER CLOSED DATE', axis=1, inplace=True)
df4.drop('ORDER CLOSED REASON', axis=1, inplace=True)
df4.drop('PSG ORDER-TRAVELERS', axis=1, inplace=True)
df4.drop('Unnamed: 20', axis=1, inplace=True)
df4.drop('LAST TIMECARD DATE', axis=1, inplace=True)
df4.drop('ORDER LENGTH DAYS', axis=1, inplace=True)
df4['ORDER OFFICE#'] = df4['ORDER OFFICE#'].fillna(0).astype(int)
    
df4['ORDER DATE'] = pd.to_datetime(df4['ORDER DATE'])
df4['START DATE'] = pd.to_datetime(df4['START DATE'])
df4['ORDER CLOSED DATE'] = pd.to_datetime(df4['ORDER CLOSED DATE'])
df4['FIRST TIMECARD DATE'] = pd.to_datetime(df4['FIRST TIMECARD DATE'], errors='coerce')
df4 = df4[pd.to_numeric(df4['ORDER#'], errors='coerce').notnull()]
df4 = df4[pd.to_numeric(df4['ORDERED'], errors='coerce').notnull()]
df4 = df4[pd.to_numeric(df4['FILLED'], errors='coerce').notnull()]
df4 = df4[pd.to_numeric(df4['UNFILLED'], errors='coerce').notnull()]
df4.columns= ['ORDER_ID','CLIENT_ID','OFFICE_ID','ORDER_DATE', 'ORDER_START_DATE','TRADE_SPECIFIC_ID','ORDERED','FILLED','UNFILLED', 'REP_ID','ORDER_CLOSED_DATE','FIRST_TIMECARD_DATE']



df4['ORDER_START_LAG_DAYS'] = ((df4['ORDER_START_DATE'] - df4['ORDER_DATE']).dt.days).astype(float)
df4['ORDER_START_LAG_DAYS'] = df4['ORDER_START_LAG_DAYS'].fillna(0).astype(int)
df4['ORDER_DURATION_DAYS'] = ((df4['ORDER_CLOSED_DATE'] - df4['ORDER_START_DATE'] ).dt.days).astype(float)
df4['ORDER_DURATION_DAYS'] = df4['ORDER_DURATION_DAYS'].fillna(0).astype(int)




df4['LATE_ORDER'] = np.where(df4['ORDER_DATE'] >= df4['FIRST_TIMECARD_DATE'] ,1,0)
df4['UNFILLED_ORDER'] = np.where(df4['ORDERED'] == df4['UNFILLED'],1,0)
df4['CLIENT_ID'] = df4['CLIENT_ID'].str.upper()


path = BASEPATH +'/sisenseassignment' 
df5= appendcsvs(path)

df5 = df5[['ORDER#','ASSIGNMENT#','JOB#','EMP ID','DISPATCH DATE']]
df5['ASSIGNMENT#'] = df5['ASSIGNMENT#'].astype(str)
df5.sort_values(by=['ORDER#','ASSIGNMENT#']) 
df5 = df5.drop_duplicates(subset='ORDER#', keep='first', inplace=False)       
df5.columns = ['ORDER_ID','ASSIGNMENT_ID','FIRST_JOB_ID','FIRST_EMPLOYEE_ID','FIRST_DISPATCH_DATE']      
         


df5['ORDER_ID'] = df5['ORDER_ID'].astype(str)
df4['ORDER_ID'] = df4['ORDER_ID'].astype(str)

df4 = df4.merge(df5,on=["ORDER_ID"], how = "left")


df4['FIRST_TIMECARD_DATE'] = pd.to_datetime(df4['FIRST_TIMECARD_DATE'])
df4['FIRST_DISPATCH_DATE'] = pd.to_datetime(df4['FIRST_DISPATCH_DATE'])
df4['LATE_DISPATCH'] = np.where(df4['FIRST_DISPATCH_DATE'] >= df4['FIRST_TIMECARD_DATE'] ,1,0)
df4 = df4.drop_duplicates(subset='ORDER_ID', keep='first', inplace=False)

df4.to_csv(BASEPATHEXT + '/FACT_ORDER8.csv', sep=',', encoding='windows-1252', index = False)

df3 = None
df4 = None
df1 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_SECURITY")

#############################FACT_SECURITY######################################################################################

df4=pd.read_csv(BASEPATH + '/sisenseuseraccess//useraccess.csv',encoding='utf-8',error_bad_lines=False)
df4.drop('Unnamed: 3', axis=1, inplace=True)
df4.columns = [c.replace(' ', '_') for c in df4.columns]
df4['USER_ID'] = df4['USER_ID'].str.upper()

df4.to_csv(BASEPATHEXT + '/FACT_SECURITY.csv', sep=',', encoding='windows-1252', index = False)

df4 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_TIME_CARD_ADJ")

#############################FACT_TIME_CARD_ADJ######################################################################################

path =BASEPATH + '/sisensetimecard' 

df4 = appendcsvs(path)
df4 = df4.dropna(axis=0, subset=['ENTERED DATE'])



df4.drop('TRADE', axis=1, inplace=True)
df4.drop('PAY ADJ CODE', axis=1, inplace=True)
df4.drop('PAY ADJ QTY', axis=1, inplace=True)
df4.drop('PAY ADJ AMOUNT', axis=1, inplace=True)
df4.drop('Unnamed: 33', axis=1, inplace=True)
df4.drop('Unnamed: 34', axis=1, inplace=True)
#df4.drop('ELECTRONIC TIMECARD (Y)', axis=1, inplace=True)
df4.drop('ELECTRONIC TIMECARD DATE', axis=1, inplace=True)
df4.drop('ELECTRONIC TIMECARD TIME', axis=1, inplace=True)
df4['INVOICE DATE'] = pd.to_datetime(df4['INVOICE DATE'])
df4['TI WEEKEND DATE'] = pd.to_datetime(df4['TI WEEKEND DATE'])
df4['EMP ID'] = df4['EMP ID'].map(lambda x: str(x)[:-2])
df4['INVOICE#'] = df4['INVOICE#'].map(lambda x: str(x)[:-2])

    
df4.columns = ['BILL_ADJ_AMT','BILL_ADJ_CODE', 'BILL_ADJ_QTY','DT_HOURS_BILLED','OT_HOURS_BILLED','REG_HOURS_BILLED','TOTAL_HOURS_BILLED',
               'CLIENT_ID','TIME_CARD_TYPE', 'ETC_ID', 'ETC_ID2','EMPLOYEE_ID','ENTERED_USER_ID','ENTERED_DATE', 'ENTERED_TIME','INVOICE_DATE', 'INVOICE_ID',
               'DT_HOURS_PAY','OT_HOURS_PAY','REG_HOURS_PAY','TOTAL_HOURS_PAY','PAYROLL_PROCESS_DATE','POSITION_CODE', 'POSTED_USER' ,
               'POSTED_DATE','POSTED_TIME','WEEK_END_DATE','TIMECARD_ID',]
    
    



df4['CLIENT_ID'] = df4['CLIENT_ID'].str.upper()
df4['ENTERED_DATE'] = pd.to_datetime(df4['ENTERED_DATE'])
df4['TIME_CARD_TYPE'] = np.where(df4['TIME_CARD_TYPE'] == 'Y', 'ELECTRONIC', 'PAPER')
df4['DAYS_LATE'] =  ((df4['INVOICE_DATE'] - df4['WEEK_END_DATE']).dt.days).astype(float)
df4['DAYS_LATE_FLAG'] = np.where(df4['DAYS_LATE'] >= 7,'Y','N')
df4['BILL_ADJ_CODE'] = df4['BILL_ADJ_CODE'].fillna("None").astype(str)
df4['BILL_ADJ_QTY'] = df4['BILL_ADJ_QTY'].fillna(0).astype(str)
df4['BILL_ADJ_AMT'] = df4['BILL_ADJ_AMT'].fillna(0).astype(int)




df4.to_csv(BASEPATHEXT + '/FACT_TIME_CARD_ADJ2.csv', sep=',', encoding='utf-8', index = False)

df4 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_BUDGET_UNPIVOTED")

   
#############################FACT_BUDGET_UNPIVOTED######################################################################################

df5 = pd.read_csv(BASEPATHEXT + '/FACT_COMPANY_FINANCIALS.csv',encoding='windows-1252')

df5.drop('OFFICE#', axis=1, inplace=True)

df5 = df5.groupby(['YEARMONTH']).sum().reset_index()

badboylist = ['BUDGET_NET_SALES',
'BUDGET_COS_WAGES',
'BUDGET_COS_PR_TAXES_/_WC',
'BUDGET_COS_HEALTH_AND_OTHER',
'BUDGET_GROSS_PROFIT',
'BUDGET_WAGES',
'BUDGET_BONUSES',
'BUDGET_COMMISSIONS',
'BUDGET_PAYROLL_TAXES',
'BUDGET_MARKETING_EXPENSE',
'BUDGET_RECRUITMENT',
'BUDGET_AUTO_EXPENSE',
'BUDGET_CASUAL_LABOR',
'BUDGET_COMPUTER_EXPENSE',
'BUDGET_COPIER_AND_FAX',
'BUDGET_DUES_AND_SUBS',
'BUDGET_OFFICE_SUPPLIES',
'BUDGET_POSTAGE_AND_DELIVERY',
'BUDGET_SALES_EXPENSE',
'BUDGET_TRAVEL_AND_ENTERTAINMENT',
'BUDGET_TRAINING',
'BUDGET_MISCELLANEOUS',
'BUDGET_EMPLOYEE_PROMOTION',
'BUDGET_401K_EXPENSE',
'BUDGET_HEALTH_INSURANCE',
'BUDGET_RENTS',
'BUDGET_REPAIRS_AND_MAINTENANCE',
'BUDGET_TELEPHONE',
'BUDGET_UTILITIES',
'BUDGET_TAXES',
'BUDGET_PROFESSIONAL_FEES',
'BUDGET_SAFETY_EXPENSE',
'BUDGET_BAD_DEBTS',
'BUDGET_BANK_FEES',
'BUDGET_FEES/LICENSE/PERMITS',
'BUDGET_GENERAL_INSURANCE',
'BUDGET_DEPRECIATION',
'ACTUAL_NET_SALES',
'ACTUAL_COS_WAGES',
'ACTUAL_COS_PR_TAXES_/_WC',
'ACTUAL_COS_HEALTH_AND_OTHER',
'ACTUAL_GROSS_PROFIT',
'ACTUAL_WAGES',
'ACTUAL_BONUSES',
'ACTUAL_COMMISSIONS',
'ACTUAL_PAYROLL_TAXES',
'ACTUAL_MARKETING_EXPENSE',
'ACTUAL_RECRUITMENT',
'ACTUAL_AUTO_EXPENSE',
'ACTUAL_CASUAL_LABOR',
'ACTUAL_COMPUTER_EXPENSE',
'ACTUAL_COPIER_AND_FAX',
'ACTUAL_DUES_AND_SUBS',
'ACTUAL_OFFICE_SUPPLIES',
'ACTUAL_POSTAGE_AND_DELIVERY',
'ACTUAL_SALES_EXPENSE',
'ACTUAL_TRAVEL_AND_ENTERTAINMENT',
'ACTUAL_TRAINING',
'ACTUAL_MISCELLANEOUS',
'ACTUAL_EMPLOYEE_PROMOTION',
'ACTUAL_401K_EXPENSE',
'ACTUAL_HEALTH_INSURANCE',
'ACTUAL_RENTS',
'ACTUAL_REPAIRS_AND_MAINTENANCE',
'ACTUAL_TELEPHONE',
'ACTUAL_UTILITIES',
'ACTUAL_TAXES',
'ACTUAL_PROFESSIONAL_FEES',
'ACTUAL_SAFETY_EXPENSE',
'ACTUAL_BAD_DEBTS',
'ACTUAL_BANK_FEES',
'ACTUAL_FEES/LICENSE/PERMITS',
'ACTUAL_GENERAL_INSURANCE',
'ACTUAL_DEPRECIATION',
'ACTUAL_SGNA',
'BUDGET_SGNA',
'BUDGET_HOURS',
'ACTUAL_HOURS',
'BUDGET_REPS',
'ACTUAL_REPS',
'BUDGET_SALES_PER_REP',
'ACTUAL_SALES_PER_REP',
'BUDGET_EMPLOYEES',
'ACTUAL_EMPLOYEES',
'BUDGET_HOURS_PER_EMP',
'ACTUAL_HOURS_PER_EMP',
'BUDGET_USERS',
'ACTUAL_USERS',
'BUDGET_ACTIVE_CLIENTS',
'ACTUAL_ACTIVE_CLIENTS',
'BUDGET_REGS',
'ACTUAL_REGS',
'BUDGET_RESTARTS',
'ACTUAL_RESTARTS']

df6 =  pd.DataFrame([])
for l in badboylist:

    df = df5[['YEARMONTH',l]]
    df.columns = ['YEARMONTH', 'AMOUNT']
    df['ITEM'] = l
    df['LINE_ITEM'] = df['ITEM'].str[7:]
    df['TYPE'] = df['ITEM'].str[:6]
    df6 = df.append(df6)


df6.drop('ITEM', axis=1, inplace=True)

df7= df6[(df6.TYPE == 'ACTUAL')]
df7.drop('TYPE', axis=1, inplace=True)
df7.columns = ['YEARMONTH','ACTUAL','LINE_ITEM']


df8= df6[(df6.TYPE == 'BUDGET')]
df8.drop('TYPE', axis=1, inplace=True)
df8.columns = ['YEARMONTH','BUDGET','LINE_ITEM']

df9 = df7.merge(df8,on=["YEARMONTH","LINE_ITEM"], how = "left")

df9.to_csv(BASEPATHEXT + '/FACT_BUDGET_UNPIVOTED.csv', sep=',', encoding='utf-8', index = False)

df5 = None
df6 = None
df7 = None
df8 = None
df9 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_OFFICE_SUMMARY")


#############################FACT_OFFICE_SUMMARY######################################################################################

df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')

df1.drop('INVOICE_POSITION', axis=1, inplace=True)
df1.drop('TXN_TYPE', axis=1, inplace=True)
df1.drop('BILL_RATE', axis=1, inplace=True)
df1.drop('INVOICE_PAY_RATE', axis=1, inplace=True)
df1.drop('COST_OF_SALES', axis=1, inplace=True)
df1.drop('INVOICE_YEAR', axis=1, inplace=True)
df1.drop('INVOICE_MONTH', axis=1, inplace=True)
df1.drop('SUG_BILL_RATE', axis=1, inplace=True)
df1.drop('PAYRATE_BIN', axis=1, inplace=True)
df1.drop('PAYRATE_BIN_CRITICAL', axis=1, inplace=True)
df1.drop('BILL_GRAPH_FLAG', axis=1, inplace=True)
df1.drop('SUG_SALES', axis=1, inplace=True)
df1.drop('WC_COST', axis=1, inplace=True)
df1.drop('PYL_TAX_HLTH_OTHER', axis=1, inplace=True)
df1.drop('TXN_DESC', axis=1, inplace=True)
df1.drop('OVERTIME_FLAG', axis=1, inplace=True)
df1.drop('SPLIT_INVOICE', axis=1, inplace=True)
df1['OFFICE_ID']= df1['OFFICE_ID'].astype(str)
df1['OFFICE_ID'] = df1['OFFICE_ID'].apply(lambda x: x.zfill(3))

dfd=pd.read_csv(BASEPATHEXT + '/DIM_DATE_4.csv',encoding='windows_1252')
dfd = dfd[['DATE','445_MONTH_NUM','445_YEAR_NAME']]
dfd.columns = ['INVOICE_DATE','445_MONTH_NUM','445_YEAR_NAME']
dfd['INVOICE_DATE'] = pd.to_datetime(dfd['INVOICE_DATE'])
dfd['INVOICE_DATE'] = dfd['INVOICE_DATE'].astype(str)


dfn = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows_1252')
dfn = dfn[['CLIENT_ID','CLIENT_FIRST_SALE_DATE']]

df1 = df1.merge(dfn,on = ["CLIENT_ID"], how = "left")
df1['NEW_USERS'] = np.where(np.logical_and(df1['CLIENT_FIRST_SALE_DATE'] == df1['INVOICE_DATE'],df1['INVOICE_HOURS'] > 0),1,0)


df2 = df1.merge(dfd,on=["INVOICE_DATE"], how = "left")

df1 = None

df3 = df2.groupby(by = ['OFFICE_ID','445_MONTH_NUM','445_YEAR_NAME']).agg({
     'EMPLOYEE_ID' : 'nunique','CLIENT_ID' : 'nunique','INVOICE_HOURS' : 'sum','SALES' : 'sum','INVOICE_WAGES' : 'sum',
     'INVOICE_GP' : 'sum','INVOICE_ID' : 'nunique', 'ASSIGNMENT_ID' : 'nunique' , 'NEW_USERS' : 'sum'}).reset_index()

df2 = None

df3.columns = ['OFFICE_ID','445_MONTH_NUM','445_YEAR_NAME','WORKING_EMPS','USERS','HOURS','SALES','WAGES',
               'GP','INVOICES','ASSIGNMENTS', 'NEW_USERS']



df3['OFFICE_ID'] = df3['OFFICE_ID'].apply(lambda x: x.zfill(3))

df15=pd.read_csv(BASEPATHEXT + '/FACT_COMPANY_FINANCIALS.csv',encoding='windows_1252')
df15= df15[['OFFICE#','YEARMONTH','BUDGET_HOURS','BUDGET_NET_SALES','BUDGET_COS_WAGES','BUDGET_GROSS_PROFIT','BUDGET_EMPLOYEES']]

df15['445_YEAR_NAME'] =  df15['YEARMONTH'].str[:4].astype(float)
df15['PART'] =  df15['YEARMONTH'].str[:7].astype(str)
df15['445_MONTH_NUM'] = df15['PART'].str[-2:].astype(float)
df15.drop('YEARMONTH', axis=1, inplace=True)
df15.drop('PART', axis=1, inplace=True)
df15['OFFICE#'] =  df15['OFFICE#'].astype(str)
df15['OFFICE#'] = df15['OFFICE#'].apply(lambda x: x.zfill(3))
df15.columns = ['OFFICE_ID','BUDGET_HOURS','BUDGET_NET_SALES','BUDGET_COS_WAGES','BUDGET_GROSS_PROFIT','BUDGET_EMPLOYEES','445_YEAR_NAME','445_MONTH_NUM']



df16 = df3.merge(df15,on=["OFFICE_ID","445_YEAR_NAME","445_MONTH_NUM"], how = "left")

df3 = None
df15 = None

df16['DATE'] = df16['445_MONTH_NUM'].map(lambda x: str(x)[:-2]) + "/15/" + df16['445_YEAR_NAME'].map(lambda x: str(x)[:-2])


df16.to_csv(BASEPATHEXT + '/FACT_UNIVERSAL_SUMMARY3.csv', sep=',', encoding='utf-8', index = False)




df16 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_CLIENT")

######################################FACT_CLIENT######################################################################################

df4=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')

df4.drop('ADDR1', axis=1, inplace=True)
df4.drop('ADDR2', axis=1, inplace=True)
df4.drop('CITY', axis=1, inplace=True)
df4.drop('STATE', axis=1, inplace=True)
df4.drop('REGISTRATION REP ID', axis=1, inplace=True)
df4.drop('Unnamed: 20', axis=1, inplace=True)
df4.drop('LAST SALE', axis=1, inplace=True)
df4.drop('445 REGISTRATION DATE', axis=1, inplace=True)
df4.drop('CLIENT NAME', axis=1, inplace=True)
df4.drop('ZIP', axis=1, inplace=True)
df4.drop('TERRITORY', axis=1, inplace=True)
df4.drop('TSI', axis=1, inplace=True)
df4.drop('CLC LEGACY', axis=1, inplace=True)
df4.drop('TI-CLC MUTUAL', axis=1, inplace=True)

df4['CLIENT ID'] = df4['CLIENT ID'].astype(str)
df4['CLIENT ID'] = df4['CLIENT ID'].str.upper()
df4['HOME OFFICE#'] = df4['HOME OFFICE#'] .map(lambda x: str(x)[:-2])
df4['TERRITORY REP EMP ID'] = df4['TERRITORY REP EMP ID'] .map(lambda x: str(x)[:-2])
df4.columns = ['CLIENT_ID', 'OFFICE_ID','SALES_REP_ID','CLIENT_REG_DATE','STATUS','CLIENT_LAST_SALE_DATE','CLIENT_FIRST_SALE_DATE']
df4.head()
df4['CLIENT_REG_DATE'] = pd.to_datetime(df4['CLIENT_REG_DATE'])
df4['CLIENT_LAST_SALE_DATE'] = pd.to_datetime(df4['CLIENT_LAST_SALE_DATE'])
df4['CLIENT_FIRST_SALE_DATE'] = pd.to_datetime(df4['CLIENT_FIRST_SALE_DATE'])
df4['CLIENT_REG_DATE'] = np.where(df4['CLIENT_FIRST_SALE_DATE'] < df4['CLIENT_REG_DATE'] , df4['CLIENT_FIRST_SALE_DATE'], df4['CLIENT_REG_DATE'] )
df4["HOUSE_ACCOUNT"] = np.where(df4['SALES_REP_ID'] == 'n',1,0)
df4["NO_SALES"] = df4.CLIENT_LAST_SALE_DATE.isnull().map({True: 1, False: 0})
df4["REP_ACCOUNT"] = np.where(df4["HOUSE_ACCOUNT"] == 1 , 0, 1)
df4["ACTIVE_REP_ACCOUNT"] = np.where(np.logical_and(df4["REP_ACCOUNT"] == 1 , df4["STATUS"] == "ACTIVE"),1,0)
df4["ACTIVE_HOUSE_ACCOUNT"] = np.where(np.logical_and(df4["HOUSE_ACCOUNT"] == 1 , df4["STATUS"] == "ACTIVE"),1,0)
df4["NEW_ACCOUNT_NEVER_USED"] = np.where(np.logical_and(df4["NO_SALES"] == 1 , df4["STATUS"] == "NEW"),1,0)
df4["INACTIVE_REVOKED_ACCOUNTS"] = np.where(np.logical_or(df4["STATUS"] == "REVOKED" , df4["STATUS"] == "INACTIVE"),1,0)
df4.drop('STATUS', axis=1, inplace=True)

df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')


df1.drop('INVOICE_POSITION', axis=1, inplace=True)
df1.drop('TXN_TYPE', axis=1, inplace=True)
df1.drop('INVOICE_YEAR', axis=1, inplace=True)
df1.drop('INVOICE_MONTH', axis=1, inplace=True)
df1.drop('SUG_BILL_RATE', axis=1, inplace=True)
df1.drop('PAYRATE_BIN', axis=1, inplace=True)
df1.drop('PAYRATE_BIN_CRITICAL', axis=1, inplace=True)
df1.drop('BILL_GRAPH_FLAG', axis=1, inplace=True)
df1.drop('SUG_SALES', axis=1, inplace=True)
df1.drop('WC_COST', axis=1, inplace=True)
df1.drop('PYL_TAX_HLTH_OTHER', axis=1, inplace=True)
df1.drop('TXN_DESC', axis=1, inplace=True)
df1.drop('OVERTIME_FLAG', axis=1, inplace=True)
df1.drop('SPLIT_INVOICE', axis=1, inplace=True)
df1.drop('TERRITORY_REP_ID', axis=1, inplace=True)
df1.drop('INVOICE_DATE', axis=1, inplace=True)
df1.drop('OFFICE_ID', axis=1, inplace=True)
df1.drop('EMPLOYEE_ID', axis=1, inplace=True)

df2 = df1.groupby(by = ['CLIENT_ID']).agg({
     'INVOICE_HOURS' : 'sum', 'BILL_RATE' : 'mean','SALES' : 'sum','INVOICE_PAY_RATE' : 'mean','INVOICE_WAGES' : 'sum',
     'COST_OF_SALES' : 'sum', 'INVOICE_GP' : 'sum','INVOICE_ID' : 'nunique','ORDER_ID' : 'nunique', 'JOB_ID' : 'nunique','ASSIGNMENT_ID' : 'nunique',
     'DISPATCH_ID' : 'nunique'}).reset_index()

df2.columns = ['CLIENT_ID','HOURS','AVG_BILL_RATE','SALES','AVG_WAGE_RATE','WAGES','BURDEN','GP','INVOICES','ORDERS','JOBS','ASSIGNMENTS','DISPATCHES']

df1 = None


df5 = df4.merge(df2,on=["CLIENT_ID"], how = "left")
df4 = None
df2 = None

df5['MARGIN'] = df5['GP'] / df5['SALES'] 
df5['GP_DOLLAR'] = df5['GP'] / df5['HOURS'] 
df5['QUADRANT'] = np.where(np.logical_and(df5['MARGIN'] > 0.28 , df5['GP_DOLLAR'] > 10.50) , 1 ,
                          np.where(np.logical_and(df5['MARGIN'] > 0.28 , df5['GP_DOLLAR'] < 10.50) , 2 ,
                          np.where(np.logical_and(df5['MARGIN'] < 0.28 , df5['GP_DOLLAR'] > 10.50) , 3 ,
                          np.where(np.logical_and(df5['MARGIN'] < 0.28 , df5['GP_DOLLAR'] < 10.50) , 4 , 0))))
df5.drop('GP_DOLLAR', axis=1, inplace=True)
df5.drop('MARGIN', axis=1, inplace=True)
df5 = df5.fillna(0)


df700 = df5[['CLIENT_ID','QUADRANT']]

df7=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv', encoding='utf-8')

df10 = df7.merge(df700,on=["CLIENT_ID"], how = "left")
df7 = None
df6 = None

df10.to_csv(BASEPATHEXT +'/DIM_CLIENT.csv', sep=',', encoding='utf-8', index = False)


df5 = df5[df5.CLIENT_REG_DATE != 0]
df5 = df5[df5.CLIENT_REG_DATE >= pd.to_datetime('1/4/2016')]
df5['DAYS_TO_FIRST_SALE'] = ((pd.to_datetime(df5['CLIENT_FIRST_SALE_DATE'],errors = 'coerce') - pd.to_datetime(df5['CLIENT_REG_DATE'],errors = 'coerce')).dt.days).astype(float)
df5['DAYS_SINCE_LAST_SALE'] = ((pd.to_datetime(date.today()) - pd.to_datetime(df5['CLIENT_LAST_SALE_DATE'],errors = 'coerce')).dt.days).astype(float)
df5['ABOUT_TO_GO_INACTIVE'] = np.where(df5['DAYS_SINCE_LAST_SALE'].between(150, 180), 1,0)
df5['RECENTLY_TURNED_INACTIVE'] = np.where(df5['DAYS_SINCE_LAST_SALE'].between(181, 365), 1,0)



df30 = df5[['CLIENT_ID', 'CLIENT_FIRST_SALE_DATE']]
df30['CLIENT_FIRST_SALE_DATE'] = pd.to_datetime(df30['CLIENT_FIRST_SALE_DATE'] , errors = 'coerce')


df70=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df70['INVOICE_DATE'] = pd.to_datetime(df70['INVOICE_DATE'] , errors = 'coerce')
df70 = df70.groupby(by = ['CLIENT_ID' , 'ORDER_ID']).agg({'INVOICE_DATE' : 'min'}).reset_index()
df70.columns = ['CLIENT_ID','ORDER_ID', 'CLIENT_FIRST_SALE_DATE']

df90 = df30.merge(df70,on=["CLIENT_ID","CLIENT_FIRST_SALE_DATE"], how = "left")
df90.drop('CLIENT_FIRST_SALE_DATE', axis=1, inplace=True)

df60 = pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows_1252')
df60 = df60[['ORDER_ID','CLIENT_ID','ORDERED','FILLED']]

df100 = df90.merge(df60,on=["CLIENT_ID","ORDER_ID"], how = "left")

df50=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df50 = df50.groupby(by = ['ORDER_ID']).agg({'SALES' : 'sum','INVOICE_HOURS' : 'sum', 'INVOICE_WAGES' : 'sum' , 'INVOICE_GP' : 'sum'}).reset_index()

df110 = df100.merge(df50,on=["ORDER_ID"], how = "inner")
df110.columns = ['CLIENT_ID','FIRST_ORDER_ID','FIRST_MEN_ORDERD','FIRST_MEN_FILLED','FIRST_SALES','FIRST_HOURS','FIRST_WAGES','FIRST_GP']

df5 = df5.merge(df110,on=["CLIENT_ID"], how = "left")

df6 = df5[['CLIENT_ID','ABOUT_TO_GO_INACTIVE','RECENTLY_TURNED_INACTIVE']]


df5.drop('ABOUT_TO_GO_INACTIVE', axis=1, inplace=True)
df5.drop('RECENTLY_TURNED_INACTIVE', axis=1, inplace=True)

df5.to_csv(BASEPATHEXT + '/FACT_CLIENT.csv', sep=',', encoding='utf-8', index = False)


df7=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv', encoding='utf-8')

df10 = df7.merge(df6,on=["CLIENT_ID"], how = "left")
df10 = df10.drop_duplicates(subset='CLIENT_ID', keep='first', inplace=False)

df7 = None
df6 = None

df10.to_csv(BASEPATHEXT +'/DIM_CLIENT.csv', sep=',', encoding='utf-8', index = False)




df5 = None


df10 = None

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ABC_ENGAGEMENT")

#####################################FACT_ABC_ENGAGEMENT###########################################################################################

df8 = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')

df8 = pd.pivot_table(df8, values = 'ABC_MEMBER', index=['OFFICE_ID'], columns = 'CLIENT_STATUS', aggfunc = "sum").reset_index()

df8['OFFICE_ID'] = df8['OFFICE_ID'].astype(str)

df21 = df8.groupby(by = ['OFFICE_ID']).agg({
     'ACTIVE' : 'sum', 'CREDIT HOLD' : 'sum','INACTIVE' : 'sum','NEW' : 'sum','OUT OF BUSINESS' : 'sum','REVOKED' : 'sum'}).reset_index()

df21['ABC_ACTIVE'] = df21['ACTIVE'] + df21['NEW']
df21['ABC_INACTIVE'] = df21['INACTIVE'] + df21['REVOKED']
df21['ABC_TOTAL'] = df21['ABC_ACTIVE'] + df21['ABC_INACTIVE']
df21.drop('ACTIVE', axis=1, inplace=True)
df21.drop('CREDIT HOLD', axis=1, inplace=True)
df21.drop('INACTIVE', axis=1, inplace=True)
df21.drop('NEW', axis=1, inplace=True)
df21.drop('OUT OF BUSINESS', axis=1, inplace=True)
df21.drop('REVOKED', axis=1, inplace=True)

df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')

df1.drop('TXN_TYPE', axis=1, inplace=True)
df1.drop('INVOICE_YEAR', axis=1, inplace=True)
df1.drop('INVOICE_MONTH', axis=1, inplace=True)
df1.drop('SUG_BILL_RATE', axis=1, inplace=True)
df1.drop('PAYRATE_BIN', axis=1, inplace=True)
df1.drop('PAYRATE_BIN_CRITICAL', axis=1, inplace=True)
df1.drop('BILL_GRAPH_FLAG', axis=1, inplace=True)
df1.drop('SUG_SALES', axis=1, inplace=True)
df1.drop('WC_COST', axis=1, inplace=True)
df1.drop('PYL_TAX_HLTH_OTHER', axis=1, inplace=True)
df1.drop('TXN_DESC', axis=1, inplace=True)
df1.drop('OVERTIME_FLAG', axis=1, inplace=True)
df1.drop('SPLIT_INVOICE', axis=1, inplace=True)
df1.drop('TERRITORY_REP_ID', axis=1, inplace=True)
df1.drop('EMPLOYEE_ID', axis=1, inplace=True)

df10=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')
df10 = df10[['CLIENT_ID','ABC_MEMBER',]]

df11 = df1.merge(df10, on = 'CLIENT_ID' , how = 'left')
df11 = df11[(df11.ABC_MEMBER == 1)]
df11.drop('INVOICE_POSITION', axis=1, inplace=True)


df12 = df11.groupby(by = ['OFFICE_ID','INVOICE_DATE']).agg({
     'INVOICE_HOURS' : 'sum', 'BILL_RATE' : 'mean','SALES' : 'sum','INVOICE_PAY_RATE' : 'mean','INVOICE_WAGES' : 'sum',
     'COST_OF_SALES' : 'sum', 'INVOICE_GP' : 'sum','INVOICE_ID' : 'nunique','ORDER_ID' : 'nunique', 'JOB_ID' : 'nunique','ASSIGNMENT_ID' : 'nunique',
     'CLIENT_ID' : 'nunique','DISPATCH_ID' : 'nunique'}).reset_index()

df12.columns = ['OFFICE_ID','INVOICE_DATE','ABC_HOURS','ABC_AVG_BILL_RATE','ABC_SALES','ABC_AVG_WAGE_RATE','ABC_WAGES','ABC_BURDEN','ABC_GP','ABC_INVOICES','ABC_ORDERS','ABC_JOBS','ABC_ASSIGNMENTS', 'ABC_USERS','ABC_DISPATCHES']

df13 = df1.merge(df10, on = 'CLIENT_ID' , how = 'left')
df13 = df13[(df13.ABC_MEMBER == 0)]
df13.drop('INVOICE_POSITION', axis=1, inplace=True)

df14 = df13.groupby(by = ['OFFICE_ID','INVOICE_DATE']).agg({
     'INVOICE_HOURS' : 'sum', 'BILL_RATE' : 'mean','SALES' : 'sum','INVOICE_PAY_RATE' : 'mean','INVOICE_WAGES' : 'sum',
     'COST_OF_SALES' : 'sum', 'INVOICE_GP' : 'sum','INVOICE_ID' : 'nunique','ORDER_ID' : 'nunique', 'JOB_ID' : 'nunique','ASSIGNMENT_ID' : 'nunique',
     'CLIENT_ID' : 'nunique','DISPATCH_ID' : 'nunique'}).reset_index()

df14.columns = ['OFFICE_ID','INVOICE_DATE','NON_ABC_HOURS','NON_ABC_AVG_BILL_RATE','NON_ABC_SALES','NON_ABC_AVG_WAGE_RATE','NON_ABC_WAGES','NON_ABC_BURDEN','NON_ABC_GP','NON_ABC_INVOICES','NON_ABC_ORDERS','NON_ABC_JOBS','NON_ABC_ASSIGNMENTS', 'NON_ABC_USERS', 'NON_ABC_DISPATCHES']

df15 = df12.merge(df14,on=["OFFICE_ID","INVOICE_DATE"], how = "left")
df15['OFFICE_ID'] = df15['OFFICE_ID'].astype(str)

df16 = df15.merge(df21,on=["OFFICE_ID"])

df16.to_csv(BASEPATHEXT + '/FACT_ABC_ENGAGEMENT.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_TIME_SERIES")

#####################################FACT_TIME_SERIES###########################################################################################

df5 = pd.read_csv(BASEPATHEXT + '/FACT_COMPANY_FINANCIALS.csv',index_col='YEARMONTH',parse_dates=True)
df5.drop('OFFICE#', axis=1, inplace=True)
df5 = df5.groupby(['YEARMONTH']).sum().reset_index()
df5 = df5.set_index(pd.DatetimeIndex(df5['YEARMONTH']))
df5.drop('YEARMONTH', axis=1, inplace=True)

df6 = df5.rolling(3).mean()

list3 = ['BUDGET_NET_SALES 3MO MA',
'BUDGET_COS_WAGES 3MO MA',
'BUDGET_COS_PR_TAXES_/_WC 3MO MA',
'BUDGET_COS_HEALTH_AND_OTHER 3MO MA',
'BUDGET_GROSS_PROFIT 3MO MA',
'BUDGET_WAGES 3MO MA',
'BUDGET_BONUSES 3MO MA',
'BUDGET_COMMISSIONS 3MO MA',
'BUDGET_PAYROLL_TAXES 3MO MA',
'BUDGET_MARKETING_EXPENSE 3MO MA',
'BUDGET_RECRUITMENT 3MO MA',
'BUDGET_AUTO_EXPENSE 3MO MA',
'BUDGET_CASUAL_LABOR 3MO MA',
'BUDGET_COMPUTER_EXPENSE 3MO MA',
'BUDGET_COPIER_AND_FAX 3MO MA',
'BUDGET_DUES_AND_SUBS 3MO MA',
'BUDGET_OFFICE_SUPPLIES 3MO MA',
'BUDGET_POSTAGE_AND_DELIVERY 3MO MA',
'BUDGET_SALES_EXPENSE 3MO MA',
'BUDGET_TRAVEL_AND_ENTERTAINMENT 3MO MA',
'BUDGET_TRAINING 3MO MA',
'BUDGET_MISCELLANEOUS 3MO MA',
'BUDGET_EMPLOYEE_PROMOTION 3MO MA',
'BUDGET_401K_EXPENSE 3MO MA',
'BUDGET_HEALTH_INSURANCE 3MO MA',
'BUDGET_RENTS 3MO MA',
'BUDGET_REPAIRS_AND_MAINTENANCE 3MO MA',
'BUDGET_TELEPHONE 3MO MA',
'BUDGET_UTILITIES 3MO MA',
'BUDGET_TAXES 3MO MA',
'BUDGET_PROFESSIONAL_FEES 3MO MA',
'BUDGET_SAFETY_EXPENSE 3MO MA',
'BUDGET_BAD_DEBTS 3MO MA',
'BUDGET_BANK_FEES 3MO MA',
'BUDGET_FEES/LICENSE/PERMITS 3MO MA',
'BUDGET_GENERAL_INSURANCE 3MO MA',
'BUDGET_DEPRECIATION 3MO MA',
'ACTUAL_NET_SALES 3MO MA',
'ACTUAL_COS_WAGES 3MO MA',
'ACTUAL_COS_PR_TAXES_/_WC 3MO MA',
'ACTUAL_COS_HEALTH_AND_OTHER 3MO MA',
'ACTUAL_GROSS_PROFIT 3MO MA',
'ACTUAL_WAGES 3MO MA',
'ACTUAL_BONUSES 3MO MA',
'ACTUAL_COMMISSIONS 3MO MA',
'ACTUAL_PAYROLL_TAXES 3MO MA',
'ACTUAL_MARKETING_EXPENSE 3MO MA',
'ACTUAL_RECRUITMENT 3MO MA',
'ACTUAL_AUTO_EXPENSE 3MO MA',
'ACTUAL_CASUAL_LABOR 3MO MA',
'ACTUAL_COMPUTER_EXPENSE 3MO MA',
'ACTUAL_COPIER_AND_FAX 3MO MA',
'ACTUAL_DUES_AND_SUBS 3MO MA',
'ACTUAL_OFFICE_SUPPLIES 3MO MA',
'ACTUAL_POSTAGE_AND_DELIVERY 3MO MA',
'ACTUAL_SALES_EXPENSE 3MO MA',
'ACTUAL_TRAVEL_AND_ENTERTAINMENT 3MO MA',
'ACTUAL_TRAINING 3MO MA',
'ACTUAL_MISCELLANEOUS 3MO MA',
'ACTUAL_EMPLOYEE_PROMOTION 3MO MA',
'ACTUAL_401K_EXPENSE 3MO MA',
'ACTUAL_HEALTH_INSURANCE 3MO MA',
'ACTUAL_RENTS 3MO MA',
'ACTUAL_REPAIRS_AND_MAINTENANCE 3MO MA',
'ACTUAL_TELEPHONE 3MO MA',
'ACTUAL_UTILITIES 3MO MA',
'ACTUAL_TAXES 3MO MA',
'ACTUAL_PROFESSIONAL_FEES 3MO MA',
'ACTUAL_SAFETY_EXPENSE 3MO MA',
'ACTUAL_BAD_DEBTS 3MO MA',
'ACTUAL_BANK_FEES 3MO MA',
'ACTUAL_FEES/LICENSE/PERMITS 3MO MA',
'ACTUAL_GENERAL_INSURANCE 3MO MA',
'ACTUAL_DEPRECIATION 3MO MA',
'ACTUAL_SGNA 3MO MA',
'BUDGET_SGNA 3MO MA',
'BUDGET_HOURS 3MO MA',
'ACTUAL_HOURS 3MO MA',
'BUDGET_REPS 3MO MA',
'ACTUAL_REPS 3MO MA',
'BUDGET_SALES_PER_REP 3MO MA',
'ACTUAL_SALES_PER_REP 3MO MA',
'BUDGET_EMPLOYEES 3MO MA',
'ACTUAL_EMPLOYEES 3MO MA',
'BUDGET_HOURS_PER_EMP 3MO MA',
'ACTUAL_HOURS_PER_EMP 3MO MA',
'BUDGET_USERS 3MO MA',
'ACTUAL_USERS 3MO MA',
'BUDGET_ACTIVE_CLIENTS 3MO MA',
'ACTUAL_ACTIVE_CLIENTS 3MO MA',
'BUDGET_REGS 3MO MA',
'ACTUAL_REGS 3MO MA',
'BUDGET_RESTARTS 3MO MA',
'ACTUAL_RESTARTS 3MO MA']

df6.columns = list3

df7 = df5.rolling(6).mean()
list6 = ['BUDGET_NET_SALES 6MO MA',
'BUDGET_COS_WAGES 6MO MA',
'BUDGET_COS_PR_TAXES_/_WC 6MO MA',
'BUDGET_COS_HEALTH_AND_OTHER 6MO MA',
'BUDGET_GROSS_PROFIT 6MO MA',
'BUDGET_WAGES 6MO MA',
'BUDGET_BONUSES 6MO MA',
'BUDGET_COMMISSIONS 6MO MA',
'BUDGET_PAYROLL_TAXES 6MO MA',
'BUDGET_MARKETING_EXPENSE 6MO MA',
'BUDGET_RECRUITMENT 6MO MA',
'BUDGET_AUTO_EXPENSE 6MO MA',
'BUDGET_CASUAL_LABOR 6MO MA',
'BUDGET_COMPUTER_EXPENSE 6MO MA',
'BUDGET_COPIER_AND_FAX 6MO MA',
'BUDGET_DUES_AND_SUBS 6MO MA',
'BUDGET_OFFICE_SUPPLIES 6MO MA',
'BUDGET_POSTAGE_AND_DELIVERY 6MO MA',
'BUDGET_SALES_EXPENSE 6MO MA',
'BUDGET_TRAVEL_AND_ENTERTAINMENT 6MO MA',
'BUDGET_TRAINING 6MO MA',
'BUDGET_MISCELLANEOUS 6MO MA',
'BUDGET_EMPLOYEE_PROMOTION 6MO MA',
'BUDGET_401K_EXPENSE 6MO MA',
'BUDGET_HEALTH_INSURANCE 6MO MA',
'BUDGET_RENTS 6MO MA',
'BUDGET_REPAIRS_AND_MAINTENANCE 6MO MA',
'BUDGET_TELEPHONE 6MO MA',
'BUDGET_UTILITIES 6MO MA',
'BUDGET_TAXES 6MO MA',
'BUDGET_PROFESSIONAL_FEES 6MO MA',
'BUDGET_SAFETY_EXPENSE 6MO MA',
'BUDGET_BAD_DEBTS 6MO MA',
'BUDGET_BANK_FEES 6MO MA',
'BUDGET_FEES/LICENSE/PERMITS 6MO MA',
'BUDGET_GENERAL_INSURANCE 6MO MA',
'BUDGET_DEPRECIATION 6MO MA',
'ACTUAL_NET_SALES 6MO MA',
'ACTUAL_COS_WAGES 6MO MA',
'ACTUAL_COS_PR_TAXES_/_WC 6MO MA',
'ACTUAL_COS_HEALTH_AND_OTHER 6MO MA',
'ACTUAL_GROSS_PROFIT 6MO MA',
'ACTUAL_WAGES 6MO MA',
'ACTUAL_BONUSES 6MO MA',
'ACTUAL_COMMISSIONS 6MO MA',
'ACTUAL_PAYROLL_TAXES 6MO MA',
'ACTUAL_MARKETING_EXPENSE 6MO MA',
'ACTUAL_RECRUITMENT 6MO MA',
'ACTUAL_AUTO_EXPENSE 6MO MA',
'ACTUAL_CASUAL_LABOR 6MO MA',
'ACTUAL_COMPUTER_EXPENSE 6MO MA',
'ACTUAL_COPIER_AND_FAX 6MO MA',
'ACTUAL_DUES_AND_SUBS 6MO MA',
'ACTUAL_OFFICE_SUPPLIES 6MO MA',
'ACTUAL_POSTAGE_AND_DELIVERY 6MO MA',
'ACTUAL_SALES_EXPENSE 6MO MA',
'ACTUAL_TRAVEL_AND_ENTERTAINMENT 6MO MA',
'ACTUAL_TRAINING 6MO MA',
'ACTUAL_MISCELLANEOUS 6MO MA',
'ACTUAL_EMPLOYEE_PROMOTION 6MO MA',
'ACTUAL_401K_EXPENSE 6MO MA',
'ACTUAL_HEALTH_INSURANCE 6MO MA',
'ACTUAL_RENTS 6MO MA',
'ACTUAL_REPAIRS_AND_MAINTENANCE 6MO MA',
'ACTUAL_TELEPHONE 6MO MA',
'ACTUAL_UTILITIES 6MO MA',
'ACTUAL_TAXES 6MO MA',
'ACTUAL_PROFESSIONAL_FEES 6MO MA',
'ACTUAL_SAFETY_EXPENSE 6MO MA',
'ACTUAL_BAD_DEBTS 6MO MA',
'ACTUAL_BANK_FEES 6MO MA',
'ACTUAL_FEES/LICENSE/PERMITS 6MO MA',
'ACTUAL_GENERAL_INSURANCE 6MO MA',
'ACTUAL_DEPRECIATION 6MO MA',
'ACTUAL_SGNA 6MO MA',
'BUDGET_SGNA 6MO MA',
'BUDGET_HOURS 6MO MA',
'ACTUAL_HOURS 6MO MA',
'BUDGET_REPS 6MO MA',
'ACTUAL_REPS 6MO MA',
'BUDGET_SALES_PER_REP 6MO MA',
'ACTUAL_SALES_PER_REP 6MO MA',
'BUDGET_EMPLOYEES 6MO MA',
'ACTUAL_EMPLOYEES 6MO MA',
'BUDGET_HOURS_PER_EMP 6MO MA',
'ACTUAL_HOURS_PER_EMP 6MO MA',
'BUDGET_USERS 6MO MA',
'ACTUAL_USERS 6MO MA',
'BUDGET_ACTIVE_CLIENTS 6MO MA',
'ACTUAL_ACTIVE_CLIENTS 6MO MA',
'BUDGET_REGS 6MO MA',
'ACTUAL_REGS 6MO MA',
'BUDGET_RESTARTS 6MO MA',
'ACTUAL_RESTARTS 6MO MA']

df7.columns = list6

df8 = df5.rolling(12).mean()
list12 = ['BUDGET_NET_SALES 12MO MA',
'BUDGET_COS_WAGES 12MO MA',
'BUDGET_COS_PR_TAXES_/_WC 12MO MA',
'BUDGET_COS_HEALTH_AND_OTHER 12MO MA',
'BUDGET_GROSS_PROFIT 12MO MA',
'BUDGET_WAGES 12MO MA',
'BUDGET_BONUSES 12MO MA',
'BUDGET_COMMISSIONS 12MO MA',
'BUDGET_PAYROLL_TAXES 12MO MA',
'BUDGET_MARKETING_EXPENSE 12MO MA',
'BUDGET_RECRUITMENT 12MO MA',
'BUDGET_AUTO_EXPENSE 12MO MA',
'BUDGET_CASUAL_LABOR 12MO MA',
'BUDGET_COMPUTER_EXPENSE 12MO MA',
'BUDGET_COPIER_AND_FAX 12MO MA',
'BUDGET_DUES_AND_SUBS 12MO MA',
'BUDGET_OFFICE_SUPPLIES 12MO MA',
'BUDGET_POSTAGE_AND_DELIVERY 12MO MA',
'BUDGET_SALES_EXPENSE 12MO MA',
'BUDGET_TRAVEL_AND_ENTERTAINMENT 12MO MA',
'BUDGET_TRAINING 12MO MA',
'BUDGET_MISCELLANEOUS 12MO MA',
'BUDGET_EMPLOYEE_PROMOTION 12MO MA',
'BUDGET_401K_EXPENSE 12MO MA',
'BUDGET_HEALTH_INSURANCE 12MO MA',
'BUDGET_RENTS 12MO MA',
'BUDGET_REPAIRS_AND_MAINTENANCE 12MO MA',
'BUDGET_TELEPHONE 12MO MA',
'BUDGET_UTILITIES 12MO MA',
'BUDGET_TAXES 12MO MA',
'BUDGET_PROFESSIONAL_FEES 12MO MA',
'BUDGET_SAFETY_EXPENSE 12MO MA',
'BUDGET_BAD_DEBTS 12MO MA',
'BUDGET_BANK_FEES 12MO MA',
'BUDGET_FEES/LICENSE/PERMITS 12MO MA',
'BUDGET_GENERAL_INSURANCE 12MO MA',
'BUDGET_DEPRECIATION 12MO MA',
'ACTUAL_NET_SALES 12MO MA',
'ACTUAL_COS_WAGES 12MO MA',
'ACTUAL_COS_PR_TAXES_/_WC 12MO MA',
'ACTUAL_COS_HEALTH_AND_OTHER 12MO MA',
'ACTUAL_GROSS_PROFIT 12MO MA',
'ACTUAL_WAGES 12MO MA',
'ACTUAL_BONUSES 12MO MA',
'ACTUAL_COMMISSIONS 12MO MA',
'ACTUAL_PAYROLL_TAXES 12MO MA',
'ACTUAL_MARKETING_EXPENSE 12MO MA',
'ACTUAL_RECRUITMENT 12MO MA',
'ACTUAL_AUTO_EXPENSE 12MO MA',
'ACTUAL_CASUAL_LABOR 12MO MA',
'ACTUAL_COMPUTER_EXPENSE 12MO MA',
'ACTUAL_COPIER_AND_FAX 12MO MA',
'ACTUAL_DUES_AND_SUBS 12MO MA',
'ACTUAL_OFFICE_SUPPLIES 12MO MA',
'ACTUAL_POSTAGE_AND_DELIVERY 12MO MA',
'ACTUAL_SALES_EXPENSE 12MO MA',
'ACTUAL_TRAVEL_AND_ENTERTAINMENT 12MO MA',
'ACTUAL_TRAINING 12MO MA',
'ACTUAL_MISCELLANEOUS 12MO MA',
'ACTUAL_EMPLOYEE_PROMOTION 12MO MA',
'ACTUAL_401K_EXPENSE 12MO MA',
'ACTUAL_HEALTH_INSURANCE 12MO MA',
'ACTUAL_RENTS 12MO MA',
'ACTUAL_REPAIRS_AND_MAINTENANCE 12MO MA',
'ACTUAL_TELEPHONE 12MO MA',
'ACTUAL_UTILITIES 12MO MA',
'ACTUAL_TAXES 12MO MA',
'ACTUAL_PROFESSIONAL_FEES 12MO MA',
'ACTUAL_SAFETY_EXPENSE 12MO MA',
'ACTUAL_BAD_DEBTS 12MO MA',
'ACTUAL_BANK_FEES 12MO MA',
'ACTUAL_FEES/LICENSE/PERMITS 12MO MA',
'ACTUAL_GENERAL_INSURANCE 12MO MA',
'ACTUAL_DEPRECIATION 12MO MA',
'ACTUAL_SGNA 12MO MA',
'BUDGET_SGNA 12MO MA',
'BUDGET_HOURS 12MO MA',
'ACTUAL_HOURS 12MO MA',
'BUDGET_REPS 12MO MA',
'ACTUAL_REPS 12MO MA',
'BUDGET_SALES_PER_REP 12MO MA',
'ACTUAL_SALES_PER_REP 12MO MA',
'BUDGET_EMPLOYEES 12MO MA',
'ACTUAL_EMPLOYEES 12MO MA',
'BUDGET_HOURS_PER_EMP 12MO MA',
'ACTUAL_HOURS_PER_EMP 12MO MA',
'BUDGET_USERS 12MO MA',
'ACTUAL_USERS 12MO MA',
'BUDGET_ACTIVE_CLIENTS 12MO MA',
'ACTUAL_ACTIVE_CLIENTS 12MO MA',
'BUDGET_REGS 12MO MA',
'ACTUAL_REGS 12MO MA',
'BUDGET_RESTARTS 12MO MA',
'ACTUAL_RESTARTS 12MO MA']

df8.columns = list12


df6= df6.fillna(0)
df7= df7.fillna(0)
df8= df8.fillna(0)

df9 = df5.merge(df6,on=["YEARMONTH"], how = "left")
df10 = df9.merge(df7,on=["YEARMONTH"], how = "left")
df11 = df10.merge(df8,on=["YEARMONTH"], how = "left")

df11.to_csv(BASEPATHEXT + '/FACT_TIME_SERIES.csv', sep=',', encoding='utf-8')

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ACCIDENT")


##########################################FACT_ACCIDENT############################################################

path = BASEPATH +'/sisenseinjuryreport' 
df4 = appendcsvs(path)

df4.drop('Unnamed: 12', axis=1, inplace=True)
df4.columns = ('ACCIDENT_ID','EMPLOYEE_ID','CLIENT_ID','POSITION_CODE','OFFICE_ID','OCIP_FLAG','INJURY_DATE',
               'NOTIFIED_DATE','NATURE_OF_INJURY','CAUSE_OF_INJURY','BODY_PART','OSHA_REPORTABLE')

df4['CLIENT_ID'] = df4['CLIENT_ID'].str.upper()
df4.to_csv(BASEPATHEXT + '/FACT_ACCIDENT.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_INVOICE_TRACKER")
##########################################FACT_INVOICE_EMAIL############################################################


path = BASEPATH +'/sisenseinvoiceemails' 
df4 = appendcsvs(path)

df4.drop('Unnamed: 9', axis=1, inplace=True)
df4.drop('CLIENT NAME', axis=1, inplace=True)
df4.columns = ['INVOICE_ID','CLIENT_ID','SENT_DATE','SENT_TIME','USER_SENT_ID','SENT_TO','SENT_CC','SENT_BCC']
df4 = df4.replace(np.nan, "No One")
df4['CLIENT_ID'] = df4['CLIENT_ID'].str.upper()
df4['USER_SENT_ID'] = df4['USER_SENT_ID'].str.upper()
df4.to_csv(BASEPATHEXT + '/FACT_INVOICE_TRACKER.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": FACT_ASSIGNMENT")
##########################################FACT_ASSIGNMENT############################################################

path = BASEPATH + '/sisenseassignment'
df4 = appendcsvs(path)

df4.drop('DISPATCH DATE', axis=1, inplace=True)
df4.drop('Unnamed: 12', axis=1, inplace=True)
df4.columns = ['ASSIGNMENT_ID','ORDER_ID','JOB_ID','CLIENT_ID','EMPLOYEE_ID','PAY_RATE','BILL_RATE','PSG_FLAG','OFFICE_ID','DISPATCH_DATE', 'PICK_USER']
df4['JOB_ID'] = df4['JOB_ID'].astype(str)
df4 = df4[df4['OFFICE_ID'].notnull()]
df4['OFFICE_ID'] = df4['OFFICE_ID'].astype(int)
df4.columns = df4.columns.str.strip()
df4['FLAG']= np.where(np.logical_and(df4['PSG_FLAG'] == 'Y', df4['OFFICE_ID'] == 99),1,0)
df4['ASSIST']= np.where(np.logical_or(df4['PSG_FLAG'] == 'Y', df4['OFFICE_ID'] == 99),1,0)
df4.drop('PSG_FLAG', axis=1, inplace=True)


path = BASEPATH + '/sisensejob'
df5 = appendcsvs(path)
df5 = df5[['JOB#','POSITION CODE']]
df5.columns = ['JOB_ID','POSITION_ID']
df5['POSITION_ID'] = df5['POSITION_ID'].str.upper()
df5['JOB_ID'] = df5['JOB_ID'].astype(str)
df5.columns = df5.columns.str.strip()



df6 = df4.merge(df5,on=["JOB_ID"], how = "left")
df6 = df6[df6['POSITION_ID'].notnull()]
df6['DISPATCH_DATE'] = pd.to_datetime(df6['DISPATCH_DATE'])
df6['YEAR'] = df6['DISPATCH_DATE'].dt.year
df6['MONTH'] = df6['DISPATCH_DATE'].dt.month
df6['BILL_OUT_ID']=df6['YEAR'].astype(str)+df6['MONTH'].astype(str)+df6['POSITION_ID']+df6['PAY_RATE'].astype(str)


path = BASEPATHEXT + '/wf_BILL RATE/BILLRATETABLES' 
df2 = appendcsvs(path)
df2['BILLOUT CODE'] =  df2['BILLOUT CODE'].str.upper()


df2.drop('BILLID', axis=1, inplace=True)

df2['BILLID']=df2['YEAR'].astype(str)+df2['MONTH'].astype(str)+df2['BILLOUT CODE']+df2['PAY RATE'].astype(str)
df2.drop('BILLOUT CODE', axis=1, inplace=True)
df2.drop('OCC Code', axis=1, inplace=True)
df2.drop('PAY RATE', axis=1, inplace=True)
df2.drop('YEAR', axis=1, inplace=True)
df2.drop('MONTH', axis=1, inplace=True)

df2.columns = ['SUG_BILL_RATE','BILL_OUT_ID']

df7 = df6.merge(df2,on=["BILL_OUT_ID"], how = "left")

df7['SUG_BILL_RATE'] = df7['SUG_BILL_RATE'].fillna(0).astype(float)

df7['SUG_BILL_RATE'] = np.where(df7['SUG_BILL_RATE'] != 0.0, df7['SUG_BILL_RATE'],
                       np.where(df7['PAY_RATE'].between(6, 25), (.0013*df7['PAY_RATE']**2 - .0667*df7['PAY_RATE'] + 2.5743)*df7['PAY_RATE'],
                       np.where(df7['PAY_RATE'].between(25.01, 40), (.0003*df7['PAY_RATE']**2 - .0204*df7['PAY_RATE'] + 2.0562)*df7['PAY_RATE'],
                       np.where(df7['PAY_RATE'].between(40.01, 50), (.00006*df7['PAY_RATE']**2 - .0063*df7['PAY_RATE'] + 1.8376)*df7['PAY_RATE'],df7['PAY_RATE']*1.55))))


df7.drop('BILL_OUT_ID', axis=1, inplace=True)
df7.drop('YEAR', axis=1, inplace=True)
df7.drop('MONTH', axis=1, inplace=True)

df7['DISPATCH_ID'] = df7['EMPLOYEE_ID'].astype(str) +  df7['ORDER_ID'].astype(str) 

dfx = pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows-1252')
dfx = dfx[['ORDER_ID','FIRST_TIMECARD_DATE','FIRST_DISPATCH_DATE','LATE_DISPATCH','ORDER_DATE']]
dfx['ORDER_ID'] = dfx['ORDER_ID'].astype(str)


df7 = df7.merge(dfx,on=["ORDER_ID"], how = "left")



df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df1 = df1[['ASSIGNMENT_ID','SALES','INVOICE_HOURS','INVOICE_GP']]
df1 = df1.groupby(by = [ 'ASSIGNMENT_ID']).agg({'SALES' : 'sum','INVOICE_HOURS' : 'sum','INVOICE_GP' : 'sum'}).reset_index()


df7 = df7.merge(df1, on = 'ASSIGNMENT_ID', how = 'left')


df7['FIRST_DISPATCH_DATE'] = pd.to_datetime(df7['FIRST_DISPATCH_DATE'])
df7.sort_values(by=['FIRST_DISPATCH_DATE'],ascending=False) 



df7.to_csv(BASEPATHEXT + '/FACT_ASSIGNMENT5.csv', sep=',', encoding='utf-8', index = False)


df7 = df7[['ASSIGNMENT_ID','FLAG','ASSIST']]
df7['ASSIGNMENT_ID'] = df7['ASSIGNMENT_ID'].astype(str)
df5 = pd.read_csv(BASEPATHEXT + '/DIM_ASSIGNMENT.csv',encoding='utf-8')
df5['ASSIGNMENT_ID'] = df5['ASSIGNMENT_ID'].astype(str)
df8 = df5.merge(df7,on=["ASSIGNMENT_ID"], how = "left")



df8.to_csv(BASEPATHEXT + '/DIM_ASSIGNMENT.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_CORPRATE_RECRUITING")

#############################FACT_CORPRATE_RECRUITING#######################################################################################

df11=pd.read_csv(BASEPATH + '/sisenseofficeemployee/officeemployees.csv',encoding='utf-8')
df11 = df11[['EMP ID','OFFICE#','ICIMS ID','HIRE DATE']]
df11.columns = ['EMPLOYEE_ID','OFFICE_ID','ICIMS_ID','HIRE_DATE']
df11['HIRE_DATE'] = pd.to_datetime(df11['HIRE_DATE'])
df11['CAL_DATE'] = df11['HIRE_DATE']
df11['NEW_HIRE'] = 1

df12=pd.read_csv(BASEPATH + '/sisenseofficeemployee/officeemployees.csv',encoding='utf-8')
df12 = df12[['EMP ID','OFFICE#','ICIMS ID','REHIRE DATE']]
df12.columns = ['EMPLOYEE_ID','OFFICE_ID','ICIMS_ID','REHIRE_DATE']
df12['REHIRE_DATE'] = pd.to_datetime(df12['REHIRE_DATE'], errors = 'coerce')
df12['CAL_DATE'] = df12['REHIRE_DATE']
df12['RE_HIRE'] = 1

df13=pd.read_csv(BASEPATH + '/sisenseofficeemployee/officeemployees.csv',encoding='utf-8')
df13 = df13[['EMP ID','OFFICE#','ICIMS ID','TERM DATE']]
df13.columns = ['EMPLOYEE_ID','OFFICE_ID','ICIMS_ID','TERM_DATE']
df13['TERM_DATE'] = pd.to_datetime(df13['TERM_DATE'])
df13['CAL_DATE'] = df13['TERM_DATE']
df13['TERM'] = 1

df14 = df11.append([df12, df13], sort=False)
df14.drop('HIRE_DATE', axis=1, inplace=True)
df14.drop('REHIRE_DATE', axis=1, inplace=True)
df14.drop('TERM_DATE', axis=1, inplace=True)
df11=None
df12=None
df13=None


df14 = df14.dropna(axis=0, subset=['CAL_DATE'])
df14 = df14.dropna(axis=0, subset=['OFFICE_ID'])

df14['NEW_HIRE'] = df14['NEW_HIRE'].fillna(0).astype(int)
df14['RE_HIRE'] = df14['RE_HIRE'].fillna(0).astype(int)
df14['TERM'] = df14['TERM'].fillna(0).astype(int)


    
value_to_check = pd.Timestamp(date.today())
filter_mask = df14['CAL_DATE'] < value_to_check
df14 = df14[filter_mask]


df14.to_csv(BASEPATHEXT + '/FACT_CORPRATE_RECRUITING.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ABC_MEMBERS")
#############################FACT_ABC_MEMBERS#######################################################################################

df11=pd.read_csv(BASEPATHEXT + '/wf_OFFICE/TOTAL_ABC_CLIENTS.csv',encoding='utf-8')
df11['OFFICE_ID'] = df11['OFFICE_ID'].astype(str)
df11['CLIENT_TERRITORY'] = df11['CLIENT_TERRITORY'].astype(str)

df14 = pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows-1252')
df14 = df14[(df14['INVOICE_YEAR'] == 2019)]
df14 = df14[['INVOICE_ID','CLIENT_ID']]
df14 = df14.groupby(by = [ 'CLIENT_ID']).agg({'INVOICE_ID' : 'count'}).reset_index()



df18 = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')
df18 = df18[['CLIENT_ID', 'OFFICE_ID', 'CLIENT_TERRITORY','ABC_MEMBER']]


df16 = df14.merge(df18,on=["CLIENT_ID"], how = "left")
df16 = df16[(df16['ABC_MEMBER'] == 1)]
df16['2019_USERS'] = 1
df16.drop('CLIENT_ID', axis=1, inplace=True)

df16.drop('INVOICE_ID', axis=1, inplace=True)
df16.drop('ABC_MEMBER', axis=1, inplace=True)
df16['OFFICE_ID'] = df16['OFFICE_ID'].astype(str)
df16['CLIENT_TERRITORY'] = df16['CLIENT_TERRITORY'].astype(str)


df16 = df16.groupby(by = ['OFFICE_ID','CLIENT_TERRITORY']).agg({'2019_USERS' : 'sum'}).reset_index()
df16.columns = ['OFFICE_ID','CLIENT_TERRITORY','2019_USERS']


df17 = df11.merge(df16,on=["OFFICE_ID", 'CLIENT_TERRITORY' ], how = "left")

df19 = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')
df19= df19[['CLIENT_TERRITORY','CLIENT_REP_ID']]
df19 = df19.drop_duplicates()

df20 = df17.merge(df19,on=["CLIENT_TERRITORY" ], how = "left")


df20.to_csv(BASEPATHEXT + '/FACT_ABC_MEMBERS.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ABC_HOME_OFFICE")

#############################FACT_ABC_HOME_OFFICE#######################################################################################


df14 = pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows-1252')
df14 = df14[['INVOICE_DATE','CLIENT_ID','SALES']]
df14 = df14.groupby(by = [ 'INVOICE_DATE','CLIENT_ID']).agg({'SALES' : 'sum'}).reset_index()
df14.to_csv(BASEPATHEXT + '/FACT_ABC_HOME_OFFICE.csv', sep=',', encoding='windows-1252', index = False)


df18 = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')
df18 = df18[['CLIENT_ID', 'OFFICE_ID', 'CLIENT_TERRITORY','ABC_MEMBER']]
df18 = df18.drop_duplicates()

df16 = df18.merge(df14,on=["CLIENT_ID"], how = "left")
df16 = df16[(df16['ABC_MEMBER'] == 1)]
df16.drop('ABC_MEMBER', axis=1, inplace=True)

df16['OFFICE_ID'] = df16['OFFICE_ID'].astype(str)
df16['CLIENT_TERRITORY'] = df16['CLIENT_TERRITORY'].astype(str)


#df16 = df16.groupby(by = ['INVOICE_DATE','CLIENT_ID','OFFICE_ID','CLIENT_TERRITORY']).agg({'SALES' : 'sum'}).reset_index()
df16.columns = ['CLIENT_ID', 'OFFICE_ID','CLIENT_TERRITORY','INVOICE_DATE','SALES']


df16.to_csv(BASEPATHEXT + '/FACT_ABC_HOME_OFFICE.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_SAFETY")
#############################FACT_SAFETY#######################################################################################


path =BASEPATH + '/sisenseaccident' 
df17 = appendcsvs(path)

df17.drop('Unnamed: 6', axis=1, inplace=True)
df17.drop('DATE', axis=1, inplace=True)



df17.columns = ['OFFICE_ID','YEAR', 'MONTH','ACCIDENTS','BOOKED_HOURS']
df17['OFFICE_ID'] = df17['OFFICE_ID'].map(lambda x: str(x)[:-2])
df17['OFFICE_ID'] = df17['OFFICE_ID'].apply(lambda x: x.zfill(3))
df17['MONTH'] = df17['MONTH'].astype(str) 
df17['YEAR'] = df17['YEAR'].astype(str) 
df17['DATE'] = df17['MONTH'] + "/15/" + df17['YEAR']
df17.drop('YEAR', axis=1, inplace=True)
df17.drop('MONTH', axis=1, inplace=True)

df17.to_csv(BASEPATHEXT + '/FACT_SAFETY.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_OSHA")

#############################FACT_OSHA#######################################################################################


df8 = pd.read_csv(BASEPATH +'/sisenseemployee/employee.csv',encoding='utf-8')
df8.columns = [c.replace(' ', '_') for c in df8.columns]
df8.columns = [c.replace('-', '_') for c in df8.columns]
df8['EMP_ID'] = df8['EMP_ID'].map(lambda x: str(x)[:-2])
df8 = df8[['EMP_ID','HOME_OFFICE#','OSHA_10_DATE']]
df8.columns=['EMPLOYEE_ID','OFFICE_ID', 'DATE']
df8 = df8[pd.notnull(df8['DATE'])]
df8['DATE'] = pd.to_datetime(df8['DATE'], errors = 'coerce')
df8['OFFICE_ID'] = df8['OFFICE_ID'].astype(str)
df8['OFFICE_ID'] = df8['OFFICE_ID'].apply(lambda x: x.zfill(3))



df9 = pd.read_csv(BASEPATH +'/sisenseemployee/employee.csv',encoding='utf-8')
df9.columns = [c.replace(' ', '_') for c in df9.columns]
df9.columns = [c.replace('-', '_') for c in df9.columns]
df9['EMP_ID'] = df9['EMP_ID'].map(lambda x: str(x)[:-2])
df9 = df9[['EMP_ID','HOME_OFFICE#','MARINE_OSHA_DATE']]
df9.columns=['EMPLOYEE_ID','OFFICE_ID', 'DATE']
df9 = df9[pd.notnull(df9['DATE'])]
df9['DATE'] = pd.to_datetime(df9['DATE'], errors = 'coerce')
df9['OFFICE_ID'] = df9['OFFICE_ID'].astype(str)
df9['OFFICE_ID'] = df9['OFFICE_ID'].apply(lambda x: x.zfill(3))


df80 = pd.concat([df8,df9], axis = 0, ignore_index = True )

df80 = df80.groupby(by = ['OFFICE_ID','DATE']).agg({ 'EMPLOYEE_ID' : 'count'}).reset_index()
df80.columns = ['OFFICE_ID','DATE','OSHA_CERTS']
df80['OFFICE_ID'] = df80['OFFICE_ID'].astype(str)
df80['DATE'] = pd.to_datetime(df80['DATE'] )

dfd=pd.read_csv(BASEPATHEXT + '/DIM_DATE_4.csv',encoding='windows_1252')
dfd = dfd[['DATE','445_MONTH_NUM','445_YEAR_NAME']]
dfd.columns = ['DATE','445_MONTH_NUM','445_YEAR_NAME']
dfd['DATE'] = pd.to_datetime(dfd['DATE'] )

df81 = dfd.merge(df80,on=["DATE"], how = "left")

df88 = df81.groupby(by = [ 'OFFICE_ID','445_YEAR_NAME','445_MONTH_NUM']).agg({'OSHA_CERTS' : 'sum'}).reset_index()


df9 = pd.read_csv(BASEPATHEXT +'/wf_OFFICE/OSHA_GOALS.csv',encoding='utf-8')


df9['OFFICE_ID'] = df9['OFFICE_ID'].astype(str)
df9['OFFICE_ID'] = df9['OFFICE_ID'].apply(lambda x: x.zfill(3))
df9['DATE'] = pd.to_datetime(df9['DATE'], errors = 'coerce')
df9.columns = ['OFFICE_ID','DATE','OSHA_CERT_GOAL']


df9 = dfd.merge(df9,on=["DATE"], how = "left")

df9 = df9.groupby(by = [ 'OFFICE_ID','445_YEAR_NAME','445_MONTH_NUM']).agg({'OSHA_CERT_GOAL' : 'sum'}).reset_index()


df88['OFFICE_ID'] = df88['OFFICE_ID'].map(lambda x: str(x)[:-2])
df88['OFFICE_ID'] = df88['OFFICE_ID'].apply(lambda x: x.zfill(3))


df22 = df9.merge(df88,on=['OFFICE_ID','445_MONTH_NUM','445_YEAR_NAME'], how = "left")
df22 = df22.fillna(0)
df22['DATE'] = df22['445_MONTH_NUM'].astype(str) + "/15/" + df22['445_YEAR_NAME'].astype(str)
df22.drop('445_MONTH_NUM', axis=1, inplace=True)
df22.drop('445_YEAR_NAME', axis=1, inplace=True)


df22.to_csv(BASEPATHEXT + '/FACT_OSHA.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ORDER_PRIORITY")

#############################FACT_ORDER_PRIORITY#######################################################################################


df6 = pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows-1252')
df6 = df6[['CLIENT_ID','CLIENT_TSI_FLAG','CLIENT_CLC_LEGACY_FLAG','ABC_MEMBER','CLIENT_VINTAGE','QUADRANT','CLIENT_LAST_SALE_DATE']]
df6['CLIENT_TSI_FLAG'] = np.where(df6['CLIENT_TSI_FLAG'] == 'N',0,1)
df6['CLIENT_CLC_LEGACY_FLAG'] = np.where(df6['CLIENT_CLC_LEGACY_FLAG'] == 'N',0,1)
df6['CLIENT_LAST_SALE_DATE'] = pd.to_datetime(df6['CLIENT_LAST_SALE_DATE'])
df6['CLIENT_LAST_SALE_DATE'] = df6['CLIENT_LAST_SALE_DATE'].fillna(pd.Timestamp(date.today()))



df7 = pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows-1252')
df7 = df7[['CLIENT_ID', 'ORDERED', 'ORDER_ID', 'ORDER_START_LAG_DAYS', 'ORDER_DURATION_DAYS']]
df7 = df7.groupby(by = ['CLIENT_ID']).agg({'ORDERED' : 'sum', 'ORDER_ID' : 'nunique','ORDER_START_LAG_DAYS' : 'mean','ORDER_DURATION_DAYS' : 'mean'}).reset_index()


df8 = df6.merge(df7,on=["CLIENT_ID"], how = "inner")
df8 = df8.drop_duplicates(subset='CLIENT_ID', keep='first', inplace=False)
df8['LOGIT'] = df8['CLIENT_TSI_FLAG']*0.38793522+ df8['CLIENT_CLC_LEGACY_FLAG']*-0.26998766 + df8['ABC_MEMBER']*0.41016871 + df8['CLIENT_VINTAGE']* 0.00223186 + df8['QUADRANT']*-1.63706783 + df8['ORDERED']*0.0030608 + df8['ORDER_ID']* -0.00205972 + df8['ORDER_START_LAG_DAYS']* 0.01942065+  df8['ORDER_DURATION_DAYS']*0.00384266         
df8['CLIENT_SCORE'] = 1/(1+np.exp(-df8['LOGIT']))
df8['RUN_DATE'] = datetime.datetime.now().strftime("%Y-%m-%d")

df8.to_csv(BASEPATHEXT + '/ML MODELS/CLIENT_SCORE_ARCHIVE/CLIENT_SCORES_AS_OF' + datetime.datetime.now().strftime("%Y-%m-%d") +'.csv', sep=',', encoding='utf-8', index = False)

df8['ORDER_PRIORITY_TYPE'] = np.where(pd.to_datetime(df8['CLIENT_LAST_SALE_DATE']) == pd.Timestamp(date.today()), 'NEW USER',
                             np.where(((pd.Timestamp(date.today()) - pd.to_datetime(df8['CLIENT_LAST_SALE_DATE'])).dt.days).astype(float) > 180, 'RESTART' ,  'ACTIVE'))

df9 = df8[['CLIENT_ID','CLIENT_SCORE','ORDER_PRIORITY_TYPE']]


df10=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv', encoding='utf-8')

df11 = df10.merge(df9,on=["CLIENT_ID"], how = "left")
df11['CLIENT_SCORE'] = np.where(df11['ORDER_PRIORITY_TYPE'] == 'ACTIVE', df11['CLIENT_SCORE'], 1)
df7 = None
df6 = None

df11 = df11.drop_duplicates(subset='CLIENT_ID', keep='first', inplace=False)
df11.to_csv(BASEPATHEXT +'/DIM_CLIENT.csv', sep=',', encoding='utf-8', index = False)




print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_MONTHLY_KPI")

############################################MONTHLY_KPI##################################################################

path = BASEPATH + '/sisensebudgetactual' 
df1 = appendcsvs(path)


df1['YEARMONTH'] = df1['YEARMONTH'].astype(str)
df1['YEARMONTH'] = df1['YEARMONTH'] +"01"
df1['YEARMONTH'] = pd.to_datetime(df1['YEARMONTH'])

df1.drop('Unnamed: 38', axis=1, inplace=True)

df1.columns = [c.replace(' ', '_') for c in df1.columns]

df1.drop('BUDGET_SALES', axis=1, inplace=True)
df1.drop('ACTUAL_SALES', axis=1, inplace=True)
df1.drop('BUDGET_GP', axis=1, inplace=True)
df1.drop('ACTUAL_GP', axis=1, inplace=True)
df1.drop('BUDGET_OE', axis=1, inplace=True)
df1.drop('ACTUAL_OE', axis=1, inplace=True)
df1.drop('BUDGET_OI', axis=1, inplace=True)
df1.drop('ACTUAL_OI', axis=1, inplace=True)
df1.drop('BUDGET_WAGES', axis=1, inplace=True)
df1.drop('ACTUAL_WAGES', axis=1, inplace=True)
df1.drop('BUDGET_SALES_EXP', axis=1, inplace=True)
df1.drop('ACTUAL_SALES_EXP', axis=1, inplace=True)
df1.drop('BUDGET_EMP_PROMO', axis=1, inplace=True)
df1.drop('ACTUAL_EMP_PROMO', axis=1, inplace=True)
df1.drop('BUDGET_TRAVEL', axis=1, inplace=True)
df1.drop('ACTUAL_TRAVEL', axis=1, inplace=True)
df1.drop('BUDGET_BAD_DEBT', axis=1, inplace=True)
df1.drop('ACTUAL_BAD_DEBT', axis=1, inplace=True)

df1.to_csv(BASEPATHEXT + '/FACT_MONTHLY_KPI.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_FIELD_EMPLOYEE_COUNTS")

############################################FACT_FIELD_EMPLOYEE_COUNTS##################################################################

df11=pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df11.columns = [c.replace(' ', '_') for c in df11.columns]
df11.columns = [c.replace('-', '_') for c in df11.columns]
df11 = df11[['EMP_ID','445_HIRE_DATE']]
df11['445_HIRE_DATE'] = pd.to_datetime(df11['445_HIRE_DATE'])
df11.columns=['EMPLOYEE_ID','CAL_DATE']
df11['NEW_HIRE'] = 1


df13=pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df13.columns = [c.replace(' ', '_') for c in df13.columns]
df13.columns = [c.replace('-', '_') for c in df13.columns]
df13 = df13[['EMP_ID','445_LAST_WORKED']]
df13['445_LAST_WORKED'] = pd.to_datetime(df13['445_LAST_WORKED'],errors='coerce')
df13.columns=['EMPLOYEE_ID','CAL_DATE']
df13['LAST_WORKED'] = 1


path = BASEPATH + '/sisenserehire' 
df14 = appendcsvs(path)

df14.columns = [c.replace(' ', '_') for c in df14.columns]
df14.columns = [c.replace('-', '_') for c in df14.columns]
df14 = df14[['EMP_ID','445_REHIRE_DATE']]
df14['445_REHIRE_DATE'] = pd.to_datetime(df14['445_REHIRE_DATE'],errors='coerce')
df14.columns=['EMPLOYEE_ID','CAL_DATE']
df14['REHIRE'] = 1
df14.head()


df16 = df11.append([df13, df14], sort=False)

df18 = pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df18.columns = [c.replace(' ', '_') for c in df18.columns]
df18.columns = [c.replace('-', '_') for c in df18.columns]
df18 = df18[['EMP_ID','HOME_OFFICE#']]
df18.columns=['EMPLOYEE_ID','OFFICE_ID']

df19 = df16.merge(df18,on=["EMPLOYEE_ID"], how = "left")



df19 = df19.groupby(by = ['EMPLOYEE_ID','OFFICE_ID','CAL_DATE']).agg({
     'NEW_HIRE' : 'sum','LAST_WORKED' : 'sum','REHIRE': 'sum'  }).reset_index()

df19['OFFICE_ID'] = df19['OFFICE_ID'].astype(int)



df20 = pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df20 = df20[(df20['STATUS'].isin(['WORKING', 'AVAILABLE','ACTIVE'] ))]
df20 = df20[['EMP ID','HOME OFFICE#', 'STATUS']]
df20['CAL_DATE'] = pd.to_datetime(date.today() + timedelta(days=-1))
df20.columns = ['EMPLOYEE_ID','OFFICE_ID','STATUS','CAL_DATE']
df20['COUNT'] = 1
df20 =pd.pivot_table(df20, values = 'COUNT', index=['EMPLOYEE_ID','OFFICE_ID','CAL_DATE'], columns = 'STATUS', aggfunc = "sum").reset_index()
df20 = df20[['EMPLOYEE_ID','OFFICE_ID','CAL_DATE','WORKING', 'AVAILABLE','ACTIVE']]
df20.columns = ['EMPLOYEE_ID','OFFICE_ID','CAL_DATE','EMPLOYEES_WORKING','EMPLOYEES_AVAILABLE','EMPLOYEES_ACTIVE']
df20 = df20[['EMPLOYEE_ID','OFFICE_ID','CAL_DATE','EMPLOYEES_WORKING','EMPLOYEES_AVAILABLE','EMPLOYEES_ACTIVE']]
df20  = df20.fillna(0)


df20.to_csv(BASEPATHEXT + '/Wf_EMP_WORKING/WORKING_ON' + datetime.datetime.now().strftime("%Y-%m-%d") +'.csv', sep=',', encoding='utf-8', index = False)

path = BASEPATHEXT + '/Wf_EMP_WORKING'
df20 = appendcsvs(path)
df20['CAL_DATE'] = pd.to_datetime(df20['CAL_DATE'])



df19 = df19.append(df20, sort=False)


df19 = df19.groupby(by = ['EMPLOYEE_ID','OFFICE_ID','CAL_DATE']).agg({
     'NEW_HIRE' : 'sum','LAST_WORKED' : 'sum','REHIRE': 'sum' , "EMPLOYEES_WORKING" : 'sum', "EMPLOYEES_AVAILABLE" : 'sum', "EMPLOYEES_ACTIVE" : 'sum'}).reset_index()

df19['EMPLOYEES_WORKING'] = df19['EMPLOYEES_WORKING'].fillna(0).astype(float)
df19['NEW_HIRE'] = df19['NEW_HIRE'].fillna(0).astype(float)
df19['LAST_WORKED'] = df19['LAST_WORKED'].fillna(0).astype(float)
df19['EMPLOYEES_AVAILABLE'] = df19['EMPLOYEES_AVAILABLE'].fillna(0).astype(float)
df19['EMPLOYEES_ACTIVE'] = df19['EMPLOYEES_ACTIVE'].fillna(0).astype(float)
df19['REHIRE'] = df19['REHIRE'].fillna(0).astype(float)


df21=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df21=df21[['EMPLOYEE_ID','INVOICE_DATE']]
df21.columns = ['EMPLOYEE_ID','CAL_DATE']
df21 = df21.drop_duplicates()


df22=pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df22 = df22[['EMP ID','HOME OFFICE#']]
df22.columns = ['EMPLOYEE_ID','OFFICE_ID']
df22 = df21.merge(df22,on=["EMPLOYEE_ID"], how = "left")
df22['EMPLOYEES_PAID'] = 1


df19 = df19.append(df22, sort=False)


df19 = df19.groupby(by = ['EMPLOYEE_ID','OFFICE_ID','CAL_DATE']).agg({
     'NEW_HIRE' : 'sum','LAST_WORKED' : 'sum','REHIRE': 'sum' , "EMPLOYEES_WORKING" : 'sum', "EMPLOYEES_AVAILABLE" : 'sum', "EMPLOYEES_ACTIVE" : 'sum', "EMPLOYEES_PAID" : 'sum'}).reset_index()

df19['EMPLOYEES_PAID'] = df19['EMPLOYEES_PAID'].fillna(0).astype(float)
df19['EMPLOYEES_WORKING'] = df19['EMPLOYEES_WORKING'].fillna(0).astype(float)
df19['NEW_HIRE'] = df19['NEW_HIRE'].fillna(0).astype(float)
df19['LAST_WORKED'] = df19['LAST_WORKED'].fillna(0).astype(float)
df19['EMPLOYEES_AVAILABLE'] = df19['EMPLOYEES_AVAILABLE'].fillna(0).astype(float)
df19['EMPLOYEES_ACTIVE'] = df19['EMPLOYEES_ACTIVE'].fillna(0).astype(float)
df19['REHIRE'] = df19['REHIRE'].fillna(0).astype(float)

    
df19.to_csv(BASEPATHEXT +  '/FACT_FIELD_EMP_COUNTS6.csv', sep=',', encoding='windows-1252', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ETC")

df11 = None
df13 = None
df14= None
df15 = None
df16= None
df18 = None
df19 = None


##################################################              TIME CARD CAPSURE                               ################################################################

import pymysql

user = 'analytics'
passw = '8q4p8i1K'
host =  'Sql01.ti.local'
port = 3306
database = 'TimeTracking'

connection = pymysql.connect(host=host,
                       port=port,
                       user=user, 
                       passwd=passw,  
                       db=database,
                       charset='utf8')

query1 = """
SELECT DISTINCT Timecard.TimecardID,Timecard.AssignmentID, Assignment.EmployeeID, JobID, OfficeID, TimecardStatus.TimecardStatus,SubmittedDateTime,TotalWorkDuration,ApprovalDateTime,RejectedDateTime,WeekendDate,WeekStartDate,
case when IsSubmittedWithSignature = 0 then 'EMAIL' else 'SIGNATURE' end as 'APPROVAL TYPE',
1 as ETC,
case when TimecardStatus.TimecardStatus = 'SUBMITTED' then 1 else 0 end as SUBMITTED,
case when TimecardStatus.TimecardStatus = 'CORRECTION' then 1 else 0 end as CORRECTION,
case when TimecardStatus.TimecardStatus = 'APPROVED' then 1 else 0 end as APPROVED


FROM Timecard 

JOIN TimecardStatus on TimecardStatus.TimecardStatusID = Timecard.TimecardStatusID 
JOIN TimecardWeek on TimecardWeek.TimecardWeekID = Timecard.TimecardWeekID 
join Assignment on Assignment.AssignmentID = Timecard.AssignmentID 
join EmployeeOffice on EmployeeOffice.EmployeeID = Assignment.EmployeeID 

"""

df = pd.read_sql(query1, connection)

df.to_csv(BASEPATHEXT + '/FACT_ETC4.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_ETC_SUP")


query3 = """

SELECT TimecardApprovalSupervisor.TimecardID, Supervisor.EmailAddress,
case when SUBSTRING_INDEX(EmailAddress, '@', -1) = 'tradesmeninternational.com' AND SUBSTRING_INDEX(EmailAddress, '@', 1) not in ('ashley.boyce','timecapsure')
	THEN 'TRADESMEN APPROVED' else 'CUSTOMER APPROVED' end as 'APPROVER'
FROM TimecardApprovalSupervisor 
JOIN Supervisor on Supervisor.SupervisorID = TimecardApprovalSupervisor.SupervisorID

"""


dfy = pd.read_sql(query3, connection)

dfy.to_csv(BASEPATHEXT + '/DIM_ETC_SUP.csv', sep=',', encoding='utf-8', index = False)


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ETC_EVENTS")

query2 = """ SELECT TimecardEventQueue.TimecardID, Timecard.AssignmentID, Assignment.EmployeeID, JobID, OfficeID,QueueStatus.Description as queuestatus, 
            EventType.Description as eventtype ,QueueMessage,TimecardEventQueue.LastModifiedDateTime 
            
            FROM TimecardEventQueue 
            
            join Timecard on Timecard.TimecardID = TimecardEventQueue.TimecardID 
            join Assignment on Assignment.AssignmentID = Timecard.AssignmentID 
            join EmployeeOffice on EmployeeOffice.EmployeeID = Assignment.EmployeeID 
            join EventType on EventType.EventTypeID = TimecardEventQueue.EventTypeID 
            JOIN QueueStatus on QueueStatus.QueueStatusID = TimecardEventQueue.QueueStatusID """

dff = pd.read_sql(query2, connection)

dff.to_csv(BASEPATHEXT + '/FACT_ETC_EVENTS.csv', sep=',', encoding='utf-8', index = False)


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_I9")

###################################################               FACT_I9                 ################################################################ DEPENDENT ON EXISTING

df11=pd.read_csv(BASEPATH + '/sisensei9/TRACKER.I9.csv',encoding='utf-8')
df11= df11[['EMP ID','DISPATCH DATE','TOTAL CHECKS','REPORT DATE']]

df9 = pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df9 = df9[['EMP ID','HOME OFFICE#']]

df19 = df11.merge(df9,on=["EMP ID"], how = "left")
df19.columns = ['EMPLOYEE_ID','FIRST_DISPATCH_DATE','TOTAL_CHECKS', 'PROD_DATE','OFFICE_ID']
df19['STATUS'] = np.where(pd.isnull(df19['FIRST_DISPATCH_DATE']), 'NEVER DISPATCHED','WORKED')

df19.to_csv(BASEPATHEXT + '/FACT_I9.csv', sep=',', encoding='utf-8', index = False)


df19 = df19[(df19.TOTAL_CHECKS != 0)]
df19 = df19[['EMPLOYEE_ID', 'OFFICE_ID', 'TOTAL_CHECKS']]
df19 = df19.drop_duplicates()
df19.columns = ['EMPLOYEE_ID','OFFICE_ID', 'ILLEGAL_CHECKS']
df19 = df19.groupby(by = ['EMPLOYEE_ID','OFFICE_ID']).agg({'ILLEGAL_CHECKS' : 'sum' }).reset_index()
df19['INCOMPLETE_I9'] = 1


df9000 =  pd.read_csv(BASEPATHEXT + '/FACT_FIELD_EMP_COUNTS6.csv',encoding='windows_1252')


df9000['OFFICE_ID'] = df9000['OFFICE_ID'].astype(str)
df9000['OFFICE_ID'] = df9000['OFFICE_ID'] .map(lambda x: str(x)[:-2])
df19['OFFICE_ID'] = df19['OFFICE_ID'].fillna(-1)
df19['OFFICE_ID'] = df19['OFFICE_ID'].astype(str)
df19['OFFICE_ID'] = df19['OFFICE_ID'] .map(lambda x: str(x)[:-2])
df9000.head()

df19 = df9000.merge(df19,on=['EMPLOYEE_ID','OFFICE_ID',], how = "left")
df19['INCOMPLETE_I9'] = df19['INCOMPLETE_I9'].fillna(0).astype(float)
df19['ILLEGAL_CHECKS'] = df19['ILLEGAL_CHECKS'].fillna(0).astype(float)
df19['INCOMPLETE_I9'] = np.where((df19['NEW_HIRE'] == 1) | (df19['REHIRE'] == 1) , df19['INCOMPLETE_I9'],0 )
df19['ILLEGAL_CHECKS'] = np.where((df19['NEW_HIRE'] == 1) | (df19['REHIRE'] == 1) , df19['ILLEGAL_CHECKS'],0 )

df19.to_csv(BASEPATHEXT +  '/FACT_FIELD_EMP_COUNTS6.csv', sep=',', encoding='windows-1252', index = False)


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting DIM_JIRA_ISSUES")


###################################################                JIRA             ################################################################

import  psycopg2

conn = psycopg2.connect(host='jira.tradesmeninternational.com', database='jira', user='postgres', password='Trade9760')

cur = conn.cursor()

cur.execute("SELECT jiraissue.id, issuenum,project.pname, project.lead, projecttype,reporter,assignee,creator,to_char(created,'MM-DD-YYYY'),to_char(updated,'MM-DD-YYYY'),to_char(resolutiondate,'MM-DD-YYYY'),watches,issuetype.pname, priority.pname,resolution.pname, issuestatus.pname FROM public.jiraissue LEFT JOIN public.project on project.id = jiraissue.project LEFT JOIN  public.issuetype on issuetype.id = jiraissue.issuetype LEFT JOIN  public.priority on priority.id = jiraissue.priority LEFT JOIN  public.resolution on resolution.id = jiraissue.resolution LEFT JOIN  public.issuestatus on issuestatus.id = jiraissue.issuestatus;")

the_data = cur.fetchall()

colnames = [desc[0] for desc in cur.description]

cur.close()
conn.close()


df1 = pd.DataFrame(the_data)
df1.columns = ['ISSUE_ID','ISSUE_NUM','PROJECT_NAME', 'PROJECT_LEADER','PROJECT_TYPE','REPOERTED_BY','ASSIGNED_TO', 'CREATED_BY',
               'CREATED_DATE','LAST_UPDATE_DATE','RESOLUTION_DATE','WATCHES','ISSUE_TYPE','ISSUE_PRIORITY','RESOLUTION','ISSUE_STATUS']

df1['CREATED_BY'] = df1['CREATED_BY'].str.split('@').str[0]
df1['REPOERTED_BY'] = df1['REPOERTED_BY'].str.split('@').str[0]
df1['ASSIGNED_TO'] = df1['ASSIGNED_TO'].str.split('@').str[0]
df1['REPOERTED_BY'] = df1['REPOERTED_BY'].str.upper()
df1['ASSIGNED_TO'] = df1['ASSIGNED_TO'].str.upper()
df1['CREATED_BY'] = df1['CREATED_BY'].str.upper()

df1.to_csv(BASEPATHEXT + '/DIM_JIRA_ISSUES.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_JIRA_ISSUES")

df3 = pd.read_csv(BASEPATHEXT + '/DIM_JIRA_ISSUES.csv',encoding='utf-8')


df3 = df3[['ISSUE_ID','REPOERTED_BY','ASSIGNED_TO', 'CREATED_BY','CREATED_DATE','LAST_UPDATE_DATE','RESOLUTION_DATE','WATCHES']]

df3['CREATED_DATE'] = pd.to_datetime(df3['CREATED_DATE'])
df3['LAST_UPDATE_DATE'] = pd.to_datetime(df3['LAST_UPDATE_DATE'])
df3['RESOLUTION_DATE'] = pd.to_datetime(df3['RESOLUTION_DATE'])
df3['DAYS_TO_CLOSE'] = ((df3['RESOLUTION_DATE'] - df3['CREATED_DATE']).dt.days).astype(float)
df3['REPOERTED_BY'] = df3['REPOERTED_BY'].str.upper()
df3['ASSIGNED_TO'] = df3['ASSIGNED_TO'].str.upper()
df3['CREATED_BY'] = df3['CREATED_BY'].str.upper()
df3["ISSUES"]= 1
df3["RESOLVED_ISSUES"] = np.where(pd.isnull(df3['RESOLUTION_DATE']),0,1)
df3['SAME_DAY_RESOLUTION'] = np.where(df3['CREATED_DATE'] == df3['RESOLUTION_DATE'],1,0)


df3.to_csv(BASEPATHEXT + '/FACT_JIRA_ISSUES.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_JIRA_COUNTS")

df3 = pd.read_csv(BASEPATHEXT + '/FACT_JIRA_ISSUES.csv',encoding='utf-8')
df3 = df3[['ISSUE_ID','REPOERTED_BY','ASSIGNED_TO','CREATED_DATE']]
df3['CREATED_DATE'] = pd.to_datetime(df3['CREATED_DATE'],errors='coerce')
df3.columns=['ISSUE_ID','CREATED_BY','ASSIGNED_TO','CAL_DATE']
df3['NEW_ISSUES'] = 1

df13 = pd.read_csv(BASEPATHEXT + '/FACT_JIRA_ISSUES.csv',encoding='utf-8')
df13 = df13[['ISSUE_ID','REPOERTED_BY','ASSIGNED_TO','RESOLUTION_DATE']]
df13['RESOLUTION_DATE'] = pd.to_datetime(df13['RESOLUTION_DATE'],errors='coerce')
df13.columns=['ISSUE_ID','CREATED_BY','ASSIGNED_TO','CAL_DATE']
df13['CLOSED_ISSUES'] = 1
df13 = df13[df13['CAL_DATE'].notnull()]

df16 = df3.append(df13, sort=False)

df16 = df16.groupby(by = ['ISSUE_ID','CREATED_BY','ASSIGNED_TO','CAL_DATE']).agg({'NEW_ISSUES' : 'sum','CLOSED_ISSUES' : 'sum'}).reset_index()

df16.to_csv(BASEPATHEXT + '/FACT_JIRA_COUNTS.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_BX_TERRITORY_ANALYSIS")
#############################################   FACT_BX_TERRITORY_ANALYSIS  ########################################################


df4=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')
df4 = df4[['TERRITORY','TERRITORY REP EMP ID']]
df4.columns = ['TERRITORY','SALES_REP_ID']
df4 = df4.drop_duplicates()


df5=pd.read_csv(BASEPATHEXT + '/wf_OFFICE/TERRITORY_ZIP.csv',encoding='windows-1252')
df5['ZIP'] = df5['ZIP'].astype(str)


df6=pd.read_csv(BASEPATHEXT + '/wf_GEOGRAPHY/Zip Lookup.csv',encoding='windows-1252')
df6['ZIP'] = df6['ZIP'].astype(str)

df5 = df5.merge(df6,on=["ZIP"], how = "left")

df5 = df5.merge(df4,on=["TERRITORY"], how = "left")

df5.to_csv(BASEPATHEXT + '/FACT_BX_TERRITORY_ANALYSIS.csv', sep=',', encoding='utf-8', index = False)


df5 = df5[['OFFICE_ID','LEAD_WEIGHT','SOLID_LEADS','TOTAL_LEADS']]


df5 = df5.groupby(by = ['OFFICE_ID']).agg({'LEAD_WEIGHT' : 'mean', 'SOLID_LEADS' : 'sum','TOTAL_LEADS' : 'sum'}).reset_index()
df5['OFFICE_ID'] = df5['OFFICE_ID'].astype(str)



df13 = pd.read_csv(BASEPATHEXT + '/DIM_OFFICE_EMPLOYEE7.csv',encoding='windows-1252')
df13 = df13[(df13['OFFICE_EMPLOYEE_POSITION_CODE'].isin(['AREP','DBD','INSREP','MAE','REP','REPIND']))]
df14 = df13[['OFFICE_EMPLOYEE_OFFICE_ID','OFFICE_EMPLOYEE_ID','TERM_REASON','OFFICE_EMPLOYEE_TERMED','OFFICE_EMPLOYEE_TENURE']]
df14.columns = ['OFFICE_ID','OFFICE_EMPLOYEE','TERM_REASON','TERM_DATE', 'TENURE']
df14['TERM_DATE'] = pd.to_datetime(df14['TERM_DATE'], errors = 'coerce')
df14 = df14[(df14['TERM_DATE'] >= pd.to_datetime('2016-01-01'))]

df15 = df13[['OFFICE_EMPLOYEE_OFFICE_ID','OFFICE_EMPLOYEE_ID','TERM_REASON','OFFICE_EMPLOYEE_TERMED','OFFICE_EMPLOYEE_TENURE']]
df15.columns = ['OFFICE_ID','OFFICE_EMPLOYEE','TERM_REASON','TERM_DATE','TENURE']
df15 = df15[(df15['TERM_DATE'].isnull())]

df16 = df14.append(df15)


df17 = df16.groupby(by = ['OFFICE_ID']).agg({'OFFICE_EMPLOYEE' : 'count', 'TERM_DATE' : 'count','TENURE' : 'mean'}).reset_index()
df17['OFFICE_ID'] = df17['OFFICE_ID'].astype(str)


df18 = pd.pivot_table(df16, values = ['OFFICE_EMPLOYEE'], index=['OFFICE_ID'], columns = 'TERM_REASON', aggfunc = "count").reset_index()
df18['OFFICE_ID'] = df18['OFFICE_ID'].astype(str)



df19 = df5.merge(df17, on = 'OFFICE_ID',how = 'left')

df20 = df19.merge(df18, on = 'OFFICE_ID',how = 'left')
df20.columns = ['OFFICE_ID','LEAD_WEIGHT','SOLID_LEADS','TOTAL_LEADS','REPS','TERMS', 'AVG_TENURE_MO', 'INVOLUNTARY_TERM', 'VOLUNTARY_TERM']

df20.to_csv(BASEPATHEXT + '/FACT_OFFICE_TERMS_ANALYSIS.csv', sep=',', encoding='utf-8', index = False)





print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ZIP_PRODUCTION")

#############################################   FACT_ZIP_PRODUCTION ########################################################
df20=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows_1252')
df20=df20[['CLIENT_ID','CLIENT_ZIP']]
df20 = df20.drop_duplicates()

df21=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')

df22 = df21.merge(df20, on = "CLIENT_ID")


df22 = df22.groupby(by = ['CLIENT_ZIP','CLIENT_ID']).agg({'EMPLOYEE_ID' : 'nunique',  'TIME_CARD_ID' : 'nunique',
                  'INVOICE_HOURS' : 'sum' , 'SALES' : 'sum', 'INVOICE_WAGES' : 'sum' , 'INVOICE_GP' : 'sum' ,'ORDER_ID' : 'nunique',
                  'JOB_ID' : 'nunique', 'SUG_SALES' : 'sum' }).reset_index()

df18=pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows_1252')
df18=df18[['CLIENT_ID','ORDERED','FILLED']]
df18.columns = ['CLIENT_ID','ORDERED','FILLED']

df19 = df18.merge(df20, on = "CLIENT_ID")

df19 = df19.groupby(by = ['CLIENT_ZIP','CLIENT_ID']).agg({'ORDERED' : 'sum', 'FILLED' : 'sum'}).reset_index() 
    

df23 = pd.read_csv(BASEPATHEXT + '/FACT_BX_TERRITORY_ANALYSIS.csv',encoding='windows_1252')
df23.columns.values[0] = "CLIENT_ZIP"


df24 = df22.merge(df23, on = 'CLIENT_ZIP' , how = 'left')

df24 = df24.merge(df19, on = ['CLIENT_ZIP','CLIENT_ID'] , how = 'left')

df24.drop('OFFICE_ID', axis=1, inplace=True)
df24.drop('STATE', axis=1, inplace=True)
df24.drop('GEO_REGION', axis=1, inplace=True)
df24.drop('GEO_DIVISION', axis=1, inplace=True)
df24.drop('MSA_NUM', axis=1, inplace=True)
df24.drop('MSA_NAME', axis=1, inplace=True)
df24.drop('COUNTY_NAME', axis=1, inplace=True)
df24.drop('CITY', axis=1, inplace=True)
df24.drop('SALES_REP_ID', axis=1, inplace=True)

df24.to_csv(BASEPATHEXT + '/FACT_ZIP_PRODUCTION.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FLAKE_EMP_DATES")


#############################################   FLAKE_EMP_DATES ########################################################
df8=pd.read_csv(BASEPATH + '/sisenseemployee/employee.csv',encoding='utf-8')
df8 = df8[['EMP ID','HIRE DATE','LAST WORKED','HOME OFFICE#']]
df8.columns = ['EMPLOYEE_ID','HIRE_DATE', 'LAST_WORKED', 'OFFICE_ID']

df8.to_csv(BASEPATHEXT + '/FLAKE_EMP_DATES.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_REP_RANKS")

#############################################   FACT_REP_RANKS ########################################################

df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df1 = df1[['TERRITORY_REP_ID','INVOICE_DATE','OFFICE_ID','SALES','INVOICE_GP','INVOICE_HOURS']]
df1.columns = ['REP_ID','DATE','OFFICE_ID','SALES','GP','HOURS']
df1['DATE'] = pd.to_datetime(df1['DATE'] )
df1['OFFICE_ID'] = df1['OFFICE_ID'].astype(str)


dfd = pd.read_csv(BASEPATHEXT + '/DIM_DATE_4.csv',encoding='windows_1252')
dfd = dfd[['DATE','445_WEEK_NUM','445_MONTH_NUM','445_QUARTER_NUM','445_YEAR_NAME']]
dfd['DATE'] = pd.to_datetime(dfd['DATE'] )

df1 = df1.merge(dfd, on = 'DATE')

df1 = df1.groupby(by = ['REP_ID','DATE','OFFICE_ID','445_WEEK_NUM','445_MONTH_NUM','445_QUARTER_NUM','445_YEAR_NAME']).agg({'SALES' : 'sum', 
                 'GP' : 'sum', 'HOURS' : 'sum'}).reset_index()

df1['GP_DOLLARS_PER_HOUR'] = df1['GP'] / (df1['HOURS']+.0000001)
df1['GP_MARGIN'] = df1['GP'] / (df1['SALES']+.0000001)


dfg = pd.read_csv(BASEPATHEXT + '/wf_OFFICE/RANKING_GOALS.csv',encoding='windows_1252')
dfg.columns.values[1] = "DATE"
dfg['DATE'] = pd.to_datetime(dfg['DATE'] )
dfg['OFFICE_ID'] = dfg['OFFICE_ID'].astype(str)

df1 = df1.merge(dfg, on = ['DATE', 'OFFICE_ID'] ,how = 'left')
df1['SALES_CONTRIBUTION'] = df1['SALES'] / (df1['BUDGET_SALES']+.0000001)


df15 = pd.read_csv(BASEPATHEXT +'/DIM_OFFICE_EMPLOYEE7.csv', encoding='utf-8')
df15 = df15[['OFFICE_EMPLOYEE_ID','OFFICE_EMPLOYEE_OFFICE_ID']]
df15.columns = ['REP_ID','OFFICE_ID']
df15['OFFICE_ID'] = df15['OFFICE_ID'].astype(str)
df15['REP_ID'] = df15['REP_ID'].astype(str)


df1 = df1.merge(df15, on = ['REP_ID', 'OFFICE_ID'])



df8=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')
df8['CLIENT ID']= df8['CLIENT ID'].str.upper()
df9 = df8[['REGISTRATION REP ID','CLIENT ID','REGISTRATION DATE']]
df9.columns = ['REP_ID','MSA','DATE']
df9['REP_ID'] = df9['REP_ID'].astype(str)
df9['REP_ID'] = df9['REP_ID'].map(lambda x: str(x)[:-2])
df9['DATE'] = pd.to_datetime(df9['DATE'])
df9['445_YEAR_NAME'] = df9['DATE'].dt.year
df9['445_MONTH_NUM'] = df9['DATE'].dt.month
df9['445_WEEK_NUM'] = df9['DATE'].dt.week
df9 = df9.groupby(by = [ 'REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM']).agg({'MSA' : 'nunique'}).reset_index()

df1 = df1.merge(df9,on=["REP_ID",'445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM'], how = "left")
df1 = df1.fillna(0)



df10 = df8[['REGISTRATION REP ID','CLIENT ID','445 FIRST SALE']]
df10.columns = ['REP_ID','NEW_USER','DATE']
df10['REP_ID'] = df10['REP_ID'].astype(str)
df10['REP_ID'] = df10['REP_ID'].map(lambda x: str(x)[:-2])
df10['DATE'] = pd.to_datetime(df10['DATE'])
df10['445_YEAR_NAME'] = df10['DATE'].dt.year
df10['445_MONTH_NUM'] = df10['DATE'].dt.month
df10['445_WEEK_NUM'] = df10['DATE'].dt.week
df10.drop('DATE', axis=1, inplace=True)



df10 = df10.groupby(by = [ 'REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM']).agg({'NEW_USER' : 'nunique'}).reset_index()
df1 = df1.merge(df10,on=["REP_ID",'445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM'], how = "left")
df1 = df1.fillna(0)


df9000 =  pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows_1252')
df9000 = df9000[['REP_ID','ORDER_ID','ORDER_DATE']]
df9000.columns = ['REP_ID','NEW_ORDER','DATE']
df9000['DATE'] = pd.to_datetime(df9000['DATE'])
df9000['445_YEAR_NAME'] = df9000['DATE'].dt.year
df9000['445_MONTH_NUM'] = df9000['DATE'].dt.month
df9000['445_WEEK_NUM'] = df9000['DATE'].dt.week
df9000 = df9000.groupby(by = [ 'REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM']).agg({'NEW_ORDER' : 'nunique'}).reset_index()


df1 = df1.merge(df9000,on=['REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM'], how = "left")
df1 = df1.fillna(0)


df9000 =  pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows_1252')
df9000 = df9000[['REP_ID','ORDERED','ORDER_DATE']]
df9000.columns = ['REP_ID','ORDERED','DATE']
df9000['DATE'] = pd.to_datetime(df9000['DATE'])
df9000['445_YEAR_NAME'] = df9000['DATE'].dt.year
df9000['445_MONTH_NUM'] = df9000['DATE'].dt.month
df9000['445_WEEK_NUM'] = df9000['DATE'].dt.week
df9000 = df9000.groupby(by = [ 'REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM']).agg({'ORDERED' : 'sum'}).reset_index()

df1 = df1.merge(df9000,on=['REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM'], how = "left")
df1 = df1.fillna(0)



df4= pd.read_csv(BASEPATHEXT + '/FACT_ACCIDENT.csv',encoding='windows-1252')
df4 = df4[['INJURY_DATE','CLIENT_ID','ACCIDENT_ID']]


df15 = pd.read_csv(BASEPATHEXT +'/DIM_CLIENT.csv', encoding='utf-8')
df15 = df15[['CLIENT_ID','CLIENT_REP_ID']]
df15.columns = ['CLIENT_ID','REP_ID']

df4 = df4.merge(df15, on = 'CLIENT_ID' )


df4 = df4.groupby(by = ['REP_ID','INJURY_DATE']).agg({'ACCIDENT_ID' : 'nunique'}).reset_index()
df4.columns = ['REP_ID', 'DATE','REPORTED_ACCIDENTS']
df4['DATE'] = pd.to_datetime(df4['DATE'])
df4['445_YEAR_NAME'] = df4['DATE'].dt.year
df4['445_MONTH_NUM'] = df4['DATE'].dt.month
df4['445_WEEK_NUM'] = df4['DATE'].dt.week
df4.drop('DATE', axis=1, inplace=True)



df1 = df1.merge(df4,on=['REP_ID','445_YEAR_NAME','445_MONTH_NUM','445_WEEK_NUM'], how = "left")
df1 = df1.fillna(0)
df1.drop('445_WEEK_NUM', axis=1, inplace=True)
df1.drop('445_MONTH_NUM', axis=1, inplace=True)
df1.drop('445_QUARTER_NUM', axis=1, inplace=True)
df1.drop('445_YEAR_NAME', axis=1, inplace=True)




df1.to_csv(BASEPATHEXT + '/FACT_REP_RANKS.csv', sep=',', encoding='utf-8', index = False)


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_DISPATCH")

#############################################   FACT_DISPATCH ########################################################


path = BASEPATH + '/sisenseassignment'
df4 = appendcsvs(path)

df4 = df4[['ORDER#','JOB#','CLIENT ID','EMP ID','DISPATCH DATE']]

df4.columns = ['ORDER_ID','JOB_ID','CLIENT_ID','EMPLOYEE_ID', 'DISPATCH_DATE']
df4['JOB_ID'] = df4['JOB_ID'].astype(str)
df4['ORDER_ID'] = df4['ORDER_ID'].astype(str)
df4['EMPLOYEE_ID'] = df4['EMPLOYEE_ID'].astype(str)
df4['DISPATCH_ID'] = df4['EMPLOYEE_ID'].astype(str) +  df4['ORDER_ID'].astype(str)
df4['DISPATCH_DATE'] = pd.to_datetime(df4['DISPATCH_DATE'])
df4.sort_values(by=['DISPATCH_ID','DISPATCH_DATE']) 
df4 = df4.drop_duplicates(subset='DISPATCH_ID', keep='first', inplace=False)

df4 = df4.drop_duplicates()



df1=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')


    
df1 = df1[['ORDER_ID','EMPLOYEE_ID','SALES','INVOICE_HOURS','INVOICE_GP']]
df1['ORDER_ID'] = df1['ORDER_ID'].astype(str)
df1['EMPLOYEE_ID'] = df1['EMPLOYEE_ID'].astype(str)
df1 = df1.groupby(by = [ 'ORDER_ID','EMPLOYEE_ID']).agg({'SALES' : 'sum','INVOICE_HOURS' : 'sum','INVOICE_GP' : 'sum'}).reset_index()


df4 = df4.merge(df1, on = [ 'ORDER_ID','EMPLOYEE_ID'], how = 'left')



df4.to_csv(BASEPATHEXT + '/FACT_DISPATCH2.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting VIEW_REP_TERR")

#############################TERRITORY VIEW#######################################################################################
df20=pd.read_csv(BASEPATHEXT + '/DIM_CLIENT.csv',encoding='windows_1252')
df20=df20[['CLIENT_TERRITORY','CLIENT_REP_ID']]
df20 = df20.drop_duplicates(subset='CLIENT_TERRITORY', keep='first', inplace=False)

df20.to_csv(BASEPATHEXT + '/VIEW_REP_TERR.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_TERRITORY_EFFECTIVENESS")

#############################TERRITORY_EFFECTIVENESS#######################################################################################

df2=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df2 = df2[['CLIENT_ID','OFFICE_ID','SALES','INVOICE_WAGES','INVOICE_HOURS','COST_OF_SALES','INVOICE_GP','INVOICE_DATE','SUG_SALES']]
df2 = df2.groupby(by = ['INVOICE_DATE','CLIENT_ID','OFFICE_ID']).agg({'SALES' : 'sum', 'INVOICE_WAGES' : 'sum','INVOICE_HOURS' : 'sum','COST_OF_SALES' : 'sum','INVOICE_GP' : 'sum','SUG_SALES' : 'sum'}).reset_index()
df2['OFFICE_ID'] = df2['OFFICE_ID'].astype(float)


df8=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')
df8['CLIENT ID']= df8['CLIENT ID'].str.upper()
df8 = df8[['CLIENT ID','HOME OFFICE#']]
df8.columns = ['CLIENT_ID','TERRITORY_OFFICE']
df8['TERRITORY_OFFICE'] = df8['TERRITORY_OFFICE'].astype(float)



df9 = df2.merge(df8, on ='CLIENT_ID', how = 'left' )
df9 ['IN_TERRITORY_SALE'] = np.where(df9['OFFICE_ID'] == df9['TERRITORY_OFFICE'],1,0)
df9 ['IN_TERRITORY_SALE_DD'] = np.where(df9['OFFICE_ID'] == df9['TERRITORY_OFFICE'],'IN TERRITORY','OUT OF TERRITORY')
df9.drop('TERRITORY_OFFICE', axis=1, inplace=True)

df9.to_csv(BASEPATHEXT + '/FACT_TERRITORY_EFFECTIVENESS.csv', sep=',', encoding='utf-8', index = False)


print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_ICIMS_WORKFLOW")
#############################################   ICIMS RECRUITING WORKFLOWS  ########################################################

path = BASEPATH + '/icimsftpmirror/Recruiting Workflow' 
filename = max(glob.iglob(path + "/*.csv"), key=os.path.getmtime)


df1 = pd.read_csv(filename, encoding = 'ISO-8859-1')
df1.drop('Person : First Name', axis=1, inplace=True)
df1.drop('Person : Last Name', axis=1, inplace=True)
df1.drop('Offer Amount', axis=1, inplace=True)
df1.drop('Bonus', axis=1, inplace=True)
df1.drop('Offer Date', axis=1, inplace=True)
df1.drop('Proposed Start Date', axis=1, inplace=True)
df1.drop('Offer Expiration', axis=1, inplace=True)
df1.drop('Counter Offer', axis=1, inplace=True)
df1.drop('Accept or Decline Date', axis=1, inplace=True)
df1.drop('PTO Allowance', axis=1, inplace=True)
df1.drop('Relocation Amount', axis=1, inplace=True)
df1.columns = [c.replace('Date Last Placed in Status ', '') for c in df1.columns]




badboylist = ['Client: Submitted to Client',
'Hired (Canada Only): Hired',
'Hired (Office Staff): Hired',
'Incomplete: Incomplete',
'Initial Review: 1st Touch',
'Initial Review: 2nd Touch',
'Initial Review: Contact at a Later Date',
'Initial Review: Decision Pending',
'Initial Review: Interview Scheduled - No Show',
'Initial Review: Interview Scheduled - Office Position',
'Initial Review: Interview Scheduled',
'Initial Review: Interviewed - Not Selected',
'Initial Review: Offer Considered',
'Initial Review: Offer Declined - Office Position',
'Initial Review: Offer Extended',
'Initial Review: Phone Interview - No Show',
'Initial Review: Phone Interview - Not Selected',
'Initial Review: Phone Interview',
'Initial Review: Phone Interview Not Selected - Disqualified',
'Initial Review: Phone Interview Not Selected - Opted Out',
'Initial Review: Resume Requested',
'Initial Review: Reviewed',
'Initial Review: Reviewed; Not Selected',
'Initial Review: Ride Along/AM Interview',
'Initial Review: Scheduled Interview for Local Office',
'Initial Review: Submitted to Local Office',
'Initial Review: Submitted to Order',
'Invite to Apply: Application Not Complete w/in 7 Days',
'Invite to Apply: Interview Completed',
'Invite to Apply: No Show',
'Invite to Apply: Project Coordinator Worksheet',
'Invite to Apply: Send Application - Local',
'Invite to Apply: Send Application - Travelers',
'Onboarding: Available - Onboarding Completed',
'Onboarding: No Show',
'Onboarding: Offer Accepted - Office Position',
'Onboarding: Onboarding Completed',
'Onboarding: Ready to Onboard',
'Onboarding: Remote Hire Verification',
'Onboarding: Start Onboarding (Office Position)',
'Onboarding: Start Onboarding (Tasks)',
'Pre-Hire: Offer Accepted',
'Pre-Hire: Paperwork Audit - Office Position',
'Pre-Hire: Paperwork Audit',
'Send to Accuterm: Debugging',
'Send to Accuterm: Dispatch - Send to Accuterm',
'Send to Accuterm: Office Hire',
'Send to Accuterm: Paper Application',
'Submissions: Agency Submission',
'Submissions: Initial DNQ',
'Submissions: Jobs Portal (Candidate Not Reviewed)',
'Submissions: Office Jobs (Candidates not reviewed)',
'Submissions: Recruiter Tagged']

df6 =  pd.DataFrame([])
for l in badboylist:

    df = df1[['Job Posting : System ID','Person : System ID', 'Job Posting : Posting Title','Source','Source Channel','Source Name', 'Source Person : Full Name: First Last','Source Portal','Source Origin','Source Device',l]]
    df.columns = ['POSTING_ID', 'ICIMS_ID','JOB_POSTING','SOURCE','SOURCE_CHANNEL','SOURCE_NAME','RECRUITER','SOURCE_PORTAL','SOURCE_ORIGIN','SOURCE_DEVICE','CAL_DATE']
    df['WORKFLOW_ITEM'] = l
    df[['WORKFLOW_STAGE','WORKFLOW_TASK']] = df['WORKFLOW_ITEM'].str.split(':',expand=True)
    df.drop('WORKFLOW_ITEM', axis=1, inplace=True)
    df = df.dropna(axis=0, subset=['CAL_DATE'])
    df6 = df.append(df6)



df6 = df6.drop_duplicates()
df6['RECRUITER'] = df6['RECRUITER'] .replace(' ', '.', regex=True)
df6['RECRUITER'] = df6['RECRUITER'].str.upper()
df6['ICIMS_RECRUITER_TYPE'] = np.where(df6['JOB_POSTING'].str[:5] == 'Order', 'CORP', 'FIELD')



df8=pd.read_csv(BASEPATH + '/sisenseofficeemployee/officeemployees.csv',encoding='utf-8')
df8 = df8[['FIRST NAME', 'LAST NAME', 'OFFICE#']]
df8['RECRUITER'] = df8['FIRST NAME'] + '.' + df8['LAST NAME']
df8.drop('FIRST NAME', axis=1, inplace=True)
df8.drop('LAST NAME', axis=1, inplace=True)

df9 = df6.merge(df8, on =['RECRUITER'], how = "left")

df100 = pd.read_csv(BASEPATHEXT + '/wf_HR/EMPTY_ICIMS.csv',encoding='utf-8')

weekday = datetime.datetime.today().weekday()

if weekday == 6:
    df9 = df100
    
if weekday == 0:
    df9 = df100 

df9.to_csv(BASEPATHEXT + '/wf_iCIMS_ARCHIVE/' + datetime.datetime.now().strftime("%Y-%m-%d") +'.csv', sep=',', encoding='utf-8', index = False)

path = BASEPATHEXT + '/wf_iCIMS_ARCHIVE/'
df9 = appendcsvs(path)
df9 = df9.drop_duplicates()


df9.to_csv(BASEPATHEXT + '/FACT_ICIMS_WORKFLOW.csv', sep=',', encoding='utf-8', index = False)

print(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d %H%M%S") + ": Starting FACT_REP_ROADMAP")

#############################FACT_REP_ROADMAP#######################################################################################

server="mssql.ti.local:1435"
user="bhdmq"
password="QueriesR4Fun"
dbname="BullhornDM"

conn = pymssql.connect(server, user, password, dbname, charset="ISO-8859-1")
cursor = conn.cursor()
cursor.execute("""
SELECT 
       CU.customText1				[EMPLOYEE_ID]
	  ,isnull(case when len(UPPER(CC.[customText1])) <5 then '0' else UPPER(CC.[customText1]) end,'0') [CLIENT_ID]
	  ,UPPER([action]) as			[ACTION]
      ,CONVERT(varchar,(note.[dateAdded]),101) as				[ACTION_DATE]
	  ,UPPER([trackTitle])			[TYPE]
	 ,count(distinct note.noteid)							[COUNT]
	 --,1 [COUNT]

  FROM [BULLHORNDM].[dbo].[Note] Note
  Left JOIN [BULLHORNDM].[dbo].[NoteEntity] NE on Note.noteID = NE.noteID
  Left JOIN [BULLHORNDM].[dbo].[CorporateUser] CU on Note.[commentingPersonID] = CU.[userID]
  Left JOIN [BULLHORNDM].[dbo].[ClientCorporation]CC on note.personReferenceID = cc.clientCorporationID
       
  where len(CU.customText1) >1
 

  group by

        CU.customText1				
	  ,case when len(UPPER(CC.[customText1])) <5 then '0' else 	UPPER(CC.[customText1]) end	
      ,UPPER([action]) 
      ,CONVERT(varchar,(note.[dateAdded]),101) 
	  ,UPPER([trackTitle])	
               
""")

rows = cursor.fetchall()



df1 = pd.DataFrame(rows)
df1.columns = [item[0] for item in cursor.description]
df1.columns = ['EMPLOYEE_ID','CLIENT_ID','ACTION', '445_DATE','CONTACT_TYPE', 'COUNT']
df1['445_DATE'] = pd.to_datetime(df1['445_DATE'])

#df1 = df1.drop_duplicates()

df1.to_csv(BASEPATHEXT +'/FACT_BH_NOTES.csv', sep=',', encoding='utf-8', index = False)

conn.close()



df1['YEAR'] = df1['445_DATE'].dt.year
df1['MONTH'] = df1['445_DATE'].dt.month
df1['WEEK'] = df1['445_DATE'].dt.week

df1.drop('CLIENT_ID', axis=1, inplace=True)
df1.drop('445_DATE', axis=1, inplace=True)

df1 = df1.groupby(by = [ 'EMPLOYEE_ID','ACTION','CONTACT_TYPE','YEAR','MONTH','WEEK']).agg({'COUNT' : 'sum'}).reset_index()
df1['ACTION'] = df1['ACTION'].astype(str) +" - "+ df1['CONTACT_TYPE']
df1.drop('CONTACT_TYPE', axis=1, inplace=True)





table = pd.pivot_table(df1, values='COUNT', index=['EMPLOYEE_ID', 'YEAR','MONTH','WEEK'],
                    columns=['ACTION'], aggfunc=np.sum, fill_value=0)


df1 = pd.DataFrame(data = table ).reset_index()
df1['APPOINTMENTS'] = df1['APPOINTMENT - CLIENT']+df1['APPOINTMENT - PROSPECT']
df1.drop('APPOINTMENT - CLIENT', axis=1, inplace=True)
df1.drop('APPOINTMENT - PROSPECT', axis=1, inplace=True)


dfy = pd.read_csv(BASEPATHEXT + '/wf_HR/TIME_DECODER.csv',encoding='windows_1252')
dfy.drop('DATE', axis=1, inplace=True)

df1 = df1.merge(dfy,on=["YEAR" , "MONTH", "WEEK"], how = "left")
df1 = df1[df1['PROGRAM WEEK'].notnull()]

df1 = df1.groupby(by = ['EMPLOYEE_ID','PROGRAM WEEK']).agg({
     'JOBSITE VISIT - CLIENT' : 'sum', 'JOBSITE VISIT - PROSPECT' : 'sum','OFFICE VISIT - CLIENT' : 'sum','OFFICE VISIT - PROSPECT' : 'sum','PHONE CALL - CLIENT' : 'sum','PHONE CALL - PROSPECT' : 'sum','APPOINTMENTS' : 'sum'}).reset_index()



df2=pd.read_csv(BASEPATHEXT + '/FACT_PRODUCTION2.csv',encoding='windows_1252')
df2 = df2[['TERRITORY_REP_ID','INVOICE_DATE','SALES','EMPLOYEE_ID','INVOICE_HOURS','SUG_SALES','INVOICE_GP']]
df2 = df2.groupby(by = ['TERRITORY_REP_ID','INVOICE_DATE']).agg({
      'SALES' : 'sum','EMPLOYEE_ID' : 'nunique','INVOICE_HOURS' : 'sum','SUG_SALES' : 'sum','INVOICE_GP' : 'sum'}).reset_index()

df2.columns = ['EMPLOYEE_ID','445_DATE','SALES','WORKING','HOURS','SUG_SALES','GP']
df2['445_DATE'] = pd.to_datetime(df2['445_DATE'])
df2['YEAR'] = df2['445_DATE'].dt.year
df2['MONTH'] = df2['445_DATE'].dt.month
df2['WEEK'] = df2['445_DATE'].dt.week
df2.drop('445_DATE', axis=1, inplace=True)


df2 = df2.merge(dfy,on=["YEAR" , "MONTH", "WEEK"], how = "left")
#df2 = df2[df2['PROGRAM WEEK'].notnull()]
df2 = df2.drop_duplicates()


df3 = df2.merge(df1,on=["EMPLOYEE_ID", "PROGRAM WEEK"], how = "left")
df3 = df3.fillna(0)





df4=pd.read_csv(BASEPATHEXT + '/DIM_OFFICE_EMPLOYEE7.csv',encoding='windows_1252')
df4=df4[['OFFICE_EMPLOYEE_ID', 'OFFICE_EMPLOYEE_DOH','OFFICE_EMPLOYEE_OFFICE_ID']]
df4['OFFICE_EMPLOYEE_DOH'] = pd.to_datetime(df4['OFFICE_EMPLOYEE_DOH'])
df4['YEAR'] = df4['OFFICE_EMPLOYEE_DOH'].dt.year
df4['MONTH'] = df4['OFFICE_EMPLOYEE_DOH'].dt.month
df4['WEEK'] = df4['OFFICE_EMPLOYEE_DOH'].dt.week
df4 = df4.merge(dfy,on=["YEAR" , "MONTH", "WEEK"], how = "left")
df4['HIRE_WEEK'] = df4['WEEK']
df4.drop('PROGRAM WEEK', axis=1, inplace=True)
df4.drop('YEAR', axis=1, inplace=True)
df4.drop('MONTH', axis=1, inplace=True)
df4.drop('WEEK', axis=1, inplace=True)
df4.drop('OFFICE_EMPLOYEE_DOH', axis=1, inplace=True)

df4.columns = ['EMPLOYEE_ID','OFFICE_ID','HIRE_WEEK']


df4['EMPLOYEE_ID'] = df4['EMPLOYEE_ID'].astype(str)
df4 = df4.dropna()


df5 = df3.merge(df4,on=["EMPLOYEE_ID"], how = "left")
df5['TIME_AXIS'] = df5['PROGRAM WEEK'] - df5['HIRE_WEEK'] + 1
#df5 = df5.dropna()
df5 = df5.drop_duplicates()





df6 = pd.read_csv(BASEPATHEXT + '/wf_HR/SURVIVAL_REP_GOALS.csv',encoding='windows_1252')
df7 = df5.merge(df6,on=["TIME_AXIS"], how = "left")
df7 = df7.fillna(0)


df9000 =  pd.read_csv(BASEPATHEXT + '/FACT_ORDER8.csv',encoding='windows_1252')
df9000 = df9000[['REP_ID','ORDER_ID','ORDER_DATE']]
df9000['ORDER_DATE'] = pd.to_datetime(df9000['ORDER_DATE'])
df9000['YEAR'] = df9000['ORDER_DATE'].dt.year
df9000['MONTH'] = df9000['ORDER_DATE'].dt.month
df9000['WEEK'] = df9000['ORDER_DATE'].dt.week
df9000 = df9000.groupby(by = [ 'REP_ID','YEAR','MONTH','WEEK']).agg({'ORDER_ID' : 'nunique'}).reset_index()
df9000.columns = ['EMPLOYEE_ID','YEAR','MONTH','WEEK','ORDERS']

df7 = df7.merge(df9000,on=["EMPLOYEE_ID",'YEAR','MONTH','WEEK'], how = "left")
df7 = df7.fillna(0)




df8=pd.read_csv(BASEPATH + '/sisenseclient/client.csv',encoding='windows-1252')
df8['CLIENT ID']= df8['CLIENT ID'].str.upper()
df9 = df8[['REGISTRATION REP ID','CLIENT ID','REGISTRATION DATE']]
df9.columns = ['EMPLOYEE_ID','MSA','445_DATE']
df9['EMPLOYEE_ID'] = df9['EMPLOYEE_ID'].astype(str)
df9['EMPLOYEE_ID'] = df9['EMPLOYEE_ID'].map(lambda x: str(x)[:-2])
df9['445_DATE'] = pd.to_datetime(df9['445_DATE'])
df9['YEAR'] = df9['445_DATE'].dt.year
df9['MONTH'] = df9['445_DATE'].dt.month
df9['WEEK'] = df9['445_DATE'].dt.week
df9 = df9.groupby(by = [ 'EMPLOYEE_ID','YEAR','MONTH','WEEK']).agg({'MSA' : 'nunique'}).reset_index()

df9 = df7.merge(df9,on=["EMPLOYEE_ID",'YEAR','MONTH','WEEK'], how = "left")
df9 = df9.fillna(0)



df10 = df8[['REGISTRATION REP ID','CLIENT ID','445 FIRST SALE']]
df10.columns = ['EMPLOYEE_ID','NEW_USER','445_DATE']
df10['EMPLOYEE_ID'] = df10['EMPLOYEE_ID'].astype(str)
df10['EMPLOYEE_ID'] = df10['EMPLOYEE_ID'].map(lambda x: str(x)[:-2])
df10['445_DATE'] = pd.to_datetime(df10['445_DATE'])
df10['YEAR'] = df10['445_DATE'].dt.year
df10['MONTH'] = df10['445_DATE'].dt.month
df10['WEEK'] = df10['445_DATE'].dt.week
df10.drop('445_DATE', axis=1, inplace=True)



df10 = df10.groupby(by = [ 'EMPLOYEE_ID','YEAR','MONTH','WEEK']).agg({'NEW_USER' : 'nunique'}).reset_index()
df10 = df9.merge(df10,on=["EMPLOYEE_ID",'YEAR','MONTH','WEEK'], how = "left")
df10 = df10.fillna(0)



df10.to_csv(BASEPATHEXT + '/FACT_REP_ROADMAP4.csv', sep=',', encoding='utf-8', index = False)


conn = pymssql.connect(server, user, password, dbname)
cursor = conn.cursor()
cursor.execute("""
 SELECT 
      cast([userID]	as varchar)					[LEAD_ID]
      ,CU.customText1				[EMPLOYEE_ID]
	  ,isnull(case when len(UPPER(CC.[customText1])) <5 then '0' else UPPER(CC.[customText1]) end,'0') [CLIENT_ID]
	  ,upper([businessSectorList])	[CLIENT_SECTOR]	
      ,UPPER([action]) as			[ACTION]
      ,CONVERT(varchar,(note.[dateAdded]),101) as				[ACTION_DATE]
	  ,'-'			[TYPE]
      ,CONVERT(varchar,(cc.[dateLastModified]),101) as				[DATE_UPDATED]
      ,'-'				    [COMPANY_TYPE]
	  ,cc.[status]                      [STATUS]
	 ,count(distinct note.noteid)							[COUNT]
	 --,1 [COUNT]

  FROM [BULLHORNDM].[dbo].[Note] Note
  Left JOIN [BULLHORNDM].[dbo].[NoteEntity] NE on Note.noteID = NE.noteID
  Left JOIN [BULLHORNDM].[dbo].[CorporateUser] CU on Note.[commentingPersonID] = CU.[userID]
  Left JOIN [BULLHORNDM].[dbo].[ClientCorporation]CC on note.personReferenceID = cc.clientCorporationID
       
  where len(CU.customText1) >1
        AND cc.customText10 = 'Bx Priority Leads'
		AND cast(note.[dateAdded] as date) >= '7/1/2019'

  group by

        CU.customText1				
	  ,isnull(case when len(UPPER(CC.[customText1])) <5 then '0' else UPPER(CC.[customText1]) end,'0')
	  ,upper([businessSectorList])
      ,UPPER([action]) 
      ,CONVERT(varchar,(note.[dateAdded]),101) 
	  ,cc.[dateAdded]					
      ,cc.[dateLastModified]							   
	  ,cc.[status]
	  ,[userID]
               
               
               
""")

rows = cursor.fetchall()



df200 = pd.DataFrame(rows)
df200.columns = [item[0] for item in cursor.description]

df200.to_csv(BASEPATHEXT +'/FACT_BX_PRIORITY_ACTIONS.csv', sep=',', encoding='utf-8', index = False)



cursor = conn.cursor()
cursor.execute("""
SELECT cast(clientCorporationID  as varchar)        [BULLHORN_ID]
      ,upper([name])				[COMPANY_NAME]
      ,isnull(case when len(UPPER([customText1])) <5 then '0' else UPPER([customText1]) end,'0') [CLIENT_ID]           
      ,upper([businessSectorList])	[CLIENT_SECTOR]												
      ,[customText8]				[TERRITORY]
      ,[dateAdded]					[DATE_ENTERED]
      ,[dateLastModified]			[DATE_UPDATED]
      ,'-'				    [COMPANY_TYPE]
	  ,[status]                     [STATUS]
  FROM [BULLHORNDM].[dbo].[ClientCorporation]
  where  customText10 = 'Bx Priority Leads'
  order by 3 desc
               
""")

rows = cursor.fetchall()


df201 = pd.DataFrame(rows)
df201.columns = [item[0] for item in cursor.description]

df201.to_csv(BASEPATHEXT +'/FACT_BX_PRIORITY_LEADS.csv', sep=',', encoding='utf-8', index = False)


conn.close()

