import gspread
import pandas as pd
import snowflake.connector
from snowflake.connector import connect
import gspread_dataframe as gd
from datetime import datetime, timedelta
import time
import numpy as np
start_time = time.time()

snowflake_pull = connect(
    user='Dave',
    password='Quantum314!',
    account='fva14998.us-east-1'
    )

gc=gspread.service_account()

workflow = gc.open_by_key('1U38UjtKRdtgvjCvLEgceZtzdCDSjYiZOAIzEbVovZa4')
sqtab = workflow.worksheet('SQData')
archivetab = workflow.worksheet('ArchiveData')
staffingtab = workflow.worksheet('Personnel')

workflow.worksheet('PVPDash').update('Q1', 'Off')
#workflow.worksheet('Return').update('B1', 'Off')

time.sleep(10)

sqtab.clear()
archivetab.clear()
staffingtab.clear()

##Import SQ Data

sql_no_slot = ("""

select
    case
        when len(sq.shippingqueuenumber) in (11, 14) then left(sq.shippingqueuenumber, 8)
        when len(sq.shippingqueuenumber) in (12, 15) then left(sq.shippingqueuenumber, 9)
        when len(sq.shippingqueuenumber) in (13, 16) then left(sq.shippingqueuenumber, 10)
        else sq.shippingqueuenumber
            end as queue_number
    , sq.shippingqueuenumber
    , sq.ordercount as order_count
    , sq.productcount as product_count
    , cast (sq.createdat as date) as sq_creation_date
    , sqs.name as status

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq
        inner join "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUESTATUS" as sqs
            on sq.shippingqueuestatusid = sqs.shippingqueuestatusid


where
    sq.createdat >= dateadd(dd, -7, getdate())

union all

select
    concat(bpoq.buylistpurchaseorderqueuenumber, 'POQ')
    , bpoq.buylistpurchaseorderqueuenumber as raw_blo_number
    , bpoq.ordercount
    , bpoq.productcount
    , cast (bpoq.createdat as date)
    , blpoqs.name

from
    "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUE" as bpoq
        inner join "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUESTATUS" as blpoqs
            on blpoqs.buylistpurchaseorderqueuestatusid = bpoq.buylistpurchaseorderqueuestatusid

where
    bpoq.createdat >= dateadd(dd, -7, getdate())

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_no_slot)

results_no_slot = cursor.fetch_pandas_all()

workflow_df = pd.DataFrame()
workflow_df = pd.concat([workflow_df, results_no_slot])
workflow_df.drop(workflow_df.filter(like='Unnamed'), axis=1, inplace=True)
workflow_df.dropna(subset =['QUEUE_NUMBER'], inplace=True)


##Full SQ number
workflow_df["full_sq_number"] = ""
workflow_df.loc[workflow_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'full_sq_number'] = workflow_df['QUEUE_NUMBER']
workflow_df.loc[workflow_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'full_sq_number'] = workflow_df['SHIPPINGQUEUENUMBER']

##Convert SQ number to date
workflow_df["date_part"] = workflow_df['QUEUE_NUMBER'].str[:6]
workflow_df["year"] = workflow_df['date_part'].str[:2]
workflow_df["day"] = workflow_df['date_part'].str[-2:]
workflow_df["month_a"] = workflow_df['date_part'].str[:4]
workflow_df["month"] = workflow_df['month_a'].str[-2:]

workflow_df["sq_date"] = '20' + workflow_df["year"] + '-' + workflow_df["month"] + '-' + workflow_df["day"]

workflow_df['sq_date'] = pd.to_datetime(workflow_df['sq_date'])

##Get SQ number
workflow_df["sq_number"] = workflow_df['QUEUE_NUMBER'].str.split('-').str[1]

##SQ Types
workflow_df["sq_type"] = workflow_df['full_sq_number'].str.split('-').str[1].str[-3:]

workflow_df = workflow_df[['ORDER_COUNT', 'PRODUCT_COUNT', 'QUEUE_NUMBER', 'full_sq_number', 'sq_date', 'sq_number', 'sq_type', 'STATUS']]

gd.set_with_dataframe(sqtab, workflow_df)

##connect to nuway archive
nuway=gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE')
nuway_tab=nuway.worksheet('Data')
nuwaydata = pd.DataFrame(nuway_tab.get_all_values())
nuway_header = nuwaydata.iloc[0]
nuwaydata = nuwaydata[1:]
nuwaydata.columns = nuway_header
nuway_tab_raw = pd.DataFrame()
nuway_df = pd.concat([nuway_tab_raw, nuwaydata])
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)

nuway_df = nuway_df[['Data']]

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]
nuway_df["Flex Run"] = nuway_df['Data'].str.split('|').str[12]
nuway_df["Env Run"] = nuway_df['Data'].str.split('|').str[13]

nuway_df.loc[nuway_df['Task'].str[:3] != 'PVP', 'Punch'] = None
nuway_df.dropna(subset=["Punch"], inplace=True)

##Import Staffing Data
staffingdoc = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068')
staffing = staffingdoc.worksheet('Current Staff')

staffingdata = pd.DataFrame(staffing.get_all_values())
new_header = staffingdata.iloc[0]
staffingdata = staffingdata[1:]
staffingdata.columns = new_header
staffingtab_raw = pd.DataFrame()
staffingtab_df = pd.concat([staffingtab_raw, staffingdata])
staffingtab_df.dropna(subset=["Preferred Name"], inplace=True)
staffingtab_df.drop(staffingtab_df.filter(like='Unnamed'), axis=1, inplace=True)
staffingtab_df = staffingtab_df[['Preferred Name', 'Email', 'Last, First (Formatting)', 'Role', 'Shift Name']]

staffingtab_df = staffingtab_df.sort_values(by=['Last, First (Formatting)'], ascending=True)

gd.set_with_dataframe(staffingtab, staffingtab_df)

staffingtab_df.loc[staffingtab_df['Email'] != '', 'Email'] = staffingtab_df['Email'].str.lower()

##Merge Staffing Data
nuway_df.loc[nuway_df['Puncher'] != '', 'Puncher'] = nuway_df['Puncher'].str.lower()
nuway_df = pd.merge(nuway_df, staffingtab_df, left_on = "Puncher", right_on = "Email")
nuway_df.drop(nuway_df.filter(like='Email'), axis=1, inplace=True)
nuway_df.drop(nuway_df.filter(like='EMAIL'), axis=1, inplace=True)
nuway_df["Combined"] = nuway_df['Punch'].astype(str) + nuway_df['Preferred Name'].astype(str)
nuway_df = nuway_df.drop_duplicates(subset=['Combined'])

##fix sq numbers
nuway_df["queue_date"] = nuway_df['SQ/POQ'].str.split('-').str[0]
nuway_df["queue_number"] = nuway_df['SQ/POQ'].str.split('-').str[1]
nuway_df['queue_number'] = nuway_df['queue_number'].str.zfill(3)
nuway_df['SQ/POQ'] = nuway_df['queue_date'] + '-' + nuway_df['queue_number']

##parse down data
nuway_df['Punch'] = pd.to_datetime(nuway_df['Punch'])

nuway_df.loc[nuway_df.Punch <= (pd.Timestamp(datetime.now()) - timedelta(days = 7)), 'Punch'] = None
nuway_df.dropna(subset=["Punch"], inplace=True)

##write nuway data to sheet

nuway_df = nuway_df[['Punch',  'Task', 'SQ/POQ', 'Preferred Name', 'Location/Cards', 'Flex Run', 'Env Run']]

gd.set_with_dataframe(archivetab, nuway_df)

workflow.worksheet('PVPDash').update('Q1', 'On')
#workflow.worksheet('Return').update('B1', 'On')

print(time.time() - start_time)