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

##Import Staffing Data

staffingdoc = gc.open_by_key('1sBVK5vjiB72JuKePpht2R4vZ4ShxuZKjQreE8FE7068')
staffing = staffingdoc.worksheet('Current Staff')

staffingdata = pd.DataFrame(staffing.get_all_values())
new_header = staffingdata.iloc[0]
staffingdata = staffingdata[1:]
staffingdata.columns = new_header
staffingtab_raw = pd.DataFrame()
staffing_df = pd.concat([staffingtab_raw, staffingdata])
staffing_df.dropna(subset=["Preferred Name"], inplace=True)
staffing_df.drop(staffing_df.filter(like='Unnamed'), axis=1, inplace=True)
staffing_df = staffing_df[['Preferred Name', 'Email', 'NuNuShift Length','Shift Name', 'Start Date', 'Supervisor', 'OPs Lead', 'Last, First (Formatting)', 'Role']]

staffing_df.rename(columns={'Email':'Puncher', 'NuNuShift Length':'Shift Length'}, inplace=True)
staffing_df.loc[staffing_df['Puncher'] != '', 'Puncher'] = staffing_df['Puncher'].str.lower()

##Import Standards
hub=gc.open_by_key('1AtWbOiHUmWRUoNmWaPxSfj1qMYmRBHeUXf87gvXSF6s')
standards=hub.worksheet('DynamicStandardsw/GenTasks')
standards_df = gd.get_as_dataframe(standards)
standards_df.drop(standards_df.filter(like='Unnamed'), axis=1, inplace=True)
standards_df=standards_df.fillna(0)

##Define Size Settings
for i in range(len(standards_df)):
    if standards_df.iloc[i, 0] == 'Size Settings':
        if standards_df.iloc[i, 1] == 'Small':
            small = standards_df.iloc[i, 2]
        elif standards_df.iloc[i, 1] == 'Large':
            large = standards_df.iloc[i, 2]

##Import Archive Data
nuway=gc.open_by_key('1e3XPI4d1n9kI6hENjvLU1lIyLGbZf2-riujAdH_0oFE')
nuway_tab=nuway.worksheet('Archive')

nuway_df = pd.DataFrame(nuway_tab.get_all_values())
nuway_header = nuway_df.iloc[0]
nuway_df = nuway_df[2:]
nuway_df.columns = nuway_header
nuway_df.loc[nuway_df['Data'] == '', 'Data'] = None
nuway_df.dropna(subset=["Data"], inplace=True)
nuway_df.drop(nuway_df.filter(like='Unnamed'), axis=1, inplace=True)
nuway_df = nuway_df[['Data']]

nuway_df["Punch"] = nuway_df['Data'].str.split('|').str[0]
nuway_df["Puncher"] = nuway_df['Data'].str.split('|').str[1]
nuway_df["Task"] = nuway_df['Data'].str.split('|').str[2]
nuway_df["SQ/POQ"] = nuway_df['Data'].str.split('|').str[3]
nuway_df["Location/Cards"] = nuway_df['Data'].str.split('|').str[4]
nuway_df["Extra"] = nuway_df['Data'].str.split('|').str[5]
nuway_df["Missing"] = nuway_df['Data'].str.split('|').str[6]
nuway_df["Similar"] = nuway_df['Data'].str.split('|').str[7]
nuway_df["Unrecorded"] = nuway_df['Data'].str.split('|').str[8]
nuway_df["Other"] = nuway_df['Data'].str.split('|').str[9]
nuway_df["Test"] = nuway_df['Data'].str.split('|').str[10]
nuway_df["Notes"] = nuway_df['Data'].str.split('|').str[11]
nuway_df["Flex Run"] = nuway_df['Data'].str.split('|').str[12]
nuway_df["Env Run"] = nuway_df['Data'].str.split('|').str[13]
nuway_df["Adjustment?"] = nuway_df['Data'].str.split('|').str[14]
nuway_df["Orders Completed"] = nuway_df['Data'].str.split('|').str[15]

nuway_df = nuway_df[['Punch', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Extra', 'Missing', 'Similar', 'Unrecorded', 'Other', 'Test', 'Notes', 'Flex Run', 'Env Run', 'Adjustment?', 'Orders Completed']]

##Import Receiving Data
sql_rec = ("""
select
    ri.reimbursement_invoice_number as ri_number
    , ri.total_product_quantity as number_of_cards

    , ri.processed_by_user_email as processor_email
    , ri.processing_ended_at_et as processing_ended
    , cast(ri.processing_ended_at_et as date) as day_processing_ended
    , ri.active_processing_time_minutes as proc_time_minutes
    , ri.active_verification_time_minutes as ver_time_minutes
    , case
        when ri.verified_by_user_email = processor_email and ver_time_minutes > 5 then 'Flow'
        else 'RI Proc'
        end as ri_tag

from
    "ANALYTICS"."CORE"."REIMBURSEMENT_INVOICES" as ri


where
    ri.processing_ended_at_et >= dateadd(dd, -110, getdate())
    and ri.is_auto = false

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_rec)
rec_df = cursor.fetch_pandas_all()
rec_df.drop(rec_df.filter(like='Unnamed'), axis=1, inplace=True)
rec_df.dropna(subset = ["RI_NUMBER"], inplace=True)

##Define rec standards
rec_standards_df = standards_df.copy()
rec_standards_df.loc[(rec_standards_df.Task != 'RI Proc') & (rec_standards_df.Task != 'Flow'), 'Task'] = None
rec_standards_df.dropna(subset=["Task"], inplace=True)
rec_standards_df["Combined"] = rec_standards_df['Task'].astype(str) + rec_standards_df['Subtask'].astype(str) + rec_standards_df['Size'].astype(str)

##Import RI Tags
tagdoc=gc.open_by_key('1q4LKfb3Yu5LaBvIhWkp5ZVG7yhpsnw4jhxJYNIvj9dA')
tagtab=tagdoc.worksheet('Archive')

tagdata_df = gd.get_as_dataframe(tagtab)
tagdata_df.drop(tagdata_df.filter(like='Unnamed'), axis=1, inplace=True)
tagdata_df.dropna(subset=["RI"], inplace=True)

##Create RI Size column and fill it with data
rec_df["RI_SIZE"] = ""
rec_df.loc[rec_df['NUMBER_OF_CARDS'] <= large, 'RI_SIZE'] = "Medium"
rec_df.loc[rec_df['NUMBER_OF_CARDS'] <= small, 'RI_SIZE'] = "Small"
rec_df.loc[rec_df['NUMBER_OF_CARDS'] > large, 'RI_SIZE'] = "Large"

##RI Types
rec_df = pd.merge(rec_df, tagdata_df, left_on='RI_NUMBER', right_on='RI').drop(['RI'], axis=1)

##Preferred Names
rec_df.loc[rec_df.PROCESSOR_EMAIL != '', 'PROCESSOR_EMAIL'] = rec_df['PROCESSOR_EMAIL'].str.lower()
rec_df = pd.merge(rec_df, staffing_df, left_on = "PROCESSOR_EMAIL", right_on="Puncher")
rec_df = rec_df.drop_duplicates(subset=['RI_NUMBER'])

##Combined Column
rec_df["Combined"] = rec_df['RI_TAG'].astype(str) + rec_df['Tag'].astype(str) + rec_df['RI_SIZE'].astype(str)
rec_df = pd.merge(rec_df, rec_standards_df, left_on='Combined', right_on='Combined')

##Adjusted RI Size (teardown time)
rec_df["adjusted_product_amount"] = ""
rec_df.loc[rec_df['RI_SIZE'] == 'Small', 'adjusted_product_amount'] = (61 / rec_df['Y-Int']) + rec_df['NUMBER_OF_CARDS']
rec_df.loc[rec_df['RI_SIZE'] == 'Medium','adjusted_product_amount'] = (97 / rec_df['Y-Int']) + rec_df['NUMBER_OF_CARDS']
rec_df.loc[rec_df['RI_SIZE'] == 'Large','adjusted_product_amount'] = (209 / rec_df['Y-Int']) + rec_df['NUMBER_OF_CARDS']

##Combine with nuway df
rec_df = rec_df[[
'RI_TAG',
'PROCESSING_ENDED',
'Tag',
'PROCESSOR_EMAIL',
'RI_NUMBER',
'NUMBER_OF_CARDS',
'Y-Int',
'adjusted_product_amount'
]]

rec_df.rename(columns={
'RI_TAG': 'SQ/POQ',
'PROCESSING_ENDED': 'Punch',
'Tag': 'Task',
'PROCESSOR_EMAIL': 'Puncher',
'RI_NUMBER': 'Extra',
'NUMBER_OF_CARDS': 'Location/Cards',
'Y-Int':'Missing',
'adjusted_product_amount': 'Similar'
}, inplace=True)

nuway_df = pd.concat([nuway_df, rec_df])

##Import buylist data
sql_byl = ("""

select
    bo.offer_number as buylist_offer_number
    , bo.expected_product_count as blo_product_count
    , bo.processed_by_user_email as blo_processor_email
    , bo.verified_by_user_email as blo_verifier_email

    , bo.active_processing_started_at_et as blo_processing_at
    , cast(bo.active_processing_started_at_et as date) as day_of_blo_processing

    , bo.active_verification_started_at_et as blo_verifying_at
    , cast(bo.active_verification_started_at_et as date) as day_of_blo_verifying



from
    "ANALYTICS"."CORE"."BUYLIST_OFFERS" as bo
        inner join
            "ANALYTICS"."CORE"."USERS" as u
                on u.id = bo.player_id

where
    bo.active_processing_started_at_et >= dateadd(dd, -100, getdate())

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_byl)
blo_df = cursor.fetch_pandas_all()
blo_df.drop(blo_df.filter(like='Unnamed'), axis=1, inplace=True)
blo_df.dropna(subset = ["BUYLIST_OFFER_NUMBER"], inplace=True)

##BLO Standards
blo_df["BLO_Proc_Standard"] = ""
blo_df["BLO_Ver_Standard"] = ""

for i in range(len(standards_df)):
    if standards_df.iloc[i, 1] == 'BLO Proc':
        blo_proc_standard = standards_df.iloc[i, 4]
    elif standards_df.iloc[i, 1] == 'BLO Ver':
        blo_ver_standard = standards_df.iloc[i, 4]

blo_df.loc[blo_df.BLO_PROCESSING_AT != "", 'BLO_Proc_Standard'] = blo_proc_standard
blo_df.loc[blo_df.BLO_VERIFYING_AT != "", 'BLO_Ver_Standard'] = blo_ver_standard

##Rename Columns, create empty column and task column
blo_df["blo_proc_tag"] = ""
blo_df["blo_ver_tag"] = ""
blo_df["blo"] = ""
blo_df.loc[blo_df['BLO_PROCESSING_AT'] != '', 'blo_proc_tag'] = "BLO Proc"
blo_df.loc[blo_df['BLO_VERIFYING_AT'] != '', 'blo_ver_tag'] = "BLO Ver"

blo_df = blo_df[[
'BLO_PROCESSING_AT',
'BLO_PROCESSOR_EMAIL',
'blo_proc_tag',
'BLO_VERIFYING_AT',
'BLO_VERIFIER_EMAIL',
'blo_ver_tag',
'BUYLIST_OFFER_NUMBER',
'BLO_PRODUCT_COUNT',
'BLO_Proc_Standard',
'BLO_Ver_Standard']]

##Split blo df
blo_df = blo_df.drop_duplicates(subset=['BUYLIST_OFFER_NUMBER'])
blo_proc_df = blo_df.copy()
blo_proc_df.loc[blo_proc_df['blo_proc_tag'] == '', 'BLO_PROCESSING_AT'] = None
blo_proc_df.dropna(subset = ["BLO_PROCESSING_AT"], inplace=True)

blo_proc_df = blo_proc_df[[
'BLO_PROCESSING_AT',
'BLO_PROCESSOR_EMAIL',
'blo_proc_tag',
'BUYLIST_OFFER_NUMBER',
'BLO_PRODUCT_COUNT',
'BLO_Proc_Standard']]

blo_proc_df.rename(columns={
'BLO_PROCESSING_AT':'Punch',
'BLO_PROCESSOR_EMAIL':'Puncher',
'blo_proc_tag': 'Task',
'BUYLIST_OFFER_NUMBER': 'SQ/POQ',
'BLO_PRODUCT_COUNT': 'Location/Cards',
'BLO_Proc_Standard': 'Extra'}, inplace=True)

nuway_df = pd.concat([nuway_df, blo_proc_df])

blo_ver_df = blo_df.copy()
blo_ver_df.loc[blo_ver_df['blo_ver_tag'] == '', 'BLO_VERIFYING_AT'] = None
blo_ver_df.dropna(subset = ["BLO_VERIFYING_AT"], inplace=True)

blo_ver_df = blo_ver_df[[
'BLO_VERIFYING_AT',
'BLO_VERIFIER_EMAIL',
'blo_ver_tag',
'BUYLIST_OFFER_NUMBER',
'BLO_PRODUCT_COUNT',
'BLO_Ver_Standard']]

blo_ver_df.rename(columns={
'BLO_VERIFYING_AT':'Punch',
'BLO_VERIFIER_EMAIL':'Puncher',
'blo_ver_tag': 'Task',
'BUYLIST_OFFER_NUMBER': 'SQ/POQ',
'BLO_PRODUCT_COUNT': 'Location/Cards',
'BLO_Ver_Standard': 'Extra'}, inplace=True)

nuway_df = pd.concat([nuway_df, blo_ver_df])

##Merge Staffing Data
nuway_df['Puncher'] = nuway_df['Puncher'].str.lower()
nuway_df = pd.merge(nuway_df, staffing_df, how='left')
nuway_df["Dupe"] = ""

##Create Error Dataframe
error_df = pd.DataFrame(columns = ['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe'])

##Scrub Archive Data
###Punch Column
nuway_df['Punch'] = nuway_df['Punch'].apply(pd.to_datetime, errors='coerce')
punch_error_df = nuway_df.copy()
punch_error_df = punch_error_df[punch_error_df['Punch'].isna()]
punch_error_df['Punch'] = punch_error_df['Punch'].fillna("Error")
punch_error_df = punch_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, punch_error_df])
nuway_df = nuway_df[nuway_df['Punch'].notnull()]

###Preferred Name Column
pref_name_error_df = nuway_df.copy()
pref_name_error_df = pref_name_error_df[pref_name_error_df['Preferred Name'].isna()]
pref_name_error_df['Preferred Name'] = pref_name_error_df['Preferred Name'].fillna("Error")
pref_name_error_df = pref_name_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pref_name_error_df])
nuway_df = nuway_df[nuway_df['Preferred Name'].notnull()]

###Task Column
task_error_df = nuway_df.copy()
task_error_df.loc[task_error_df['Task'] == '', 'Task'] = None
task_error_df = task_error_df[task_error_df['Task'].isna()]
task_error_df['Task'] = task_error_df['Task'].fillna("Error")
task_error_df = task_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, task_error_df])

nuway_df.loc[nuway_df['Task'] == '', 'Punch'] = None
nuway_df.dropna(subset=["Punch"], inplace=True)

###Duplicates
dupe_errors_df = nuway_df.copy()
dupe_errors_df["Dupe"] = dupe_errors_df['Punch'].astype(str) + dupe_errors_df['Puncher'].astype(str) + dupe_errors_df['Task'].astype(str) + dupe_errors_df['Location/Cards'].astype(str) + dupe_errors_df['SQ/POQ'].astype(str) + dupe_errors_df['Flex Run'].astype(str) + dupe_errors_df['Env Run'].astype(str)
dupe_errors_df = dupe_errors_df[dupe_errors_df.duplicated(subset=['Dupe'], keep=False)]
dupe_errors_df.loc[dupe_errors_df['Dupe'] != '', 'Dupe'] = "Error"
dupe_errors_df = dupe_errors_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, dupe_errors_df])

nuway_df["dupe_errors"] = nuway_df['Punch'].astype(str) + nuway_df['Puncher'].astype(str) + nuway_df['Task'].astype(str) + nuway_df['Location/Cards'].astype(str) + nuway_df['SQ/POQ'].astype(str) + nuway_df['Flex Run'].astype(str) + nuway_df['Env Run'].astype(str)
nuway_df.drop_duplicates(subset=['dupe_errors'], inplace=True)

error_df.sort_values(by=['Punch'], ascending=True, inplace=True)

##First Offset
nuway_df["first_offset"] = ""
nuway_df['Punch'] = pd.to_datetime(nuway_df['Punch'])
nuway_df.loc[(nuway_df.Punch.dt.hour < 12) & (nuway_df['Shift Name'] == 'Shipping Forest'), 'first_offset'] = nuway_df['Punch'] - timedelta(hours = 12)
nuway_df.loc[(nuway_df.Punch.dt.hour >= 12) & (nuway_df['Shift Name'] == 'Shipping Forest'), 'first_offset'] = nuway_df['Punch']
nuway_df.loc[nuway_df['Shift Name'] != 'Shipping Forest', 'first_offset'] = nuway_df['Punch']
nuway_df['first_offset'] = pd.to_datetime(nuway_df['first_offset'])
nuway_df['first_offset'] = nuway_df['first_offset'].dt.date

##Adjusted Shift Lengths
nuway_df["adjusted_shift_length"] = ""
nuway_df['Start Date'] = pd.to_datetime(nuway_df['Start Date']).dt.date
nuway_df['Punch'] = pd.to_datetime(nuway_df['Punch'])

nuway_df.loc[(nuway_df.Punch.dt.date - timedelta(days = 56) < nuway_df['Start Date']) & (nuway_df['Shift Name'] != 'Shipping Forest'), 'adjusted_shift_length'] = 7
nuway_df.loc[(nuway_df.Punch.dt.date - timedelta(days = 56) < nuway_df['Start Date']) & (nuway_df['Shift Name'] == 'Shipping Forest'), 'adjusted_shift_length'] = nuway_df['Shift Length']
nuway_df.loc[nuway_df.Punch.dt.date - timedelta(days = 56) >= nuway_df['Start Date'], 'adjusted_shift_length'] = nuway_df['Shift Length']

nuway_df['adjusted_shift_length'] = pd.to_numeric(nuway_df['adjusted_shift_length'])

##Deal with meal period extensions
nuway_df["mpe"] = ""
nuway_df.loc[nuway_df['SQ/POQ'] == 'Meal Period Extension', 'mpe'] = "Yes"
nuway_df.loc[nuway_df['SQ/POQ'] != 'Meal Period Extension', 'mpe'] = ""
nuway_df["Combined"] = nuway_df['Preferred Name'].astype(str) + nuway_df['first_offset'].astype(str)

nuway_df_mpe = nuway_df.groupby('Combined')['mpe'].max()
nuway_df = pd.merge(nuway_df, nuway_df_mpe, how='right', on='Combined')

nuway_df.loc[nuway_df['mpe_y'] == 'Yes', 'adjusted_shift_length'] = nuway_df['adjusted_shift_length'].astype(int) - 0.5

##Import no slot SQ Data
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

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq

where
    sq.createdat >= dateadd(dd, -115, getdate())

union all

select
    concat(bpoq.buylistpurchaseorderqueuenumber, 'POQ')
    , bpoq.buylistpurchaseorderqueuenumber as raw_blo_number
    , bpoq.ordercount
    , bpoq.productcount

from
    "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUE" as bpoq

where
    bpoq.createdat >= dateadd(dd, -115, getdate())

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_no_slot)

pvp_df = cursor.fetch_pandas_all()
pvp_df.drop(pvp_df.filter(like='Unnamed'), axis=1, inplace=True)
pvp_df.dropna(subset =['QUEUE_NUMBER'], inplace=True)

##Import slot SQ Data
sql_slot = ("""
select
    case
        when len(sq.shippingqueuenumber) in (11, 14) then left(sq.shippingqueuenumber, 8)
        when len(sq.shippingqueuenumber) in (12, 15) then left(sq.shippingqueuenumber, 9)
        when len(sq.shippingqueuenumber) in (13, 16) then left(sq.shippingqueuenumber, 10)
        else sq.shippingqueuenumber
            end as queue_number

    , sq.shippingqueuenumber as shippingqueuenumber

    , case
        when sqps.slot = 'A' then '1'
        when sqps.slot = 'B' then '2'
        when sqps.slot = 'C' then '3'
        when sqps.slot = 'D' then '4'
        when sqps.slot = 'E' then '5'
        when sqps.slot = 'F' then '6'
        when sqps.slot = 'G' then '7'
        when sqps.slot = 'H' then '8'
        when sqps.slot = 'I' then '9'
        when sqps.slot = 'J' then '10'
        when sqps.slot = 'K' then '11'
        when sqps.slot = 'L' then '12'
        when sqps.slot = 'M' then '13'
        when sqps.slot = 'N' then '14'
        when sqps.slot = 'O' then '15'
        when sqps.slot = 'P' then '16'
        when sqps.slot = 'Q' then '17'
        when sqps.slot = 'R' then '18'
        when sqps.slot = 'S' then '19'
        when sqps.slot = 'T' then '20'
        when sqps.slot = 'U' then '21'
        when sqps.slot = 'V' then '22'
        when sqps.slot = 'W' then '23'
        when sqps.slot = 'X' then '24'
        else sqps.slot
            end as slot

    , count(distinct sqps.productconditionid) as unique_pcids
    , sum(sqps.quantity) as quantity

from
    "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUE" as sq
        inner join
            "HVR_TCGSTORE_PRODUCTION"."TCGD"."SHIPPINGQUEUEPULLSHEET" as sqps
                on sq.shippingqueueid = sqps.shippingqueueid

where
    sq.createdat >= dateadd(dd, -115, getdate())
    and left(sq.shippingqueuenumber, 6) <> 'Holdin'

group by
    sq.shippingqueuenumber
    , sqps.slot

union all

select
    concat(blpoq.buylistpurchaseorderqueuenumber, 'POQ')
    , blpoq.buylistpurchaseorderqueuenumber

    , case
        when blpoqps.slot = 'A' then '1'
        when blpoqps.slot = 'B' then '2'
        when blpoqps.slot = 'C' then '3'
        when blpoqps.slot = 'D' then '4'
        when blpoqps.slot = 'E' then '5'
        when blpoqps.slot = 'F' then '6'
        when blpoqps.slot = 'G' then '7'
        when blpoqps.slot = 'H' then '8'
        when blpoqps.slot = 'I' then '9'
        when blpoqps.slot = 'J' then '10'
        when blpoqps.slot = 'K' then '11'
        when blpoqps.slot = 'L' then '12'
        when blpoqps.slot = 'M' then '13'
        when blpoqps.slot = 'N' then '14'
        when blpoqps.slot = 'O' then '15'
        when blpoqps.slot = 'P' then '16'
        when blpoqps.slot = 'Q' then '17'
        when blpoqps.slot = 'R' then '18'
        when blpoqps.slot = 'S' then '19'
        when blpoqps.slot = 'T' then '20'
        when blpoqps.slot = 'U' then '21'
        when blpoqps.slot = 'V' then '22'
        when blpoqps.slot = 'W' then '23'
        when blpoqps.slot = 'X' then '24'
        else blpoqps.slot
            end as slot

    , count(distinct blpoqps.productconditionid) as unique_pcids
    , sum(blpoqps.quantity) as quantity

from
    "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUE" as blpoq
        inner join
            "HVR_TCGSTORE_PRODUCTION"."BYL"."BUYLISTPURCHASEORDERQUEUEPULLSHEET" as blpoqps
                on blpoq.buylistpurchaseorderqueueid = blpoqps.buylistpurchaseorderqueueid

where
    blpoq.createdat >= dateadd(dd, -115, getdate())

group by
    blpoq.buylistpurchaseorderqueuenumber
    , blpoqps.slot

""")

cursor = snowflake_pull.cursor()
cursor.execute(sql_slot)

results_slot = cursor.fetch_pandas_all()

pull_df = pd.DataFrame()
pull_df = pd.concat([pull_df, results_slot])
pull_df.drop(pull_df.filter(like='Unnamed'), axis=1, inplace=True)
pull_df.dropna(subset =['QUEUE_NUMBER'], inplace=True)

pullver_df = pd.DataFrame()
pullver_df = pd.concat([pullver_df, results_slot])
pullver_df.drop(pullver_df.filter(like='Unnamed'), axis=1, inplace=True)
pullver_df.dropna(subset =['QUEUE_NUMBER'], inplace=True)

##Merge NuWay Data with SQ Data, fix sq numbers in datafarmes
nuway_df.rename(columns={'SQ/POQ':'QUEUE_NUMBER'}, inplace=True)

nuway_df["queue_date"] = nuway_df['QUEUE_NUMBER'].str.split('-').str[0]
nuway_df["queue_number"] = nuway_df['QUEUE_NUMBER'].str.split('-').str[1]
nuway_df['queue_number'] = nuway_df['queue_number'].str.zfill(3)

nuway_df.loc[(nuway_df.Task.str[:3] == 'pvp') | (nuway_df.Task.str[:3] == 'PVP') |(nuway_df.Task == 'Pull Verifying') | (nuway_df.Task == 'Pulling'), 'QUEUE_NUMBER'] = nuway_df['queue_date'] + '-' + nuway_df['queue_number']
nuway_df.loc[(nuway_df.Task.str[:3] != 'pvp') | (nuway_df.Task.str[:3] == 'PVP') & (nuway_df.Task != 'Pull Verifying') & (nuway_df.Task != 'Pulling'), 'QUEUE_NUMBER'] = nuway_df['QUEUE_NUMBER']

##Make pvp nuway dataframe, fix sq numbers in dataframes
pvp_nuway_df = nuway_df.copy()
pvp_nuway_df.loc[pvp_nuway_df.Task != '', 'Task'] = pvp_nuway_df.Task.str.upper()
pvp_nuway_df.loc[pvp_nuway_df.Task.str[:3] != 'PVP', 'Punch'] = None
pvp_nuway_df.dropna(subset=["Punch"], inplace=True)

pvp_df["queue_date"] = pvp_df['QUEUE_NUMBER'].str.split('-').str[0]
pvp_df["queue_number"] = pvp_df['QUEUE_NUMBER'].str.split('-').str[1]
pvp_df['queue_number'] = pvp_df['queue_number'].str.zfill(3)
pvp_df['QUEUE_NUMBER'] = pvp_df['queue_date'] + '-' + pvp_df['queue_number']

pvp_df = pd.merge(pvp_df, pvp_nuway_df, how='right')

##Make pull ver nuway dataframe, fix sq numbers in dataframes
pullver_nuway_df = nuway_df.copy()
pullver_nuway_df.loc[pullver_nuway_df.Task.str[:8] != 'Pull Ver', 'Punch'] = None
pullver_nuway_df.dropna(subset=["Punch"], inplace=True)

pullver_df["queue_date"] = pullver_df['QUEUE_NUMBER'].str.split('-').str[0]
pullver_df["queue_number"] = pullver_df['QUEUE_NUMBER'].str.split('-').str[1]
pullver_df['queue_number'] = pullver_df['queue_number'].str.zfill(3)
pullver_df['QUEUE_NUMBER'] = pullver_df['queue_date'] + '-' + pullver_df['queue_number']

pullver_df = pd.merge(pullver_df, pullver_nuway_df, how='right')

##Make pull nuway dataframe, fix sq numbers in dataframes

pull_nuway_df = nuway_df.copy()
pull_nuway_df.loc[pull_nuway_df.Task != 'Pulling', 'Punch'] = None
pull_nuway_df.dropna(subset=["Punch"], inplace=True)

pull_df["queue_date"] = pull_df['QUEUE_NUMBER'].str.split('-').str[0]
pull_df["queue_number"] = pull_df['QUEUE_NUMBER'].str.split('-').str[1]
pull_df['queue_number'] = pull_df['queue_number'].str.zfill(3)
pull_df['QUEUE_NUMBER'] = pull_df['queue_date'] + '-' + pull_df['queue_number']

pull_df = pd.merge(pull_df, pull_nuway_df, how='right')

##Full SQ Number Column
pvp_df["pvp_full_sq_number"] = ""
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'pvp_full_sq_number'] = pvp_df['QUEUE_NUMBER']
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'pvp_full_sq_number'] = pvp_df['SHIPPINGQUEUENUMBER']

pullver_df["pullver_full_sq_number"] = ""
pullver_df.loc[pullver_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'pullver_full_sq_number'] = pullver_df['QUEUE_NUMBER']
pullver_df.loc[pullver_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'pullver_full_sq_number'] = pullver_df['SHIPPINGQUEUENUMBER']

pull_df["pull_full_sq_number"] = ""
pull_df.loc[pull_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'pull_full_sq_number'] = pull_df['QUEUE_NUMBER']
pull_df.loc[pull_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'pull_full_sq_number'] = pull_df['SHIPPINGQUEUENUMBER']

###Gen Tasks
##Merge Standards with gen tasks df
gen_df = nuway_df.copy()
gen_df.loc[gen_df.Task != 'General Tasks', 'Punch'] = None
gen_df.dropna(subset=["Punch"], inplace=True)

gen_standards_df = standards_df.copy()
gen_standards_df.loc[gen_standards_df.Task != 'General Tasks', 'Task'] = None
gen_standards_df.dropna(subset=["Task"], inplace=True)

gen_df = pd.merge(gen_df, gen_standards_df, left_on='QUEUE_NUMBER', right_on='Subtask')

##Calculate Day %
gen_df["gen_credit"] = ""

gen_df['Minutes Credit'] = gen_df['Minutes Credit'].apply(pd.to_numeric, errors='coerce')
gen_df['Minutes Credit'] = gen_df['Minutes Credit'].fillna('None')

gen_df.loc[gen_df['Location/Cards'] != None, 'gen_credit'] = gen_df['Location/Cards']
gen_df.loc[gen_df['Minutes Credit'] != 'None', 'gen_credit'] = gen_df['Minutes Credit']

gen_df.rename(columns={'adjusted_shift_length':'gen_adjusted_shift_length', 'first_offset':'gen_first_offset'}, inplace=True)

gen_df['gen_credit'] = pd.to_numeric(gen_df['gen_credit'])
gen_df['gen_adjusted_shift_length'] = pd.to_numeric(gen_df['gen_adjusted_shift_length'])

gen_df["gen_day_%"] = (gen_df['gen_credit'] / 60) / gen_df['gen_adjusted_shift_length']
gen_df['gen_day_%'] = gen_df['gen_day_%'].round(4)

gen_df["gen_earned_hours"] = ((gen_df['gen_adjusted_shift_length'])/24) * gen_df['gen_day_%']
gen_df['gen_earned_hours'] = gen_df['gen_earned_hours'].round(6)

gen_df["Blank"] = ""
gen_df["Tag"] = ""
gen_df.loc[gen_df.Punch != '', 'Tag'] = "Gen"
gen_df["1"] = ""
gen_df["2"] = ""
gen_df["3"] = ""
gen_df["4"] = ""
gen_df["5"] = ""
gen_df["6"] = ""
gen_df["7"] = ""

gen_df = gen_df[[
'Punch', 'gen_first_offset',
'Preferred Name',
'Location/Cards',
'QUEUE_NUMBER',
'gen_day_%',
'gen_earned_hours',
'Test', 'Tag',
'gen_adjusted_shift_length',
'Notes', '1', '2', '3', '4', '5', '6', '7',
'Blank']]

gen_df.rename(columns={
'gen_first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'QUEUE_NUMBER':'Subtask',
'gen_day_%':'Day_%',
'gen_earned_hours':'Earned_Hours',
'gen_adjusted_shift_length':'Adjusted_Shift_Length'}, inplace=True)

###PVP
##Remove punches with bad sq number data
pvp_sq_error_df = pvp_df.copy()
pvp_sq_error_df = pvp_sq_error_df[pvp_sq_error_df['pvp_full_sq_number'].isna()]
pvp_sq_error_df['pvp_full_sq_number'] = pvp_sq_error_df['pvp_full_sq_number'].fillna("Error")
pvp_sq_error_df.rename(columns={'pvp_full_sq_number':'SQ/POQ'}, inplace=True)
pvp_sq_error_df = pvp_sq_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pvp_sq_error_df])

pvp_df.dropna(subset=["pvp_full_sq_number"], inplace=True)

##Remove takeovers with no orders completed
pvp_df['Orders Completed'] = pvp_df['Orders Completed'].apply(pd.to_numeric, errors='coerce')
pvp_orders_error_df = pvp_df.copy()
pvp_orders_error_df.loc[pvp_orders_error_df['Task'] != 'PVP TAKEOVER', 'Punch'] = None
pvp_orders_error_df.dropna(subset=["Punch"], inplace=True)
pvp_orders_error_df = pvp_orders_error_df[pvp_orders_error_df['Orders Completed'].isna()]
pvp_orders_error_df['Orders Completed'] = pvp_orders_error_df['Orders Completed'].fillna("Error")
pvp_orders_error_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
pvp_orders_error_df = pvp_orders_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pvp_orders_error_df])

pvp_df.loc[(pvp_df['Orders Completed'] == '') & (pvp_df['Task'] == 'PVP TAKEOVER'), 'Punch'] = None

pvp_df.dropna(subset=["Punch"], inplace=True)

##Remove SQs with no order count
pvp_df['ORDER_COUNT'] = pvp_df['ORDER_COUNT'].apply(pd.to_numeric, errors='coerce')
pvp_sq_orders_error_df = pvp_df.copy()
pvp_sq_orders_error_df = pvp_sq_orders_error_df[pvp_sq_orders_error_df['ORDER_COUNT'].isna()]
pvp_sq_orders_error_df['ORDER_COUNT'] = pvp_sq_orders_error_df['ORDER_COUNT'].fillna("Error")
pvp_sq_orders_error_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
pvp_sq_orders_error_df = pvp_sq_orders_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pvp_sq_orders_error_df])

pvp_df.dropna(subset=["ORDER_COUNT"], inplace=True)

##PVP Takeover Ratio
pvp_df["pvp_ratio"] = ""
pvp_df['Orders Completed'] = pd.to_numeric(pvp_df['Orders Completed'])
pvp_df['ORDER_COUNT'] = pd.to_numeric(pvp_df['ORDER_COUNT'])

pvp_df.loc[(pvp_df.Task == 'PVP TAKEOVER') & (pd.isnull(pvp_df['Orders Completed']) == True), 'Orders Completed'] = pvp_df['ORDER_COUNT'] / 2
pvp_df.loc[pvp_df.Task == 'PVP TAKEOVER', 'pvp_ratio'] = pvp_df['Orders Completed'] / pvp_df['ORDER_COUNT']
pvp_df.loc[pvp_df.Task != 'PVP TAKEOVER', 'pvp_ratio'] = 1

pvp_df["pvp_cards"] = pvp_df['PRODUCT_COUNT'].astype('int64') * pvp_df['pvp_ratio'].astype('float64')

##PVP Order Count
pvp_df["Tag"] = ""
pvp_df["6"] = ""
pvp_df.loc[pvp_df.Task == 'PVP TAKEOVER', '6'] = pvp_df['Orders Completed']
pvp_df.loc[pvp_df.Task != 'PVP TAKEOVER', '6'] = pvp_df['ORDER_COUNT']
pvp_df.loc[(pvp_df.Task != '') & (pvp_df.Task != 'PVP TAKEOVER'), 'Tag'] = "PVP"
pvp_df.loc[(pvp_df.Task != '') & (pvp_df.Task == 'PVP TAKEOVER'), 'Tag'] = "PVP Takeover"

##Define PVP standards
pvp_standards_df = standards_df.copy()
pvp_standards_df.loc[pvp_standards_df.Task != 'PVP', 'Task'] = None
pvp_standards_df.dropna(subset=["Task"], inplace=True)

##Merge Standards with Dataframe
pvp_df["sq_type"] = ""
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'sq_type'] = 'POQ'
pvp_df.loc[pvp_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'sq_type'] = pvp_df['SHIPPINGQUEUENUMBER'].str[-3:]

pvp_df = pd.merge(pvp_df, pvp_standards_df, left_on='sq_type', right_on='Subtask')

##Calculate PVP Metrics
pvp_df['Orders Coefficient'] = pvp_df['Orders Coefficient'].apply(pd.to_numeric, errors='coerce')
pvp_df['pvp_cards'] = pvp_df['pvp_cards'].apply(pd.to_numeric, errors='coerce')
pvp_df.rename(columns={'first_offset':'First_Offset'}, inplace=True)

pvp_df["pvp_day_%"] = (pvp_df['pvp_cards']) / ((3600 / ((pvp_df['ORDER_COUNT'] * pvp_df['Orders Coefficient']) + pvp_df['Y-Int'])) * pvp_df['adjusted_shift_length'])
pvp_df['pvp_day_%'] = pvp_df['pvp_day_%'].round(4)

pvp_df["pvp_earned_hours"] = ((pvp_df['adjusted_shift_length'])/24) * pvp_df['pvp_day_%']
pvp_df['pvp_earned_hours'] = pvp_df['pvp_earned_hours'].round(6)

##Create final dataframe
pvp_df = pvp_df[[
'Punch',
'First_Offset',
'Preferred Name',
'pvp_cards',
'pvp_full_sq_number',
'pvp_day_%',
'pvp_earned_hours',
'Test',
'Tag',
'adjusted_shift_length',
'Notes',
'Extra',
'Missing',
'Similar',
'Unrecorded',
'Other',
'6']]

pvp_df.rename(columns={
'Preferred Name':'Puncher',
'pvp_cards':'Units',
'pvp_full_sq_number':'Subtask',
'pvp_day_%':'Day_%',
'pvp_earned_hours':'Earned_Hours',
'adjusted_shift_length':'Adjusted_Shift_Length',
'Extra':'1',
'Missing':'2',
'Similar':'3',
'Unrecorded':'4',
'Other':'5'
}, inplace=True)

###PullVer
##Remove punches with bad sq number data and punches missing slot data
pullver_df.loc[pullver_df['Flex Run'] == '', 'Flex Run'] = None
pullver_df.loc[pullver_df['Env Run'] == '', 'Env Run'] = None

pullver_sq_error_df = pullver_df.copy()

pullver_sq_error_df  = pullver_sq_error_df[(pullver_sq_error_df['pullver_full_sq_number'].isna()) | (pullver_sq_error_df['Flex Run'].isna()) | (pullver_sq_error_df['Env Run'].isna())]

pullver_sq_error_df['pullver_full_sq_number'] = pullver_sq_error_df['pullver_full_sq_number'].fillna("Error")
pullver_sq_error_df['Flex Run'] = pullver_sq_error_df['Flex Run'].fillna("Error")
pullver_sq_error_df['Env Run'] = pullver_sq_error_df['Env Run'].fillna("Error")

pullver_sq_error_df.rename(columns={'pullver_full_sq_number':'SQ/POQ'}, inplace=True)

pullver_sq_error_df = pullver_sq_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pullver_sq_error_df])

pullver_df.dropna(subset=["pullver_full_sq_number"], inplace=True)
pullver_df.dropna(subset=["Flex Run"], inplace=True)
pullver_df.dropna(subset=["Env Run"], inplace=True)

##Deal with slots
pullver_df.loc[(pullver_df['Flex Run'].str.isalpha()!= True) | (pullver_df['Flex Run'].str.len() != 1), 'Punch'] = None
pullver_df.loc[(pullver_df['Env Run'].str.isalpha()!= True) | (pullver_df['Env Run'].str.len() != 1), 'Punch'] = None
pullver_df.dropna(subset = ["Punch"], inplace=True)

pullver_df['Flex Run'] = pullver_df['Flex Run'].str.lower()
pullver_df['Env Run'] = pullver_df['Env Run'].str.lower()

pullver_df.loc[pullver_df['Flex Run'] != '', 'Flex Run'] = pullver_df['Flex Run'].str.strip().apply(ord) - 96
pullver_df.loc[pullver_df['Env Run'] != '', 'Env Run'] = pullver_df['Env Run'].str.strip().apply(ord) - 96

##Remove slots/pcids not verified and aggregate
pullver_df["verified_quantity"] = ""

pullver_df['SLOT'] = pullver_df['SLOT'].apply(pd.to_numeric, errors='coerce')

pullver_df.loc[(pullver_df.SLOT >= pullver_df['Flex Run']) & (pullver_df.SLOT <= pullver_df['Env Run']), 'verified_quantity'] = pullver_df['QUANTITY']

pullver_df['verified_quantity'] = pullver_df['verified_quantity'].apply(pd.to_numeric, errors='coerce')

pullver_df["pullver_combined"] = pullver_df['Punch'].astype(str) + pullver_df['Preferred Name'].astype(str)

verified_quantity_agg = pullver_df.groupby('pullver_combined')['verified_quantity'].sum()
pullver_df = pd.merge(pullver_df, verified_quantity_agg, how='right', on='pullver_combined')

pullver_df = pullver_df.drop_duplicates(subset=['pullver_combined'])

##Sum pull ver discrepancies
pullver_df['Extra'] = pullver_df['Extra'].apply(pd.to_numeric, errors='coerce')
pullver_df['Missing'] = pullver_df['Missing'].apply(pd.to_numeric, errors='coerce')
pullver_df['Similar'] = pullver_df['Similar'].apply(pd.to_numeric, errors='coerce')
pullver_df['Unrecorded'] = pullver_df['Unrecorded'].apply(pd.to_numeric, errors='coerce')
pullver_df['Other'] = pullver_df['Other'].apply(pd.to_numeric, errors='coerce')

pullver_df.loc[pullver_df['Extra'] == '', 'Extra'] = 0
pullver_df.loc[pullver_df['Missing'] == '', 'Missing'] = 0
pullver_df.loc[pullver_df['Similar'] == '', 'Similar'] = 0
pullver_df.loc[pullver_df['Unrecorded'] == '', 'Unrecorded'] = 0
pullver_df.loc[pullver_df['Other'] == '', 'Other'] = 0

pullver_df['Extra'] = pd.to_numeric(pullver_df['Extra'])
pullver_df['Missing'] = pd.to_numeric(pullver_df['Missing'])
pullver_df['Similar'] = pd.to_numeric(pullver_df['Similar'])
pullver_df['Unrecorded'] = pd.to_numeric(pullver_df['Unrecorded'])
pullver_df['Other'] = pd.to_numeric(pullver_df['Other'])

pullver_df["total_discreps"] = pullver_df['Extra'] + pullver_df['Missing'] + pullver_df['Similar'] + pullver_df['Unrecorded'] + pullver_df['Other']

##Define pull ver standards
pullver_standards_df = standards_df.copy()
pullver_standards_df.loc[pullver_standards_df.Task != 'Pull Ver', 'Task'] = None
pullver_standards_df.dropna(subset=["Task"], inplace=True)

##Merge pull ver standards with dataframe
pullver_df.loc[pullver_df['Task'].str[:8] == 'Pull Ver', 'Task'] = 'Pull Ver'

pullver_df = pd.merge(pullver_df, pullver_standards_df, left_on = "Task", right_on = "Task")

##Calculate Pull Ver Metrics
pullver_df['Discrepancies Coefficient'] = pullver_df['Discrepancies Coefficient'].apply(pd.to_numeric, errors='coerce')
pullver_df['Y-Int'] = pullver_df['Y-Int'].apply(pd.to_numeric, errors='coerce')
pullver_df['verified_quantity_y'] = pullver_df['verified_quantity_y'].apply(pd.to_numeric, errors='coerce')

pullver_df.rename(columns={'adjusted_shift_length':'pullver_adjusted_shift_length', 'first_offset':'pullver_first_offset'}, inplace=True)
pullver_df['pullver_adjusted_shift_length'] = pullver_df['pullver_adjusted_shift_length'].apply(pd.to_numeric, errors='coerce')

pullver_df["pullver_day_%"] = pullver_df['verified_quantity_y'] / ((3600 / ((pullver_df['total_discreps'] * pullver_df['Discrepancies Coefficient']) + pullver_df['Y-Int'])) * pullver_df['pullver_adjusted_shift_length'])
pullver_df['pullver_day_%'] = pullver_df['pullver_day_%'].round(4)

pullver_df["pullver_earned_hours"] = ((pullver_df['pullver_adjusted_shift_length'])/24) * pullver_df['pullver_day_%']
pullver_df['pullver_earned_hours'] = pullver_df['pullver_earned_hours'].round(6)

##Rename Columns, create empty column and task column
pullver_df["Blank"] = ""
pullver_df["Tag"] = ""
pullver_df.loc[pullver_df.Punch != '', 'Tag'] = "Pull Ver"

##Write final dataframe
pullver_df = pullver_df[[
'Punch',
'pullver_first_offset',
'Preferred Name',
'verified_quantity_y',
'pullver_full_sq_number',
'pullver_day_%',
'pullver_earned_hours',
'Test',
'Tag',
'pullver_adjusted_shift_length',
'Notes',
'Extra',
'Missing',
'Similar',
'Unrecorded',
'Other',
'Flex Run',
'Env Run',
'Blank']]

pullver_df.rename(columns={
'pullver_first_offset':'First_Offset',
'Preferred Name':'Puncher',
'verified_quantity_y':'Units',
'pullver_full_sq_number':'Subtask',
'pullver_day_%':'Day_%',
'pullver_earned_hours':'Earned_Hours',
'pullver_adjusted_shift_length': 'Adjusted_Shift_Length',
'Extra':'1',
'Missing':'2',
'Similar':'3',
'Unrecorded':'4',
'Other':'5',
'Flex Run':'6',
'Env Run':'7'},
inplace=True)

###Pull
##Remove punches with bad sq number data and punches missing slot data
pull_df.loc[pull_df['Flex Run'] == '', 'Flex Run'] = None
pull_df.loc[pull_df['Env Run'] == '', 'Env Run'] = None

pull_sq_error_df = pull_df.copy()

pull_sq_error_df = pull_sq_error_df[(pull_sq_error_df['pull_full_sq_number'].isna()) | (pull_sq_error_df['Flex Run'].isna()) | (pull_sq_error_df['Env Run'].isna())]
pull_sq_error_df['pull_full_sq_number'] = pull_sq_error_df['pull_full_sq_number'].fillna("Error")
pull_sq_error_df['Flex Run'] = pull_sq_error_df['Flex Run'].fillna("Error")
pull_sq_error_df['Env Run'] = pull_sq_error_df['Env Run'].fillna("Error")
pull_sq_error_df.rename(columns={'pull_full_sq_number':'SQ/POQ'}, inplace=True)

pull_sq_error_df["Combined"] = pull_sq_error_df['Punch'].astype(str) + pull_sq_error_df['Preferred Name'].astype(str)
pull_sq_error_df.drop_duplicates(subset=['Combined'], inplace=True)

pull_sq_error_df = pull_sq_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, pull_sq_error_df])

pull_df.dropna(subset=["pull_full_sq_number"], inplace=True)
pull_df.dropna(subset=["Flex Run"], inplace=True)
pull_df.dropna(subset=["Env Run"], inplace=True)

##Deal with slots
pull_df.loc[(pull_df['Flex Run'].str.isalpha()!= True) | (pull_df['Flex Run'].str.len() != 1), 'Punch'] = ""
pull_df.loc[(pull_df['Env Run'].str.isalpha()!= True) | (pull_df['Env Run'].str.len() != 1), 'Punch'] = ""
pull_df.dropna(subset = ["Punch"], inplace=True)

pull_df['Flex Run'] = pull_df['Flex Run'].str.lower()
pull_df['Env Run'] = pull_df['Env Run'].str.lower()

pull_df.loc[pull_df['Flex Run'] != '', 'Flex Run'] = pull_df['Flex Run'].str.strip().apply(ord) - 96
pull_df.loc[pull_df['Env Run'] != '', 'Env Run'] = pull_df['Env Run'].str.strip().apply(ord) - 96

##Remove slots/pcids not pulled and aggregate
pull_df["pulled_quantity"] = ""
pull_df["pulled_pcids"] = ""

pull_df['SLOT'] = pull_df['SLOT'].apply(pd.to_numeric, errors='coerce')
pull_df.loc[pull_df['SLOT'] == '', 'Punch'] = None
pull_df.dropna(subset = ["Punch"], inplace=True)

pull_df.loc[(pull_df.SLOT >= pull_df['Flex Run']) & (pull_df.SLOT <= pull_df['Env Run']), 'pulled_quantity'] = pull_df['QUANTITY']
pull_df.loc[(pull_df.SLOT >= pull_df['Flex Run']) & (pull_df.SLOT <= pull_df['Env Run']), 'pulled_pcids'] = pull_df['UNIQUE_PCIDS']

pull_df['pulled_quantity'] = pull_df['pulled_quantity'].apply(pd.to_numeric, errors='coerce')
pull_df.loc[pull_df['pulled_quantity'] == '', 'Punch'] = None
pull_df.dropna(subset = ["Punch"], inplace=True)

pull_df['pulled_pcids'] = pull_df['pulled_pcids'].apply(pd.to_numeric, errors='coerce')
pull_df.loc[pull_df['pulled_pcids'] == '', 'Punch'] = None
pull_df.dropna(subset = ["Punch"], inplace=True)

pull_df["pull_combined"] = pull_df['Punch'].astype(str) + pull_df['Preferred Name'].astype(str)

pulled_quantity_agg = pull_df.groupby('pull_combined')['pulled_quantity'].sum()
pull_df = pd.merge(pull_df, pulled_quantity_agg, how='right', on='pull_combined')

pulled_pcids_agg = pull_df.groupby('pull_combined')['pulled_pcids'].sum()
pull_df = pd.merge(pull_df, pulled_pcids_agg, how='right', on='pull_combined')

pull_df = pull_df.drop_duplicates(subset=['pull_combined'])

pull_df.rename(columns={'pulled_quantity_y': 'pulled_quantity', 'pulled_pcids_y': 'pulled_pcids'}, inplace=True)

##Calculate density of cards pulled
pull_df["pulled_density"] = pull_df['pulled_quantity'] / pull_df['pulled_pcids']

##Define pull standards
pull_standards_df = standards_df.copy()
pull_standards_df.loc[pull_standards_df.Task != 'Pull', 'Task'] = None
pull_standards_df.dropna(subset=["Task"], inplace=True)

##Merge Standards with Dataframe
pull_df["sq_type"] = ""
pull_df.loc[pull_df['QUEUE_NUMBER'].str[-3:] == 'POQ', 'sq_type'] = 'POQ'
pull_df.loc[pull_df['QUEUE_NUMBER'].str[-3:] != 'POQ', 'sq_type'] = pull_df['pull_full_sq_number'].str[-3:]

pull_df = pd.merge(pull_df, pull_standards_df, left_on = "sq_type", right_on = "Subtask")

##Calculate Metrics
pull_df.rename(columns={'adjusted_shift_length':'pull_adjusted_shift_length', 'first_offset':'pull_first_offset'}, inplace=True)

pull_df["pull_day_%"] = pull_df['pulled_quantity'] / (((pull_df['pulled_density'] * pull_df['Density Coefficient']) + pull_df['Y-Int']) * pull_df['pull_adjusted_shift_length'])
pull_df['pull_day_%'] = pull_df['pull_day_%'].round(4)

pull_df["pull_earned_hours"] = ((pull_df['pull_adjusted_shift_length'])/24) * pull_df['pull_day_%']
pull_df['pull_earned_hours'] = pull_df['pull_earned_hours'].round(6)

##Rename Columns, create empty column and task column
pull_df["Tag"] = ""
pull_df.loc[pull_df.Punch != '', 'Tag'] = "Pull"

##Write final dataframe
pull_df = pull_df[[
'Punch',
'pull_first_offset',
'Preferred Name',
'pulled_quantity',
'pull_full_sq_number',
'pull_day_%',
'pull_earned_hours',
'Test',
'Tag',
'pull_adjusted_shift_length',
'Notes',
'Flex Run',
'Env Run']]

pull_df.rename(columns={
'pull_first_offset':'First_Offset',
'Preferred Name':'Puncher',
'pulled_quantity':'Units',
'pull_full_sq_number':'Subtask',
'pull_day_%':'Day_%',
'pull_earned_hours':'Earned_Hours',
'pull_adjusted_shift_length':'Adjusted_Shift_Length',
'Flex Run':'1',
'Env Run':'2'},
inplace=True)

###Filing
##Build filing dataframe
filing_df = nuway_df.copy()
filing_df.loc[filing_df.Task != 'Filing', 'Punch'] = None
filing_df.dropna(subset=["Punch"], inplace=True)

##Build filing standards dataframe
filing_standards_df = standards_df.copy()
filing_standards_df.loc[filing_standards_df.Task != 'Filing', 'Task'] = None
filing_standards_df.dropna(subset=["Task"], inplace=True)
filing_standards_df["Combined"] = filing_standards_df['Subtask'].map(str) + filing_standards_df['Size'].map(str)
filing_standards_df = filing_standards_df[['Subtask', 'Size', 'Combined', 'Y-Int', 'Cards Coefficient']]

##Scrub filing data
filing_df['Location/Cards'] = filing_df['Location/Cards'].apply(pd.to_numeric, errors='coerce')
filing_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
filing_cards_error_df = filing_df.copy()
filing_cards_error_df = filing_cards_error_df[filing_cards_error_df['Location/Cards'].isna()]
filing_cards_error_df['Location/Cards'] = filing_cards_error_df['Location/Cards'].fillna("Error")
filing_cards_error_df = filing_cards_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, filing_cards_error_df])

filing_df.dropna(subset=["Location/Cards"], inplace=True)

##Define sizes for RI filing runs
filing_df["ri_size"] = ""

filing_df.loc[(filing_df['Location/Cards'] <= large) & (filing_df['SQ/POQ'] == 'RIs'), 'ri_size'] = "Medium"
filing_df.loc[(filing_df['Location/Cards'] <= small) & (filing_df['SQ/POQ'] == 'RIs'), 'ri_size'] = "Small"
filing_df.loc[(filing_df['Location/Cards'] > large) & (filing_df['SQ/POQ'] == 'RIs'), 'ri_size'] = "Large"
filing_df.loc[filing_df['SQ/POQ'] != 'RIs', 'ri_size'] = "-"

filing_df["Combined"] = filing_df['SQ/POQ'].map(str) + filing_df['ri_size'].map(str)

##Merge Standards with Dataframe

filing_df = pd.merge(filing_df, filing_standards_df, left_on = "Combined", right_on = "Combined")

##Calculate Metrics
filing_df['Y-Int'] = pd.to_numeric(filing_df['Y-Int'])
filing_df['Cards Coefficient'] = filing_df['Cards Coefficient'].apply(pd.to_numeric, errors='coerce')

filing_df["filing_day_%"] = filing_df['Location/Cards'] / (filing_df['adjusted_shift_length'] * (filing_df['Y-Int'] + (filing_df['Location/Cards'] * filing_df['Cards Coefficient'])))
filing_df['filing_day_%'] = filing_df['filing_day_%'].round(4)

filing_df["filing_earned_hours"] = ((filing_df['adjusted_shift_length'])/24) * filing_df['filing_day_%']
filing_df['filing_earned_hours'] = filing_df['filing_earned_hours'].round(6)

##Create empty column and task column
filing_df["Tag"] = ""
filing_df.loc[filing_df.Punch != '', 'Tag'] = "Filing"

##Create final dataframe
filing_df = filing_df[[
'Punch',
'first_offset',
'Preferred Name',
'SQ/POQ',
'Location/Cards',
'filing_day_%',
'filing_earned_hours',
'Test',
'Tag',
'adjusted_shift_length',
'Notes']]

filing_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'SQ/POQ':'Subtask',
'filing_day_%':'Day_%',
'filing_earned_hours':'Earned_Hours',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

###Sort
##Build sort dataframe
sort_df = nuway_df.copy()
sort_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
sort_df.loc[sort_df.Task != 'Sort', 'Punch'] = None
sort_df.dropna(subset=["Punch"], inplace=True)

##Build Sort standards dataframe
sort_standards_df = standards_df.copy()
sort_standards_df.drop(sort_standards_df.filter(like='Unnamed'), axis=1, inplace=True)
sort_standards_df.loc[sort_standards_df.Task != 'Sort', 'Task'] = None
sort_standards_df.dropna(subset=["Task"], inplace=True)
sort_standards_df=sort_standards_df.fillna(0)

##Scrub Sort data
sort_df['Location/Cards'] = sort_df['Location/Cards'].apply(pd.to_numeric, errors='coerce')
sort_cards_error_df = sort_df.copy()
sort_cards_error_df = sort_cards_error_df[sort_cards_error_df['Location/Cards'].isna()]
sort_cards_error_df['Location/Cards'] = sort_cards_error_df['Location/Cards'].fillna("Error")
sort_cards_error_df = sort_cards_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, sort_cards_error_df])

sort_df.dropna(subset=["Location/Cards"], inplace=True)

##Merge Standards with Dataframe
sort_df.loc[sort_df['SQ/POQ'] == 'ROCA Operator', 'SQ/POQ'] = 'Roca Operator'

sort_df = pd.merge(sort_df, sort_standards_df, left_on = "SQ/POQ", right_on = "Subtask")

##Calculate Metrics
sort_df['Y-Int'] = sort_df['Y-Int'].apply(pd.to_numeric, errors='coerce')
sort_df['Cards Coefficient'] = sort_df['Cards Coefficient'].apply(pd.to_numeric, errors='coerce')

sort_df["sort_day_%"] = sort_df['Location/Cards'] / (sort_df['adjusted_shift_length'] * (sort_df['Y-Int'] + (sort_df['Location/Cards'] * sort_df['Cards Coefficient'])))
sort_df['sort_day_%'] = sort_df['sort_day_%'].round(4)

sort_df["sort_earned_hours"] = ((sort_df['adjusted_shift_length'])/24) * sort_df['sort_day_%']
sort_df['sort_earned_hours'] = sort_df['sort_earned_hours'].round(6)

##Rename Columns, create empty column and task column
sort_df["Tag"] = ""
sort_df.loc[sort_df.Punch != '', 'Tag'] = "Sort"

##Write final dataframe
sort_df = sort_df[[
'Punch',
'first_offset',
'Preferred Name',
'Location/Cards',
'SQ/POQ',
'sort_day_%',
'sort_earned_hours',
'Test',
'Tag',
'adjusted_shift_length',
'Notes']]

sort_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'SQ/POQ':'Subtask',
'sort_day_%':'Day_%',
'sort_earned_hours':'Earned_Hours',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

###Scale Counting
##Build scale dataframe
scale_df = nuway_df.copy()
scale_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
scale_df.loc[scale_df.Task != 'Scale Counting', 'Punch'] = None
scale_df.dropna(subset=["Punch"], inplace=True)

##Build Scale standards dataframe
scale_standards_df = standards_df.copy()
scale_standards_df.drop(scale_standards_df.filter(like='Unnamed'), axis=1, inplace=True)
scale_standards_df.loc[scale_standards_df.Task != 'Scale Counting', 'Task'] = None
scale_standards_df.dropna(subset=["Task"], inplace=True)
scale_standards_df=scale_standards_df.fillna(0)

##Scrub Scale Counting data
scale_df['Location/Cards'] = scale_df['Location/Cards'].apply(pd.to_numeric, errors='coerce')
scale_cards_error_df = scale_df.copy()
scale_cards_error_df = scale_cards_error_df[scale_cards_error_df['Location/Cards'].isna()]
scale_cards_error_df['Location/Cards'] = scale_cards_error_df['Location/Cards'].fillna("Error")
scale_cards_error_df = scale_cards_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, scale_cards_error_df])

scale_df.dropna(subset=["Location/Cards"], inplace=True)

##Merge Standards with Dataframe
scale_df = pd.merge(scale_df, scale_standards_df, left_on = "Task", right_on = "Task")

##Calculate Metrics
scale_df['Y-Int'] = scale_df['Y-Int'].apply(pd.to_numeric, errors='coerce')

scale_df["scale_day_%"] = scale_df['Location/Cards'] / ((3600 / (scale_df['Y-Int'])) * scale_df['adjusted_shift_length'])
scale_df['scale_day_%'] = scale_df['scale_day_%'].round(4)

scale_df["scale_earned_hours"] = ((scale_df['adjusted_shift_length'])/24) * scale_df['scale_day_%']
scale_df['scale_earned_hours'] =scale_df['scale_earned_hours'].round(6)
scale_df["Subtask"] = ""
scale_df["Tag"] = ""
scale_df.loc[scale_df.Punch != '', 'Tag'] = "Scale"

##Write final dataframe
scale_df = scale_df[[
'Punch',
'first_offset',
'Preferred Name',
'Location/Cards',
'Subtask',
'scale_day_%',
'scale_earned_hours',
'Test',
'Tag',
'adjusted_shift_length',
'Notes']]

scale_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'scale_day_%':'Day_%',
'scale_earned_hours':'Earned_Hours',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

###SYP Processing
##Build syp dataframe
syp_df = nuway_df.copy()
syp_df.rename(columns={'QUEUE_NUMBER':'SQ/POQ'}, inplace=True)
syp_df.loc[syp_df.Task != 'SYP Proc', 'Punch'] = None
syp_df.dropna(subset=["Punch"], inplace=True)

##Build SYP standards dataframe
syp_standards_df = standards_df.copy()
syp_standards_df.drop(syp_standards_df.filter(like='Unnamed'), axis=1, inplace=True)
syp_standards_df.loc[syp_standards_df.Task != 'SYP', 'Task'] = None
syp_standards_df.dropna(subset=["Task"], inplace=True)
syp_standards_df=syp_standards_df.fillna(0)

##Scrub Scale Counting data
syp_df['Location/Cards'] = syp_df['Location/Cards'].apply(pd.to_numeric, errors='coerce')
syp_cards_error_df = syp_df.copy()
syp_cards_error_df = syp_cards_error_df[syp_cards_error_df['Location/Cards'].isna()]
syp_cards_error_df['Location/Cards'] = syp_cards_error_df['Location/Cards'].fillna("Error")
syp_cards_error_df = syp_cards_error_df[['Punch', 'Preferred Name', 'Puncher', 'Task', 'SQ/POQ', 'Location/Cards', 'Orders Completed', 'Flex Run', 'Env Run', 'Dupe']]

error_df = pd.concat([error_df, syp_cards_error_df])

syp_df.dropna(subset=["Location/Cards"], inplace=True)

##Define syp proc sizes
syp_df["syp_size"] = ""
syp_df.loc[syp_df['Location/Cards'] <= large, 'syp_size'] = "Medium"
syp_df.loc[syp_df['Location/Cards'] <= small, 'syp_size'] = "Small"
syp_df.loc[syp_df['Location/Cards'] > large, 'syp_size'] = "Large"

##Merge Standards with Dataframe
syp_df = pd.merge(syp_df, syp_standards_df, left_on = "syp_size", right_on = "Size")

##Calculate Metrics
syp_df['Y-Int'] = syp_df['Y-Int'].apply(pd.to_numeric, errors='coerce')

syp_df["syp_day_%"] = syp_df['Location/Cards'] / ((3600 / (syp_df['Y-Int'])) * syp_df['adjusted_shift_length'])
syp_df['syp_day_%'] = syp_df['syp_day_%'].round(4)

syp_df["syp_earned_hours"] = ((syp_df['adjusted_shift_length'])/24) * syp_df['syp_day_%']
syp_df['syp_earned_hours'] = syp_df['syp_earned_hours'].round(6)

##Rename Columns, create empty column and task column
syp_df["Subtask"] = ""
syp_df["Tag"] = ""
syp_df.loc[syp_df.Punch != '', 'Tag'] = "SYP"

##Write final dataframe
syp_df = syp_df[[
'Punch',
'first_offset',
'Preferred Name',
'Location/Cards',
'Subtask',
'syp_day_%',
'syp_earned_hours',
'Test',
'Tag',
'adjusted_shift_length',
'Notes']]

syp_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'syp_day_%':'Day_%',
'syp_earned_hours':'Earned_Hours',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

##Receiving Metrics
rec_final_df = nuway_df.copy()

rec_final_df.loc[(rec_final_df['QUEUE_NUMBER'] != 'RI Proc') & (rec_final_df['QUEUE_NUMBER'] != 'Flow'), 'Punch'] = None
rec_final_df.dropna(subset=["Punch"], inplace=True)

rec_final_df['Similar'] = rec_final_df['Similar'].apply(pd.to_numeric, errors='coerce')
rec_final_df['Similar'] = rec_final_df['Similar'].apply(np.ceil)

rec_final_df["Day %"] = rec_final_df['Similar'].astype('float64') / ((3600 / rec_final_df['Missing'].astype('float64')) * rec_final_df['adjusted_shift_length'].astype('float64'))
rec_final_df['Day %'] = rec_final_df['Day %'].round(4)

rec_final_df["Earned Hours"] = ((rec_final_df['adjusted_shift_length'].astype('float64'))/24) * rec_final_df['Day %'].astype('float64')
rec_final_df['Earned Hours'] = rec_final_df['Earned Hours'].round(6)

##Write final dataframe
rec_final_df["Test"] = ""

rec_final_df = rec_final_df[[
'Punch',
'first_offset',
'Preferred Name',
'Location/Cards',
'Task',
'Day %',
'Earned Hours',
'Test',
'QUEUE_NUMBER',
'adjusted_shift_length']]

rec_final_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'Task':'Subtask',
'Day %':'Day_%',
'Earned Hours':'Earned_Hours',
'QUEUE_NUMBER':'Tag',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

##BLO Metrics
blo_proc_final_df = nuway_df.copy()

blo_proc_final_df.loc[blo_proc_final_df['Task'] != 'BLO Proc', 'Punch'] = None
blo_proc_final_df.dropna(subset=["Punch"], inplace=True)

blo_proc_final_df["blo_day_%"] = blo_proc_final_df['Location/Cards'].astype('int64') / ((3600 / blo_proc_final_df['Extra'].astype('float64')) * blo_proc_final_df['adjusted_shift_length'].astype('float64'))
blo_proc_final_df['blo_day_%'] = blo_proc_final_df['blo_day_%'].round(4)

blo_proc_final_df["blo_earned_hours"] = ((blo_proc_final_df['adjusted_shift_length'].astype('float64'))/24) * blo_proc_final_df['blo_day_%'].astype('float64')
blo_proc_final_df['blo_earned_hours'] = blo_proc_final_df['blo_earned_hours'].round(6)

blo_ver_final_df = nuway_df.copy()

blo_ver_final_df.loc[blo_ver_final_df['Task'] != 'BLO Ver', 'Punch'] = None
blo_ver_final_df.dropna(subset=["Punch"], inplace=True)

blo_ver_final_df["blo_day_%"] = blo_ver_final_df['Location/Cards'].astype('int64') / ((3600 / blo_ver_final_df['Extra'].astype('float64')) * blo_ver_final_df['adjusted_shift_length'].astype('float64'))
blo_ver_final_df['blo_day_%'] = blo_ver_final_df['blo_day_%'].round(4)

blo_ver_final_df["blo_earned_hours"] = ((blo_ver_final_df['adjusted_shift_length'].astype('float64'))/24) * blo_ver_final_df['blo_day_%'].astype('float64')
blo_ver_final_df['blo_earned_hours'] = blo_ver_final_df['blo_earned_hours'].round(6)

blo_proc_final_df = pd.concat([blo_proc_final_df, blo_ver_final_df])

##Write final dataframe
blo_proc_final_df["Test"] = ""
blo_proc_final_df["Notes"] = ""

blo_proc_final_df = blo_proc_final_df[[
'Punch',
'first_offset',
'Preferred Name',
'Location/Cards',
'Task',
'blo_day_%',
'blo_earned_hours',
'Test',
'QUEUE_NUMBER',
'adjusted_shift_length',
'Notes']]

blo_proc_final_df.rename(columns={
'first_offset':'First_Offset',
'Preferred Name':'Puncher',
'Location/Cards':'Units',
'QUEUE_NUMBER':'Subtask',
'blo_day_%':'Day_%',
'blo_earned_hours':'Earned_Hours',
'Task':'Tag',
'adjusted_shift_length':'Adjusted_Shift_Length'},
inplace=True)

##Combine Data Frames
data_df = pd.concat([gen_df, pvp_df])

data_df = pd.concat([data_df, blo_proc_final_df])

data_df = pd.concat([data_df, filing_df])

data_df = pd.concat([data_df, pull_df])

data_df = pd.concat([data_df, pullver_df])

data_df = pd.concat([data_df, rec_final_df])

data_df = pd.concat([data_df, scale_df])

data_df = pd.concat([data_df, sort_df])

data_df = pd.concat([data_df, syp_df])

data_df.sort_values(by=['Puncher', 'Punch'], ascending=[True,False], inplace=True)

data_df['Data'] = data_df['Punch'].astype(str) + "|" + data_df['First_Offset'].astype(str) + "|" + data_df['Puncher'].astype(str) + "|" + data_df['Units'].astype(str) + "|" + data_df['Subtask'].astype(str) + "|" + data_df['Day_%'].astype(str) + "|" + data_df['Earned_Hours'].astype(str) + "|" + data_df['Test'].astype(str) + "|" + data_df['Tag'].astype(str) + "|" + data_df['Adjusted_Shift_Length'].astype(str) + "|" + data_df['Notes'].astype(str) + "|" + data_df['1'].astype(str) + "|" + data_df['2'].astype(str) + "|" + data_df['3'].astype(str) + "|" + data_df['4'].astype(str) + "|" + data_df['5'].astype(str) + "|" + data_df['6'].astype(str) + "|" + data_df['7'].astype(str) + "|" + data_df['Blank'].astype(str)

data_df = data_df.replace('nan', '', regex=True)

data_df = data_df[['Data']]

error_df.loc[error_df['Preferred Name'] == 'Error', 'Punch'] = None
error_df.dropna(subset=["Punch"], inplace=True)

##Turn off card
nunuwayhub = gc.open_by_key('1AtWbOiHUmWRUoNmWaPxSfj1qMYmRBHeUXf87gvXSF6s')
urltab = nunuwayhub.worksheet('NuNuScorecards')

url_df = gd.get_as_dataframe(urltab)
url_df.dropna(subset=["Google Key"], inplace=True)

for i in range (len(url_df)):
    subgroup = url_df.iloc[i, 2]

    if subgroup == 'Operations':

        url = url_df.iloc[i, 0]
        scorecard = gc.open_by_key(url)
        dataTab = scorecard.worksheet('Data')

        dataTab.update('A2', 'Off')

        dataTab.batch_clear(['A3:A'])

        gd.set_with_dataframe(dataTab, data_df, row=3, col=1)

        staffingtab = scorecard.worksheet('Personnel')
        staffingtab.clear()
        gd.set_with_dataframe(staffingtab, staffing_df)

        errorstab = scorecard.worksheet('Errors')
        errorstab.clear()
        gd.set_with_dataframe(errorstab, error_df)


        dataTab.update('A2', 'On')


print(time.time() - start_time)

