import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from functools import reduce
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from gcloud import storage
from datetime import timedelta


import pandas as pd
import numpy as np
import datetime as dt
import pandas_gbq
import re
import fnmatch
import os

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'


###Time and Date Condition
recent_time = dt.datetime.now() + dt.timedelta(hours=7)
timenow     = recent_time.strftime( "%Y-%m-%d %H:%M:%S")
datenow     = last_date   = (recent_time+dt.timedelta(days=-1)).strftime("%Y-%m-%d")

def query_condition_gateway(table):
    query_condition_gateway  = """where date(timestampadd(hour,0,{}.updated_at)) >= '{}'
                                and timestampadd(hour,0,{}.updated_at) < '{}'
                                """.format(table, last_date, table, timenow)
    return(query_condition_gateway)

def query_condition_invoicer(table):
    query_condition_invoicer = """where date(timestampadd(hour,0,{}.updated_at)) >= '{}'
                                and timestampadd(hour,0,{}.updated_at) < '{}'
                                """.format(table, last_date, table, timenow)
    return(query_condition_invoicer)

def query_condition_invoicer_noaddtime(table):
    query_condition_invoicer = """where {}.updated_at >= '{}'
                                and {}.updated_at < '{}'
                                """.format(table, last_date, table, timenow)
    return(query_condition_invoicer)


dataset     = 'datascience_public'
PROJECTID   = 'paper-prod'
###### main function ######

def datatype_sync(raw_data, schema):
    for column in raw_data:
        try:
            datatype = schema['Type'].loc[schema['Field'] == column].item()
            if   fnmatch.fnmatch(datatype, '*char*'):
                raw_data[column] = raw_data[column].astype('object')
            elif fnmatch.fnmatch(datatype, 'date'):
                raw_data[column] = raw_data[column].astype('object')
            elif fnmatch.fnmatch(datatype, 'text'):
                raw_data[column] = raw_data[column].astype('object')
            elif fnmatch.fnmatch(datatype, '*int*'):
                raw_data[column] = raw_data[column].astype('float64')
            elif fnmatch.fnmatch(datatype, '*double*'):
                raw_data[column] = raw_data[column].astype('float64')
            elif fnmatch.fnmatch(datatype, '*time*'):
                try:
                    print("stpone")
                    raw_data[column] = pd.to_datetime(raw_data[column])
                except Exception as e:
                    # print("ini eeeee : {}".format(e))
                    if 'Out of bounds nanosecond' in e.args[0]:
                        print("stptwo")
                        for ind in range(len(raw_data[column])):
                            try:
                                raw_data[column][ind] = pd.to_datetime(raw_data[column][ind])
                            except:
                                raw_data[column][ind] = pd.Timestamp.max
                                # print("nanosecond maximum : {}".format(pd.Timestamp.max))
                    else :
                        print(e)    
                        break
        except:
            pass
    return(raw_data)

def get_column_obj(datafix):
    list_obj_column = []
    for column in datafix :
        if datafix[column].dtype == 'O':
            #list_obj_column.append(str(column))
            datafix[column] = datafix[column].apply(lambda x: str(x))
    return(list_obj_column)

def hashing_function(raw_data, list_yes1, list_yes2):
    for kolom in list_yes1 :
        print('hashing '+str(kolom))
        if kolom.find("email")>-1 :
            raw_data[kolom] = hashing_email(raw_data,kolom)
        elif kolom.find("phone")>-1:
            raw_data[kolom] = hashing_phone(raw_data,kolom)
        else :
            raw_data[kolom] = hashing_general(raw_data,kolom)
    for kolom in list_yes2 :
        print('hashing '+str(kolom))
        raw_data[kolom] = hashing_boolean(raw_data,kolom)
    return raw_data

def read_sql(query):
    connection_id = 'paper_prod'
    mysqlserver   = MySqlHook(mysql_conn_id = connection_id)
    df            = mysqlserver.get_pandas_df(query)
    return df

def delete_data(dataset, table_name, new_data, uuid_name):
    ### table_name : nama table di bigquery, new_data : dataframe data yg mau distore, uuid_name : nama uuid di dataframe
    list_uuid = new_data.copy()
    Scopes_gcp           = ['https://www.googleapis.com/auth/cloud-platform']
    credentials_location = '/home/airflow/gcs/dags/credentials/credential_bq.json'
    
    store_bq(list_uuid, dataset, table_name+'_uuid_temp')
    
    client               = bigquery.Client.from_service_account_json(credentials_location)
    QUERY = (
        """Delete `{}.{}.{}`
       where {} in (select `uuid` from `{}.{}.{}`) """.format(PROJECTID, dataset, table_name, uuid_name, PROJECTID, dataset, table_name+'_uuid_temp'))
    
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()        # Waits for query to finish   
    
    drop_table = (
        """Drop table `{}.{}.{}`""".format(PROJECTID, dataset, table_name+'_uuid_temp'))
    
    query_drop_table = client.query(drop_table)  # API request
    rows = query_drop_table.result()        # Waits for query to finish 
    print("{}.{} ready for update process".format(dataset, table_name))

def store_backup_gcs(raw, table_name):
    raw.to_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), index=False)
    print('> > > DONE ....')


def store_bq(raw, dataset, table_name):
    print(raw.head())
    # Store to BQ 
    pandas_gbq.to_gbq(raw,'{}.{}'.format(dataset,table_name) ,project_id=PROJECTID, if_exists = 'append', location = 'asia-southeast1')
    del raw
    raw = pd.DataFrame()
    print("store to bigquery done {} {} {}".format(datenow, dataset, table_name))

def store_replace_bq(raw, dataset, table_name):
    print(raw.head())
    # Store to BQ 
    pandas_gbq.to_gbq(raw,'{}.{}'.format(dataset,table_name) ,project_id=PROJECTID, if_exists = 'replace', location = 'asia-southeast1')
    del raw
    raw = pd.DataFrame()
    print("store to bigquery done {} {} {}".format(datenow, dataset, table_name))
    
    

####### hashing function ######
def getNumbers(str): 
    # Kondisi Jika string bukan bertipe numerik (angka)     
    if str.isnumeric() == False : 
        # array berisi angka dari [0-9] dari string 
        # contoh str = '8 times before 1 pm'
        # array ['8','1']          
        array = re.findall('[0-9]+', str)
        # number berisi string kosong         
        number = ''
        # Mengubah format number dari '628' menjadi '08'       
        for i in range (len(array)):
            number = number+array[i]
        if number[0:3] == '628':
            number = '0'+number[2:]
    # Kondisi jika string bertipe numerik (angka)     
    else:
        number = str
        if number[0:3] == '628':
            number = '0'+ number[2:]
    # Digit ke 5 sampai digit ke 12 diubah menjadi 'x'     
    number = number[0:4]+8*'x'
    # Jika jumlah digit number < 9 maka number = '-'     
    if len(number)< 9 :
        number ='-'
    # Output hasil hashing     
    return number 

# hashing phone number di kolom company_phone
def hashing_phone(data,nama_kolom):
    data[nama_kolom] = data[nama_kolom].fillna('')
    data[nama_kolom] = data[nama_kolom].apply(lambda x: getNumbers(x))
    return data[nama_kolom]

# hashing kolom tertentu dengan kondisi {kolom tidak kosong = 1 | kolom kosong = 0}
def hashing_boolean(data,nama_kolom):
    data[nama_kolom] = np.where((data[nama_kolom].notnull()) == True & (data[nama_kolom] != ''), 1,0)
    return data[nama_kolom]

# fungsi hash email dengan 2 kondisi
def hash_email (str) :
    # jika |str| > 3 ubah string di indeks setelah indeks 0 sampai 2 menjadi 'x' sebanyak 3 kali
    if len(str)>3 :
        mask = str[:3]+3*'x'
    #  jika |str| <= 3 ubah string menjadi 'x' sebanyak 6 kali             
    else :
        mask = 6*'x'
    return mask

# Dari email yang dibuat pengguna dilakukan split('@',1) untuk menghashing huruf setelah 3 huruf pertama
# dengan membagi email menjadi 2 string email dan domain 
def first_email(email):
    try:
        email_name,domain = email.split("@",1)
        if email_name == 'h' :
            email_name = '-'
    except : 
        email_name = hash_email(email)
    return email_name

def last_email(email):
    try:
        email_name,domain = email.split("@",1)
        list_free_domain = ['aol.com', 'bigpond.com', 'bigpond.net.id', 'bluewin.ch', 'blueyonder.co.id', 'bol.com', 
                            'centurytel.net', 'charter.net', 'chello.com', 'club-internet.com', 'comcast.net', 'comee.com', 
                            'comeenet.net', 'comontiernet.net', 'cox.net', 'earthlink.net', 'facebook.com', 
                            'gmail.com', 'gmail.co.id', 'gmail.con', 'gmal.com', 'gmx.co.id', 'gmx.net', 'gmx.com',
                            'gmx.de', 'googlemail.com', 'hetnet.co.id', 'home.co.id', 'hotmail.co.id', 
                            'hotmail.com', 'hotmail.cim', 'juno.com', 'laposte.net', 'live.co.id', 'live.com', 
                            'mac.com', 'mail.com', 'mail.ru', 'me.com', 'msn.com', 'neuf.com', 'ntlworld.com', 
                            'optonline.net', 'optusnet.com', 'orange.com', 'outlook.com', 'outlook.co.id', 
                            'planet.co.id', 'qq.com', 'rambler.co.id', 'rediffmail.com', 'rocketmail.com',
                            'sbcglobal.net', 'scom.com', 'shaw.ca', 'sky.com', 'skynet.be', 'sympatico.ca', 
                            'telenet.co.id', 'terra.com.br', 'tuta.io', 'verizon.net', 'voila.com', 
                            'wanadoo.com', 'windstream.net', 'yahoo.co.id', 'yahoo.com', 'ymail.com', 
                            'ymail.co.id', 'icloud.com', 'windowslive.com', 'mailinator.cok', 'mailinator.com',
                            'gmaul.com', 'gmai.com', 'gmail.conm', 'gmail.coma', 'outlool.com', 'yaoo.com',
                            'gmail.co', 'gemail.com', 'gmakl.com', 'gail.com', 'gmail.cm', 'gmail.coim', 'gamil.com',
                            'gmil.com', 'gmailm.com', 'yahoo.cm', 'gmali.com', 'yahoo.id', 'yahoo.ckm', 'gmail.cok',
                            'yahoo.con.id', 'gimel.com', 'gmain.com', 'gmail.xcom', 'gmaip.com', 'gmaol.com', 'gmaill.com',
                            'yahoo.co.od', 'yahoo.om', 'gmsil.com', 'gmail.com.2', 'yahoo.co.uk', 'yhoo.com', 'yahoo.com.au',
                            '3578gmail.com', '53gmail.com', '697gmeil.com', 'fmail.com', 'g.mail.com', 'gimail.com', 'gmai.l.com',
                            'gmai.lcom', 'gmaiil.com', 'gmaik.com', 'gmaikl.com', 'gmail.clm', 'gmail.co.uk', 'gmail.com (Non Aktif)',
                            'gmail.com ', 'gmail.com.el', 'g-mail.com.id', 'gmail.com081218', 'gmail.com3', 'gmail.comArjan', 
                            'gmail.cpm', 'gmail.fom', 'gmail.om', 'gmail.som', 'gmail.vom', 'gmail.xom', 'gmail1.com', 'gmail12.com',
                            'gmail88.com', 'gmaio.com', 'gmal.nom', 'gmeil.com', 'gmel.com', 'gmial.com', 'gmile.com', 'gmli.com',
                            'hmail.com', 'iclod.com', 'icloid.com', 'icloud.id', 'paper.com', 'paper.id2', 'paper.idd', 'paper.ids',
                            'paper2.id', 'tes.com', 'tesk.com', 'test.com', 'test.id', 'test3.com', 'testajaa.com', 'tested.com',
                            'testing.com', 'testingpaper.ic', 'testtt.com', 'testuser.com', 'xxx.com', 'y7mail.com', 'yaho.co.id',
                            'yaho.com', 'yahoo.co.if', 'yahoo.co.is', 'yahoo.cok', 'yahoo.com.id', 'yahoo.com.tw', 'yahoo.fr',
                            'yahoo.id.co', 'yahoo.xom', 'yahool.com', 'yahooo.com', 'yhoo.co.id', 'ymail.cim', 'yahoi.com',
                            'yahoo.con', 'gmail.cim', 'paper.id' ]
        if domain == 'h' :
            domain = '-'
        elif domain.lower() not in list_free_domain :
            domain = 'privatedomain'
    except : 
        domain = 'xxx.xxx' 
    return domain

def null_email (str) :
    if len(str) < 4 :
        mask = '-'
    else : 
        mask = str 
    return mask

def hashing_email(data,nama_kolom):
    data[nama_kolom] = data[nama_kolom].fillna('h@h')
    data['first email'] = data[nama_kolom].apply(lambda x : first_email(x))
    data['last email'] = data[nama_kolom].apply(lambda x : last_email(x))
    data[nama_kolom] = data['first email'].apply(lambda x : hash_email(x)+'@') + data['last email']
    del data['first email'],data['last email']
    data[nama_kolom] = data[nama_kolom].apply(lambda x : null_email(x))
    return data[nama_kolom]

def hash_general (str) :
    if len(str)>4 :
        mask = 6*'x'+str[-4:]
    elif len(str)<1 :
        mask = '-'
    else :
        mask = 10*'x'
    return mask

def hashing_general(data,nama_kolom):
    data[nama_kolom] = data[nama_kolom].fillna('')
    data[nama_kolom] = data[nama_kolom].apply(lambda x: hash_general(x))
    return data[nama_kolom]

def accname(acc_name):
    if acc_name.find("#") > -1 :
        b = acc_name.split("#")
        a = b[0]
    else :
        a = acc_name
    return a

def delete_last_3_days_data(dataset, table_name):
    print("prepare bigquery delete last 3 days data..")
    Scopes_gcp           = ['https://www.googleapis.com/auth/cloud-platform']
    credentials_location = '/home/airflow/gcs/dags/credentials/skilled-compass-218404-c12c1af60adf.json'
    client               = bigquery.Client.from_service_account_json(credentials_location)
    QUERY = (
        """Delete `{}.{}.{}`
       where parse_date('%Y%m%d', date_google_analytics_time) > date_sub(date(CURRENT_DATETIME()),interval 4 day)""".format(PROJECTID, dataset, table_name))
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()        # Waits for query to finish    
    print("{}.{} ready for update process".format(dataset, table_name))
    

def read_bq(sql):
    # menggunakan credential
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/airflow/gcs/dags/credentials/credential_bq.json"
    client = bigquery.Client()
   
    # df = client.query(sql).to_dataframe()
    df = client.query(sql).to_dataframe()
    print('data imported from BQ Firebase')
    return df

def bq_schema_sync(df_splited, df_schema):
    """
    Me syncronize datatype pada dataframe yang baru
    dengan table/dataframe yang ada di BQ
    df_splited = table/dataframe yang akan disamakan datatypenya
    df_schema  = table/dataframe yang sudah ada di BQ
    
    contoh untuk get schema :
    # sql_schema =
            # select 
            # tb.*
            # from (select * from `paper-prod.datascience_public.INFORMATION_SCHEMA.COLUMNS`) as tb
            # where tb.table_name = '{}'
            # .format(table_name)
    """
    
    arr = []
    for k in df_splited.dtypes:
        arr.append(k)
        
    arr_col_sql = []
    for i in arr:
        if i == "object":
            arr_col_sql.append('string')
        if i == "float64":
            arr_col_sql.append('float64')
        if i == "int64":
            arr_col_sql.append('int64')
        if i == "datetime64[ns]":
            arr_col_sql.append('timestamp')
            
    column_list = list(df_splited.columns)
    
    column_name = df_schema['column_name']
    data_type = df_schema['data_type']
    
    for i, j, k, l in zip(column_list, arr_col_sql, column_name, data_type):
        # print(i, '<>', j, ' ---> ', k, '<>',  l.lower())
        if j != l.lower():
            print(j, l)
            if l == 'INT64':
                print('to int')
                df_splited['{}'.format(i)] = df_splited['{}'.format(i)].astype(int)
            elif l == 'STRING':
                print('to string')
                df_splited['{}'.format(i)] = df_splited['{}'.format(i)].astype(str)
            elif l == 'FLOAT64':
                print('to float')
                df_splited['{}'.format(i)] = df_splited['{}'.format(i)].astype(float)
            elif l == 'TIMESTAMP':
                print('to timestamp')
                df_splited['{}'.format(i)] = pd.to_datetime(df_splited['{}'.format(i)], format='%Y-%m-%d %H:%M:%S')
        else:
            pass
    
    return df_splited


def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_blue_circle: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of failure task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)





def raw_data_sms_logs():
    dataset = 'datascience_public'
    table_name = 'raw_data_sms_logs'
    schema     = read_sql("describe paper_invoicer.sms_logs")
    raw_data   = read_sql("""
    select
    sl.*

    from paper_invoicer.sms_logs as sl
    
    {} #condition data gathering
    
    -- limit 10
    """.format(query_condition_invoicer('sl')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'sms_log_id')

        store_backup_gcs(raw_data, table_name)

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'sms_log_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)     
    
    
def raw_data_shorten_urls():
    dataset = 'datascience_public'
    table_name = 'raw_data_shorten_urls'
    schema     = read_sql("describe paper_invoicer.shorten_urls")
    raw_data   = read_sql("""
    select
    su.*
    from paper_invoicer.shorten_urls as su
    
    {} #condition data gathering
    
    -- limit 10
    """.format(query_condition_invoicer('su')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'shorten_url_id')
        store_backup_gcs(raw_data, table_name)

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'shorten_url_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name) 


def raw_data_companies_no_hash():
    dataset = 'datascience'
    table_name = 'companies'
    schema    = read_sql("describe paper_gateway.companies")
    
    raw_data   = read_sql("""
    select 
    c.*,
    timestampadd(hour, 7, c.created_at) as created_at_temp,
    timestampadd(hour, 7, c.updated_at) as updated_at_temp,
    bt.name as business_type_child_temp
    from paper_gateway.companies as c
    -- where date(updated_at) = date(now())
    left join paper_gateway.business_type as bt on bt.uuid = c.business_type_child
    
    {} #condition data gathering
    
    """.format(query_condition_gateway('c')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at',
            'business_type_child'
        ])]]
        raw_data.rename(columns = {
         'created_at_temp'          : 'created_at',
         'updated_at_temp'          : 'updated_at',
    #      'uuid'                     : 'company_id',
         'business_type_child_temp' : 'business_type_child'
        }, inplace = True)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'company_id')
        store_backup_gcs(raw_data, dataset+"_"+table_name)

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(dataset+"_"+table_name) + '/{}_{}.csv'.format(dataset+"_"+table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)
        raw_data['business_type_child'] = raw_data['business_type_child'].astype('object')

        raw_data.rename(columns = {
         'uuid'                     : 'company_id'
        }, inplace = True)

        print(raw_data.dtypes)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

    
def raw_data_users_no_hash():
    dataset    = 'datascience'
    table_name = 'users'
    schema     = read_sql("describe paper_gateway.users")
    raw_data   = read_sql("""
    select 
    u.*,
    timestampadd(hour, 7, u.created_at) as created_at_temp,
    timestampadd(hour, 7, u.updated_at) as updated_at_temp
    from paper_gateway.users as u
    """)
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
        print('\nraw_data shape : ', raw_data.shape, '\n')

        raw_data.rename(columns = {
         'created_at_temp'          : 'created_at',
         'updated_at_temp'          : 'updated_at'
        }, inplace = True)
        print('\nraw_data shape : ', raw_data.shape, '\n')

        #delete all data updated
        delete_data(dataset, table_name, raw_data, 'user_id')
        store_backup_gcs(raw_data, dataset+"_"+table_name)

        ###ubah tipe data agar ssuai di bq
        raw_data = datatype_sync(raw_data,schema)
        print('\nraw_data shape : ', raw_data.shape, '\n')

        raw_data.rename(columns = {
         'uuid'                     : 'user_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # bellow is store the dataframe (same with stored dataframe to GCS) and replace if existing
        store_replace_bq(raw=raw_data, 
            dataset=dataset, 
            table_name=table_name)  
    
    
def raw_data_users():
    dataset = 'datascience_public'
    table_name = 'raw_data_users'
    schema     = read_sql("describe paper_gateway.users")
    raw_data   = read_sql("""
    select 
    u.*,
    timestampadd(hour, 7, u.created_at) as created_at_temp,
    timestampadd(hour, 7, u.updated_at) as updated_at_temp
    from paper_gateway.users as u
    """)
    
    print('\nraw_data shape : ', raw_data.shape, '\n')
    print("updating data " + table_name)

    raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
    print('\nraw_data shape : ', raw_data.shape, '\n')

    raw_data.rename(columns = {
         'created_at_temp'          : 'created_at',
         'updated_at_temp'          : 'updated_at'
        }, inplace = True)
    print('\nraw_data shape : ', raw_data.shape, '\n')

    list_yes1 = ['name','lastname','email','username','phone']
    list_yes2 = ['verificationToken']
    raw_data = hashing_function(raw_data, list_yes1, list_yes2)    
    print('\nraw_data shape : ', raw_data.shape, '\n')

    store_backup_gcs(raw_data, table_name)
    print('\nraw_data shape : ', raw_data.shape, '\n')

    raw_data = datatype_sync(raw_data,schema)
    print('\nraw_data shape : ', raw_data.shape, '\n')

    print(raw_data.info())

    raw_data.rename(columns = {
         'uuid' : 'user_id'
    }, inplace = True)

    print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
    store_replace_bq(raw_data, dataset, table_name)
    
    
def raw_data_companies():
    dataset = 'datascience_public'
    table_name = 'raw_data_companies'
    schema    = read_sql("describe paper_gateway.companies")
    
    raw_data   = read_sql("""
    select 
    c.*,
    timestampadd(hour, 7, c.created_at) as created_at_temp,
    timestampadd(hour, 7, c.updated_at) as updated_at_temp,
    bt.name as business_type_child_temp
    from paper_gateway.companies as c
    -- where date(updated_at) = date(now())
    left join paper_gateway.business_type as bt on bt.uuid = c.business_type_child
    
    {} #condition data gathering
    
    """.format(query_condition_gateway('c')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at',
            'business_type_child'
        ])]]
        raw_data.rename(columns = {
         'created_at_temp'          : 'created_at',
         'updated_at_temp'          : 'updated_at',
         'business_type_child_temp' : 'business_type_child'
        }, inplace = True)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'company_id')

        list_yes1  = ['company_name',
                     'company_email',
                     'company_phone']
        list_yes2  = ['company_address1',
                     'company_address2',
                     'company_website',
                     'company_bank_name',
                     'company_bank_account']

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)
        raw_data['business_type_child'] = raw_data['business_type_child'].astype('object')

        raw_data.rename(columns = {
         'uuid'                     : 'company_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

def raw_data_invoices():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoices'
    schema     = read_sql("describe paper_invoicer.invoices")
    raw_data   = read_sql("""
    select 
        i.*,
        timestampadd(hour, 14, i.created_at) as created_at_temp,
        timestampadd(hour, 14, i.updated_at) as updated_at_temp,
        it.grandTotalUnformatted as invoice_amount
    from paper_invoicer.invoices as i
    left join
    (   select invoice_totals.invoice_id, grandTotalUnformatted
        from paper_invoicer.invoice_totals
    ) as it on it.invoice_id = i.uuid
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('i')))

    # raw_data   = read_sql("""
    # select 
    # i.*,
    # timestampadd(hour, 14, i.created_at) as created_at_temp,
    # timestampadd(hour, 14, i.updated_at) as updated_at_temp,
    # q.invoice_amount_temp - i.discount as invoice_amount

    # from paper_invoicer.invoices as i

    # join (
    #         select
    #         invoice_items.invoice_id,
    #         sum(case  
    #             when tax_settings.exclusive is false or tax_settings.exclusive is null            #case when tax is exclusive
    #                 then 
    #                     case 
    #                         when invoice_items.discount is not null 
    #                             then 
    #                                 invoice_items.quantity*invoice_items.price*(1-invoice_items.discount/100)      #case when discount_items is not null
    #                         else invoice_items.quantity*invoice_items.price                                             #case when discount_items is null
    #                         end
    #             else    
    #                 case 
    #                     when invoice_items.discount is not null 
    #                         then 
    #                             (invoice_items.quantity*invoice_items.price*(1-invoice_items.discount/100)) * ((100 + tax_settings.value)/100)        #case when discount_items is not null
    #                     else (invoice_items.quantity*invoice_items.price) * (110/100)                                                                   #case when discount_items is null
    #                     end
    #             end)
    #         as invoice_amount_temp
    #         from paper_invoicer.invoice_items as invoice_items
    #         left join paper_invoicer.tax_settings as tax_settings on invoice_items.tax_id = tax_settings.uuid
    #         group by invoice_items.invoice_id) as q 
    #         on i.uuid = q.invoice_id
    
    # {} #condition data gathering
    
    # """.format(query_condition_invoicer('i')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
        raw_data.rename(columns = {
         'created_at_temp'          : 'created_at',
         'updated_at_temp'          : 'updated_at'
    #      'uuid'                     : 'invoice_id'
        }, inplace = True)

        list_yes1 = []
        list_yes2 = ['terms',
                     'signature_text_footer',
                     'notes'
                     ]

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'invoice_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)    


def raw_data_invoice_items():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoice_items'
    schema     = read_sql("describe paper_invoicer.invoice_items")
    raw_data   = read_sql("""
    select 
    ii.*,
    ts.name as tax_name,
    ts.value as tax_value


    from paper_invoicer.invoice_items as ii
    left join paper_invoicer.tax_settings as ts on ts.uuid = ii.tax_id
    
    {} #condition data gathering
    
-- limit 10
    """.format(query_condition_invoicer('ii')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_item_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'invoice_item_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)    


def raw_data_invoice_reminder_histories():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoice_reminder_histories'
    schema     = read_sql("describe paper_invoicer.invoice_reminder_histories")
    raw_data   = read_sql("""
    select 
    irh.*
    from paper_invoicer.invoice_reminder_histories as irh
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('irh')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_reminder_id')
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'invoice_reminder_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)


def raw_data_invoice_reminder_settings():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoice_reminder_settings'
    schema     = read_sql("describe paper_invoicer.invoice_reminder_settings")
    raw_data   = read_sql("""
    select 
    irs.*
    from paper_invoicer.invoice_reminder_settings as irs
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('irs')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_reminder_setting_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'invoice_reminder_setting_id'
        }, inplace = True)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)


def raw_data_mailgun_trackings():
    dataset = 'datascience_public'
    table_name = 'raw_data_mailgun_trackings'
    schema     = read_sql("describe paper_invoicer.mailgun_trackings")
    raw_data   = read_sql("""
    select 
    mt.*

    from paper_invoicer.mailgun_trackings as mt
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('mt')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'mailgun_tracking_id')
        raw_data.to_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), index=False)
        print('> > > > DONE ...')

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'mailgun_tracking_id'
        }, inplace = True)

        raw_data['created_at'] = pd.to_datetime(raw_data['created_at'], errors='coerce')
        raw_data['updated_at'] = pd.to_datetime(raw_data['updated_at'], errors='coerce')

        print(raw_data.info())
        store_bq(raw_data, dataset, table_name)

def raw_data_order_items():
    dataset = 'datascience_public'
    table_name = 'raw_data_order_items'
    schema     = read_sql("describe paper_invoicer.order_items")
    raw_data   = read_sql("""
    select 
    oi.*,
    ts.name as tax_name,
    ts.value as tax_value


    from paper_invoicer.order_items as oi
    left join paper_invoicer.tax_settings as ts on ts.uuid = oi.tax_id

    {} #condition data gathering
    
    """.format(query_condition_invoicer('oi')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'order_item_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'order_item_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)        
        

def raw_data_payments():
    dataset = 'datascience_public'
    table_name = 'raw_data_payments'
    schema1    = read_sql("describe paper_invoicer.payments")
    schema2    = read_sql("describe paper_invoicer.payment_methods")
    schema2.rename(columns = {
     'name'          : 'method_name'
    }, inplace = True)
        
    schema = pd.concat([schema1,schema2]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""
    select
    p.*,
    pm.name as method_name,
    pm.selected,
    pm.active_in,
    pm.active_out,
    pm.is_digital_payment

    from paper_invoicer.payments as p
    left join paper_invoicer.payment_methods as pm on pm.uuid = p.method
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('p')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'payment_id')

        list_yes1 = []
        list_yes2 = ['notes',
                     'veritrans_masked_card',
                     'veritrans_permata_va_number',
                     'veritrans_signature_key']

        ###hashing process
        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        ###primary key rename
        raw_data.rename(columns = {
         'uuid'                     : 'payment_id'
        }, inplace = True)
        print('\n\nraw_datainfo Before Change schema : ', raw_data.info(), '\n\n')

        raw_data['notes']                       = raw_data['notes'].astype(int)
        raw_data['veritrans_masked_card']       = raw_data['veritrans_masked_card'].astype(int)
        raw_data['veritrans_transaction_time']  = raw_data['veritrans_transaction_time'].astype(str)
        raw_data['veritrans_gross_amount']      = raw_data['veritrans_gross_amount'].astype(int)
        raw_data['veritrans_signature_key']     = raw_data['veritrans_signature_key'].astype(int)
        raw_data['veritrans_status_code']       = raw_data['veritrans_status_code'].astype(int)
        raw_data['veritrans_permata_va_number'] = raw_data['veritrans_permata_va_number'].astype(int)

        print('\n\nraw_data.info After Change schema : ', raw_data.info(), '\n\n')
        ### store to bigquery
        store_bq(raw_data, dataset, table_name)

    
def raw_data_payments_invoices():
    dataset = 'datascience_public'
    table_name = 'raw_data_payments_invoices'
    schema     = read_sql("describe paper_invoicer.payments_invoices")
    raw_data   = read_sql("""
    select
    pi.*
    from paper_invoicer.payments_invoices as pi
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('pi')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'payment_invoice_id')

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'payment_invoice_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

    
def raw_data_orders():
    dataset = 'datascience_public'
    table_name = 'raw_data_orders'
    schema     = read_sql("describe paper_invoicer.orders")
    raw_data   = read_sql("""
    select
    o.*
    from paper_invoicer.orders as o
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('o')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'order_id')

        list_yes1 = []
        list_yes2 = ['supplier_reference',
                     'terms',
                     'notes'
                    ]
        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'order_id'
        }, inplace = True)

        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)


def raw_data_products():
    dataset = 'datascience_public'
    table_name = 'raw_data_products'
    schema1    = read_sql("describe paper_invoicer.products")
    schema2    = read_sql("describe paper_invoicer.uoms")
    schema2.rename(columns = {
     'name'        : 'uom_name',
     'symbol'      : 'uom_symbol',
     'code'        : 'uom_code'
        }, inplace = True)
        
    schema = pd.concat([schema1,schema2]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""
    select
    p.*,
    uoms.name as uom_name,
    uoms.symbol as uom_symbol,
    uoms.code as uom_code

    from paper_invoicer.products as p
    left join paper_invoicer.uoms on uoms.uuid = p.uom_id
    -- where year(p.created_at) < '2015'
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('p')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'product_id')

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'product_id'
        }, inplace = True)


        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

    
def raw_data_partners():
    dataset = 'datascience_public'
    table_name = 'raw_data_partners'
    schema     = read_sql("describe paper_invoicer.partners")
    raw_data   = read_sql("""
    select 
    *
    from paper_invoicer.partners
    
    {} #condition data gathering
    
    """.format(query_condition_invoicer('partners')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'partner_id')

        list_yes1 = ['name',
                    'email',
                    'mobile',
                    'phone']
        list_yes2 = ['notes',
                     'address1',
                     'address2',
                     'virtual_account',
                     'website']

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'partner_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

def raw_data_partners_no_hashing():
    dataset = 'datascience'
    table_name = 'partners'
    schema     = read_sql("describe paper_invoicer.partners")
    raw_data   = read_sql("""
    select 
    *
    from paper_invoicer.partners
    """)

    if len(raw_data) == 0:
        print(table_name + "is empty")

    else:
        import io
        import pandas as pd

        csv_buffer = io.StringIO()
        output_file = raw_data.to_csv(csv_buffer, index=False)
        raw_data = pd.read_csv(io.StringIO(csv_buffer.getvalue()), error_bad_lines=False, skipinitialspace=True, encoding='utf-8')

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'partner_id')
        print('Finist func delete_data . . . and Store to GCS . . .')

        store_backup_gcs(raw_data, dataset + "_" + table_name)
        
        raw_data.rename(columns = {
         'uuid'                     : 'partner_id'
        }, inplace = True)

        raw_data = raw_data.drop_duplicates(subset=['partner_id'], keep='first')
        print('convert to timestamp')
        raw_data['created_at'] =  pd.to_datetime(raw_data.created_at, errors='coerce')
        raw_data['updated_at'] =  pd.to_datetime(raw_data.created_at, errors='coerce')
        raw_data['deleted_at'] =  pd.to_datetime(raw_data.created_at, errors='coerce')
        raw_data['payment_term'] = raw_data['payment_term'].astype(float)
        raw_data['increment_num'] = raw_data['increment_num'].astype(str)
        print('raw_data info after to timestamp : \n', raw_data.info())
        
        # # bellow is store the dataframe (same with stored dataframe to GCS) and replace if existing
        store_replace_bq(raw=raw_data, 
            dataset=dataset, 
            table_name=table_name)
            
    
def raw_data_plans():
    dataset = 'datascience_public'
    table_name = 'raw_data_plans'
    schema     = read_sql("describe paper_invoicer.plans")
    raw_data   = read_sql("""
    select 
    *
    from paper_invoicer.plans
    {} #condition data gathering
    
    """.format(query_condition_invoicer('plans')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data.rename(columns = {
         'uuid'                 : 'plan_item_id'
         }, inplace = True)    
        raw_data.rename(columns = {'meta_id' : 'uuid'}, inplace = True)
        
        ###delete all data updated
        delete_data(dataset, table_name, raw_data, 'plan_id')

        list_yes1 = []
        list_yes2 = ['meta_value']

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid'  : 'plan_id'}, inplace = True)
        raw_data.rename(columns = {'plan_item_id' : 'uuid'}, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)



def raw_data_schedulers():
    dataset = 'datascience_public'
    table_name = 'raw_data_schedulers'
    schema     = read_sql("describe paper_invoicer.schedulers")
    raw_data   = read_sql("""
    select 
    *
    from paper_invoicer.schedulers
    """)
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []
        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'scheduler_id'
        }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_replace_bq(raw_data, dataset, table_name)


    
def raw_data_subscriptions():
    dataset = 'datascience_public'
    table_name = 'raw_data_subscriptions'
    schema1    = read_sql("describe paper_gateway.subscriptions")
    schema2    = read_sql("describe paper_gateway.packages")
    schema3    = read_sql("describe paper_gateway.paywall_payments")
    schema     = pd.concat([schema1,schema2,schema3]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    
    raw_data   = read_sql("""
    select 
    s.*,
    p.package_name,
    p.length,
    p.price,
    pp.subscription_order_id,
    pp.transaction_id,
    pp.gross_amount,
    pp.payment_type,
    pp.transaction_status,
    timestampadd(hour, 7, pp.transaction_time) as transaction_time,
    timestampadd(hour, 7, pp.payment_date) as payment_date,
    pp.fraud_status,
    pp.referral_code,
    timestampadd(hour, 7, s.created_at) as created_at_temp,
    timestampadd(hour, 7, s.updated_at) as updated_at_temp

    from paper_gateway.subscriptions as s 
    left join paper_gateway.packages as p on s.package_id = p.uuid
    left join paper_gateway.paywall_payments as pp on s.uuid = pp.subscription_id
    {} #condition data gathering
    
    """.format(query_condition_gateway('s')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
        raw_data.rename(columns = {
         'created_at_temp'      : 'created_at',
         'updated_at_temp'      : 'updated_at'
    #      'uuid'                 : 'partner_id'
        }, inplace = True)  
 
        ###delete all data updated
        delete_data(dataset, table_name, raw_data, 'subscription_id')

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'subscription_id'
        }, inplace = True)
        
        print(raw_data.dtypes)
        raw_data['start_date'] = pd.to_datetime(raw_data['start_date'])
        raw_data['end_date']   = pd.to_datetime(raw_data['end_date'])
        print(raw_data.dtypes)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

    
def raw_data_user_comment_meta_setting():
    dataset = 'datascience_public'
    table_name = 'raw_data_user_comment_metasetting'
    schema     = read_sql("describe paper_gateway.user_comment_metasetting")
    raw_data   = read_sql("""
    select 
    *,
    timestampadd(hour, 7, created_at) as created_at_temp,
    timestampadd(hour, 7, updated_at) as updated_at_temp

    from paper_gateway.user_comment_metasetting
    {} #condition data gathering
    
    """.format(query_condition_gateway('user_comment_metasetting')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
        raw_data.rename(columns = {
         'created_at_temp'      : 'created_at',
         'updated_at_temp'      : 'updated_at'
    #      'uuid'                 : 'partner_id'
        }, inplace = True)  
        
        ###delete all data updated
        delete_data(dataset, table_name, raw_data, 'user_comment_metasetting_id')

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        raw_data.to_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), index=False)
        print('> > > > DONE ...')

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                 : 'user_comment_metasetting_id'
        }, inplace = True)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)

  

def raw_data_expenses():
    dataset = 'datascience_public'
    table_name = 'raw_data_expenses'
    schema     = read_sql("describe paper_invoicer.expenses")
    raw_data   = read_sql("""select *
                             from paper_invoicer.expenses
                             
                             {} #condition data gathering
                             
                             """.format(query_condition_invoicer('expenses')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'expense_id')

        # Hashing table
        list_yes1  = []
        list_yes2  = ['notes']
        raw_data   = hashing_function(raw_data, list_yes1, list_yes2)

        # Store to GCS
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'expense_id'}, inplace = True)
        raw_data['custom_fields'] = raw_data['custom_fields'].astype(str)
        raw_data['status']        = raw_data['status'].astype(int)
        print('\n\n', raw_data.info(), '\n\n')
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)
     
    
def raw_data_stock_fifo_inputs():
    dataset = 'datascience_public'
    table_name = 'raw_data_stock_fifo_inputs'
    schema     = read_sql("describe paper_invoicer.stock_fifo_inputs")
    raw_data   = read_sql("""select *
                             from paper_invoicer.stock_fifo_inputs
                             
                             {} #condition data gathering
                             
                             """.format(query_condition_invoicer('stock_fifo_inputs')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'stock_fifo_input_id')
        raw_data.to_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), index=False)
        print('> > > > DONE ...')

        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'stock_fifo_input_id'
                                   }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)


def raw_data_stock_fifo_outputs():
    dataset = 'datascience_public'
    table_name = 'raw_data_stock_fifo_outputs'
    schema     = read_sql("describe paper_invoicer.stock_fifo_outputs")
    raw_data   = read_sql("""select *
                             from paper_invoicer.stock_fifo_outputs
                             
                             {} #condition data gathering
                             
                             """.format(query_condition_invoicer('stock_fifo_outputs')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'stock_fifo_output_id')
        

        # Store to GCS
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'stock_fifo_output_id'
                                   }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)



def raw_data_stock_movements():
    dataset = 'datascience_public'
    table_name = 'raw_data_stock_movements'
    schema     = read_sql("describe paper_invoicer.stock_movements")
    raw_data   = read_sql("""select *
                             from paper_invoicer.stock_movements
                             
                             {} #condition data gathering
                             
                             """.format(query_condition_invoicer('stock_movements')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'stock_movement_id')

        # Store to GCS
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'stock_movement_id'
                                   }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)


    
def raw_data_stock_documents():
    dataset = 'datascience_public'
    table_name = 'raw_data_stock_documents'
    schema1     = read_sql("describe paper_invoicer.stock_documents")
    schema2     = read_sql("describe paper_invoicer.stock_document_types")
    schema2.rename(columns = {'name'  : 'stock_document_type'
                               }, inplace = True)   
    schema = pd.concat([schema1,schema2]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
        
    raw_data   = read_sql("""select sd.*,
                            sdt.name as stock_document_type,
                            sdt.prefix as prefix
                            from paper_invoicer.stock_documents as sd 
                            left join paper_invoicer.stock_document_types as sdt on  sd.stock_document_type_id = sdt.uuid 
                            
                            {} #condition data gathering
                            
                            """.format(query_condition_invoicer('sd')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'stock_document_id')

        # Hashing table
        list_yes1  = []
        list_yes2  = ['notes','description', 'tracking_no', 'driver_name', 'vehicle_number']
        raw_data   = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'stock_document_id'
                                   }, inplace = True)
        print(raw_data.dtypes)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)

    
def raw_data_locations():
    dataset = 'datascience_public'
    table_name = 'raw_data_locations'
    schema1    = read_sql("describe paper_invoicer.locations")
    schema2    = read_sql("describe paper_invoicer.location_types")
    schema1.rename(columns = {'name'  : 'location_name'
                               }, inplace = True)   
    schema2.rename(columns = {'name'  : 'location_type_name'
                               }, inplace = True)      
    
    schema = pd.concat([schema1,schema2]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""select 
                            l.*, 
                            l.name as location_name,
                            lt.name as location_type_name,
                            lt.track_value,
                            timestampadd(hour, 14, l.created_at) as created_at_temp, 
                            timestampadd(hour, 14, l.updated_at) as updated_at_temp

                            from paper_invoicer.locations as l 
                            left join paper_invoicer.location_types as lt on l.location_type_id = lt.uuid
                            
                            {} #condition data gathering
                            
                            """.format(query_condition_invoicer('l')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data   = raw_data[raw_data.columns.difference(['name','created_at', 'updated_at'])]
        raw_data.rename(columns = {'created_at_temp' : 'created_at',
                                   'updated_at_temp' : 'updated_at'}, inplace = True)


        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'location_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'location_id'
                                  }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)

    
def raw_data_finance_transactions():
    dataset = 'datascience_public'
    table_name = 'raw_data_finance_transations'
    schema1    = read_sql("describe paper_invoicer.finance_transactions")
    schema2    = read_sql("describe paper_invoicer.finance_transaction_types")
    schema2.rename(columns = {'code'  : 'finance_transaction_type_code',
                              'name'  : 'finance_transaction_type_name',
                              'type'  : 'finance_transaction_type'
                               }, inplace = True)
    
    schema = pd.concat([schema1,schema2]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""select 
                            ft.*,
                            ftt.code as finance_transaction_type_code,
                            ftt.name as finance_transaction_type_name,
                            ftt.type as finance_transaction_type

                            from paper_invoicer.finance_transactions as ft
                            left join paper_invoicer.finance_transaction_types as ftt on ft.finance_transaction_type_id = ftt.uuid
                            
                            {} #condition data gathering
                            
                            """.format(query_condition_invoicer('ft')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'finance_transactions_id')

        # Hashing table
        list_yes1  = []
        list_yes2  = ['reference']
        raw_data   = hashing_function(raw_data, list_yes1, list_yes2)
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'finance_transactions_id'
                                  }, inplace = True)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        # Store to BQ
        store_bq(raw_data, dataset, table_name)


    
def raw_data_account_histories():
    dataset = 'datascience_public'
    table_name = 'raw_data_account_histories'
    schema1    = read_sql("describe paper_invoicer.account_histories")
    schema2    = read_sql("describe paper_invoicer.accounts")
    schema3    = read_sql("describe paper_invoicer.account_types")
    schema2.rename(columns = {'name'    : 'account_name',
                              'status'  : 'account_status'
                               }, inplace = True)   
    schema3.rename(columns = {'name'           : 'account_type_name',
                              'name_lang'      : 'account_type_name_lang',
                              'normal_balance' : 'account_type_normal_balance'
                               }, inplace = True)
    
    schema = pd.concat([schema1,schema2,schema3]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""select ah.*, 
                            a.account_type_id,
                            a.account_parent_id,
                            a.account_code as account_code,
                            a.name as account_name,
                            a.status as account_status,
                            a.is_deleteable,
                            a.original_account_code,
                            acc_typ.name as account_type_name,
                            acc_typ.name_lang as account_type_name_lang,
                            acc_typ.normal_balance as account_type_normal_balance

                            from paper_invoicer.account_histories as ah 
                            left join paper_invoicer.accounts as a on ah.account_id = a.uuid
                            left join paper_invoicer.account_types as acc_typ on a.account_type_id = acc_typ.uuid
                            
                            {} #condition data gathering
                            
                            and (ah.debit_amount <> 0) and (ah.credit_amount <> 0) and (ah.debit_balance <> 0) and (ah.credit_balance <> 0)
                            """.format(query_condition_invoicer('ah')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'account_history_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)
        
        # end BQ Schema sync
        raw_data.rename(columns = {'uuid' : 'account_history_id'
                                  }, inplace = True)
        raw_data['is_deleteable'] = raw_data['is_deleteable'].astype(int)
        print(raw_data.info())
        # Store to BQ
        store_bq(raw_data, dataset, table_name)


    
def raw_data_finance_account_history():
    dataset = 'datascience_public'
    table_name = 'raw_data_finance_account_history'
    schema1    = read_sql('describe paper_invoicer.finance_account_history')
    schema2    = read_sql('describe paper_invoicer.finance_accounts')
    schema3    = read_sql('describe paper_invoicer.finance_account_types')
    
    schema3.rename(columns = {'name'           : 'finance_account_type_name'
                             }, inplace = True)
    schema = pd.concat([schema1,schema2,schema3]).drop_duplicates(subset = ['Field'], keep='first').reset_index(drop=True)
    raw_data   = read_sql("""select 
                            fah.*,
                            fa.currency_id,
                            fa.finance_account_type_id,
                            fa.cash_code,
                            fa.cash_name,
                            fa.bank_name,
                            fa.bank_account_no,
                            fa.bank_account_name,
                            fa.bank_branch_name,
                            fa.bank_city_name,
                            fat.name as finance_account_type_name

                            from paper_invoicer.finance_account_history as fah 
                            left join paper_invoicer.finance_accounts as fa on fah.finance_account_id = fa.uuid
                            left join paper_invoicer.finance_account_types as fat on fa.finance_account_type_id = fat.uuid
                            
                            {} #condition data gathering
                            
                            and (fah.debit_amount <> 0) and (fah.credit_amount <> 0) and (fah.debit_balance <> 0) and (fah.credit_balance <> 0)
                            """.format(query_condition_invoicer('fah')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)

        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'finance_account_history_id')
        
        # Hashing table
        list_yes1  = []
        list_yes2  = ['bank_account_name', 'bank_account_no']
        raw_data   = hashing_function(raw_data, list_yes1, list_yes2)

        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        # raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {'uuid' : 'finance_account_history_id'
                                  }, inplace = True)
        raw_data['created_at'] = pd.to_datetime(raw_data['created_at'], errors='coerce')
        raw_data['updated_at'] = pd.to_datetime(raw_data['updated_at'], errors='coerce')
        raw_data['bank_account_no'] = raw_data['bank_account_no'].astype(str)
        raw_data['bank_account_name'] = raw_data['bank_account_name'].astype(str)


        print(raw_data.info())
        # Store to BQ
        store_bq(raw_data, dataset, table_name)


def raw_data_invoice_down_payments():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoice_down_payments'
    schema     = read_sql("describe paper_invoicer.invoice_down_payments")
    raw_data   = read_sql("""
    select
        idp.*,
        invoice_dp_items.invoice_dp_amount
    from paper_invoicer.invoice_down_payments as idp
    left join (
        select 
            idpi.invoice_down_payment_id,
            sum(amount) as invoice_dp_amount
        from paper_invoicer.invoice_down_payment_items as idpi
        group by invoice_down_payment_id
    ) as invoice_dp_items on invoice_dp_items.invoice_down_payment_id = idp.uuid
    
    {} #condition data gathering
    
    -- limit 10
    """.format(query_condition_invoicer('idp')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_down_payment_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        print(raw_data.info())
        ###ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
         'uuid'                     : 'invoice_down_payment_id'
        }, inplace = True)
        print(raw_data.info())

        raw_data['assigned_status'] = raw_data['assigned_status'].astype(int)
        raw_data['sent'] = raw_data['sent'].astype(int)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)


def raw_data_invoice_down_payment_items():
    dataset = 'datascience_public'
    table_name = 'raw_data_invoice_down_payment_items'
    raw_data_invoice_down_payments
    schema     = read_sql("describe paper_invoicer.invoice_down_payment_items")
    raw_data   = read_sql("""
    select
    idpi.*
    from paper_invoicer.invoice_down_payment_items as idpi
        
    {} #condition data gathering
    """.format(query_condition_invoicer('idpi')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'invoice_down_payment_item_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        print(raw_data.info())
        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)

        raw_data.rename(columns = {
            'uuid'                     : 'invoice_down_payment_item_id'
            }, inplace = True)
        raw_data['index'] = raw_data['index'].astype(int)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)



def raw_data_company_type():
    dataset = 'datascience_public'
    table_name = 'raw_data_company_type'
    schema     = read_sql("describe paper_gateway.company_type")
    raw_data   = read_sql("""
    select
    ct.*,
    timestampadd(hour, 14, ct.created_at) as created_at_temp,
    timestampadd(hour, 14, ct.updated_at) as updated_at_temp

    from paper_gateway.company_type as ct
        
    {} #condition data gathering
    
    """.format(query_condition_invoicer('ct')))
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        raw_data = raw_data[raw_data.columns[~raw_data.columns.isin([
            'created_at',
            'updated_at'
        ])]]
        raw_data.rename(columns = {
            'created_at_temp'          : 'created_at',
            'updated_at_temp'          : 'updated_at'
            }, inplace = True)

        list_yes1 = []
        list_yes2 = []

        raw_data = hashing_function(raw_data, list_yes1, list_yes2)
        
        ##delete all data updated
        delete_data(dataset, table_name, raw_data, 'uuid')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')

        ####ubah type data
        raw_data = datatype_sync(raw_data,schema)
        
        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_bq(raw_data, dataset, table_name)



def raw_data_company_referral_used():
    dataset = 'datascience_public'
    table_name = 'raw_data_company_referral_used'
    raw_data   = read_sql("""
    select 
    *
    from paper_gateway.company_referral_used
    """)

    raw_data.rename(columns={"uuid": "company_referral_used_id"}, inplace=True)
    print("df.info : \n", raw_data.info())

    store_backup_gcs(raw_data, table_name)
    store_replace_bq(raw=raw_data, dataset=dataset, table_name=table_name)



def raw_data_referral_codes():
    dataset = 'datascience_public'
    table_name = 'raw_data_referral_codes'
    schema    = read_sql("describe paper_gateway.referral_codes")
    sql_format = """
    select 
    ref.*, 
    company_name, 
    company_email 
    from paper_gateway.referral_codes as ref
    left join (
        select 
        uuid, 
        company_name, 
        company_email 
        from paper_gateway.companies) as com 
    on ref.company_id = com.uuid
    """
    print("sql_format : \n", sql_format)

    raw_data   = read_sql(sql_format)
    
    ### cek data kosong/tidak
    if len(raw_data) == 0:
        print(table_name + "is empty")
    
    else:
        print("updating data " + table_name)
        
        store_backup_gcs(raw_data, table_name)

        raw_data.rename(columns = {
         'uuid'                     : 'referral_code_id'
        }, inplace = True)

        print(raw_data.dtypes)

        print('\n\nraw_data.info : ', raw_data.info(), '\n\n')
        store_replace_bq(raw_data, dataset, table_name)




def raw_data_imported_invoice_batches():
    dataset = 'datascience_public'
    table_name = 'raw_data_imported_invoice_batches'
    schema = read_sql("describe paper_invoicer.imported_invoice_batches")
    sql = """
    select 
    imported_invoice_batches.uuid as uuid, 
    imported_invoice_batches.company_id, 
    imported_invoice_batches.created_at, 
    imported_invoice_batches.file_path,
    imported_invoice_batches.import_status, 
    imported_invoice_batches.updated_at, 
    imported_invoice_batch_invoice.invoice_id

    from paper_invoicer.imported_invoice_batches
    left join paper_invoicer.imported_invoice_batch_invoice
    on imported_invoice_batches.uuid = imported_invoice_batch_invoice.batch_id
    {}

    """.format(query_condition_invoicer_noaddtime('imported_invoice_batches'))
    print(sql)
    raw_data   = read_sql(sql)
    print(raw_data.info())

    if len(raw_data) == 0:
        print(table_name + "is empty")
    else:

        delete_data(dataset, table_name, raw_data, 'batch_id')
        store_backup_gcs(raw_data, table_name)
        raw_data = pd.read_csv('/home/airflow/gcs/data/backup/raw_data/' + str(table_name) + '/{}_{}.csv'.format(table_name, datenow), encoding = 'utf-8')
        print("raw_data info : \n", raw_data.info())

        raw_data = datatype_sync(raw_data,schema)
        print("raw_data info after sync : ", raw_data.info())
        
        raw_data.rename(columns = {
         'uuid'                     : 'batch_id'
        }, inplace = True)

        print("raw_data info : \n", raw_data.info())
        store_bq(raw_data, dataset, table_name)




default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'owner': 'data-team',
    "on_failure_callback": task_fail_slack_alert
}

dag = DAG('raw_data_hashing_update', 
          description='raw_data_hashing_update', 
          default_args=default_args,
          schedule_interval="0 19 * * *",
          tags=['raw_data_table', 'production', 'data-team'],
          catchup=False)

raw_data_companies_no_hash = PythonOperator(
    task_id='raw_data_companies_no_hash',
    python_callable= raw_data_companies_no_hash,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_users_no_hash = PythonOperator(
    task_id='raw_data_users_no_hash',
    python_callable= raw_data_users_no_hash,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)


raw_data_subscriptions = PythonOperator(
    task_id='raw_data_subscriptions',
    python_callable= raw_data_subscriptions,
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_plans = PythonOperator(
    task_id='raw_data_plans',
    python_callable= raw_data_plans,
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_schedulers = PythonOperator(
    task_id='raw_data_schedulers',
    python_callable= raw_data_schedulers,
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_user_comment_meta_setting = PythonOperator(
    task_id='raw_data_user_comment_meta_setting',
    python_callable= raw_data_user_comment_meta_setting,
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_companies = PythonOperator(
    task_id='raw_data_companies',
    python_callable= raw_data_companies,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_users = PythonOperator(
    task_id='raw_data_users',
    python_callable= raw_data_users,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoices = PythonOperator(
    task_id='raw_data_invoices',
    python_callable= raw_data_invoices,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_payments = PythonOperator(
    task_id='raw_data_payments',
    python_callable= raw_data_payments,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_payments_invoices = PythonOperator(
    task_id='raw_data_payments_invoices',
    python_callable= raw_data_payments_invoices,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_orders = PythonOperator(
    task_id='raw_data_orders',
    python_callable= raw_data_orders,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_partners = PythonOperator(
    task_id='raw_data_partners',
    python_callable= raw_data_partners,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_products = PythonOperator(
    task_id='raw_data_products',
    python_callable= raw_data_products,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_expenses = PythonOperator(
    task_id='raw_data_expenses',
    python_callable= raw_data_expenses,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_stock_fifo_inputs = PythonOperator(
    task_id='raw_data_stock_fifo_inputs',
    python_callable= raw_data_stock_fifo_inputs,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_stock_fifo_outputs = PythonOperator(
    task_id='raw_data_stock_fifo_outputs',
    python_callable= raw_data_stock_fifo_outputs,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_stock_movements = PythonOperator(
    task_id='raw_data_movements',
    python_callable= raw_data_stock_movements,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_stock_documents = PythonOperator(
    task_id='raw_data_stock_documents',
    python_callable= raw_data_stock_documents,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_locations = PythonOperator(
    task_id='raw_data_locations',
    python_callable= raw_data_locations,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_finance_transactions = PythonOperator(
    task_id='raw_data_finance_transactions',
    python_callable= raw_data_finance_transactions,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_account_histories = PythonOperator(
    task_id='raw_data_account_histories',
    python_callable= raw_data_account_histories,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_finance_account_history = PythonOperator(
    task_id='raw_data_finance_account_history',
    python_callable= raw_data_finance_account_history,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoice_items = PythonOperator(
    task_id='raw_data_invoice_items',
    python_callable= raw_data_invoice_items,
    trigger_rule="all_done",
    dag=dag )

raw_data_order_items = PythonOperator(
    task_id='raw_data_order_items',
    python_callable= raw_data_order_items,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_shorten_urls = PythonOperator(
    task_id='raw_data_shorten_urls',
    python_callable= raw_data_shorten_urls,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoice_reminder_histories = PythonOperator(
    task_id='raw_data_invoice_reminder_histories',
    python_callable= raw_data_invoice_reminder_histories,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoice_reminder_settings = PythonOperator(
    task_id='raw_data_invoice_reminder_settings',
    python_callable= raw_data_invoice_reminder_settings,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_mailgun_trackings = PythonOperator(
    task_id='raw_data_mailgun_trackings',
    python_callable= raw_data_mailgun_trackings,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_sms_logs = PythonOperator(
    task_id='raw_data_sms_logs',
    python_callable= raw_data_sms_logs,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_partners_no_hashing = PythonOperator(
    task_id='raw_data_partners_no_hashing',
    python_callable= raw_data_partners_no_hashing,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoice_down_payments = PythonOperator(
    task_id='raw_data_invoice_down_payments',
    python_callable= raw_data_invoice_down_payments,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_invoice_down_payment_items = PythonOperator(
    task_id='raw_data_invoice_down_payment_items',
    python_callable= raw_data_invoice_down_payment_items,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_company_type = PythonOperator(
    task_id='raw_data_company_type',
    python_callable= raw_data_company_type,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)


raw_data_company_referral_used = PythonOperator(
    task_id='raw_data_company_referral_used',
    python_callable= raw_data_company_referral_used,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_referral_codes = PythonOperator(
    task_id='raw_data_referral_codes',
    python_callable= raw_data_referral_codes,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)

raw_data_imported_invoice_batches = PythonOperator(
    task_id='raw_data_imported_invoice_batches',
    python_callable= raw_data_imported_invoice_batches,
    trigger_rule="all_done",
    dag=dag,
    on_success_callback=task_success_slack_alert
)


raw_data_companies             >> raw_data_payments                     >> raw_data_finance_account_history
raw_data_users                 >> raw_data_payments_invoices            >> raw_data_locations                       >> raw_data_company_type
raw_data_products              >> raw_data_invoices                     >> raw_data_subscriptions
raw_data_expenses              >> raw_data_stock_movements              >> raw_data_plans                           >> raw_data_invoice_down_payment_items
raw_data_orders                >> raw_data_stock_fifo_inputs            >> raw_data_schedulers
raw_data_partners              >> raw_data_stock_fifo_outputs           >> raw_data_user_comment_meta_setting       >> raw_data_invoice_down_payments
raw_data_account_histories     >> raw_data_order_items
raw_data_finance_transactions  >> raw_data_stock_documents              >> raw_data_invoice_items
raw_data_shorten_urls          >> raw_data_invoice_reminder_histories   >>  raw_data_invoice_reminder_settings
raw_data_mailgun_trackings     >> raw_data_users_no_hash                >> raw_data_companies_no_hash               >>  raw_data_sms_logs 
raw_data_partners_no_hashing
raw_data_company_referral_used
raw_data_referral_codes
raw_data_imported_invoice_batches
