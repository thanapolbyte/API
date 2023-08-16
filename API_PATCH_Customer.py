from ast import Num
import http.client
import json
from textwrap import indent
from numpy import number
import pyodbc
import pandas as pd
import logging
import json
import collections
from datetime import date, datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning
import time
import base64


# API_KEY = 'App 367574ed8d437adb53294df7de5bded4-78af12e8-d66c-4ef5-bfb4-81c94bd68834'

def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time

def sql_connect():
    conn = None
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=SMDC2RCEGDB02;'
                        'Database=MinorPOS;'
                        'user=trading;'
                        'password=Tr@ding;'
                        'Trusted_Connection=yes;')
        print('SQL Database connection successful')
    except :
        print('Error: Connection')
    return conn

def API_KEYs():
    
    sql_query5 = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'API_KEY'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query5)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

def url():
    
    sql_query = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'url'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

API_KEY = (", ".join(API_KEYs()))

url = (", ".join(url()))

def query(id):
    conn_string = sql_connect()
    conn = conn_string
    cursor = conn.cursor()
    query = "SELECT Last_Updated_Date,Last_Updated_By,Card_Expiry_Date,CAST([point] as int) as point,All_Accumulate_Purchase as All_Accumulate_Purchase,[Accumulate_Purchase],[Member_Level],[Customer_id],[Card_Date] FROM Infobip_Update_Customer  where  Customer_id =" +"'" + str(id) +"'"
    cursor.execute(query) 
    rows = cursor.fetchall()
    
    # data = list(rows)
    objects_list = []
   
    for row in rows:
        All_Accumulate_Purchase= float("{:.2f}".format(row[4]))
        Accumulate_Purchase= float("{:.2f}".format(row[5]))
        # d2 = collections.OrderedDict()
        # d2["number"] = row[1]
        # d3 = collections.OrderedDict()
        # d3["address"] = row[2]
        d1 = collections.OrderedDict()
        # d["uuuuuu"] = row[3]
        d1["Last Updated Date"] = row[0]+"+07:00"
        d1["Last Updated By"] = row[1]
        if row[6] == '001':
            d1["Classic Card Date"] = row[8]
        elif row[6] == '002':
            d1["Silver Card Date"] = row[8]
        elif row[6] == '003':
            d1["Gold Card Date"]= row[8]
        d1["Card Expiration Date"]= row[2]
        d1["Point"]= row[3]
        d1["All Accumulate Purchase"]=All_Accumulate_Purchase
        
        d1["Accumulate Purchase"]= Accumulate_Purchase
        
        if row[6] == '001':
            d1["Member Level"]= 'Classic'
        elif row[6] == '002':
           d1["Member Level"] = 'Silver'
        elif row[6] == '003':
            d1["Member Level"]= 'Gold'
        # d1["Member Level"]= row[8]
        # print(a)
        d = collections.OrderedDict()
        # d["id"] = row[0]
        # d["point"] = row[3]
        # d["phone"] = d1
        d["customAttributes"] = d1
        objects_list.append(d)
    conn.close()
    
    
    # print(objects_list)
    
    return objects_list


def API_PATCH(externalId):
    payload = json.dumps(query(externalId),sort_keys=True,default=str,indent=4)

    payload1 = payload.replace("[", "")
    payload2 = payload1.replace("]", "")
    # encoded = base64.b64encode(payload2.encode('utf-8'))
    # print(encoded)
    # print(payload2)
    conn = http.client.HTTPSConnection(url)
    urls = "/people/2/persons?externalId="+ str(externalId)
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request("PATCH", urls, payload2, headers)
    res = conn.getresponse()
    res.status
    data = res.read()
    # print(res.headers)
    # print(payload2)
    print(res.reason)
    # print(res.headers)
    # print(payload2)
    # data = res.read()
    # d1 = (res.headers)
    # logging.basicConfig(filename='thailand_sales.log', level=logging.INFO)
    #     # logging.info('Started ' + str(date.today()) + '|' + str(get_local_time()))
    # logging.info('|' +str(externalId) + '|' + str(d1) + '|status: ' + str(res.status) + '|' + 'Date = ' +str(date.today()) + '|' + str(get_local_time()))

    conn_string = sql_connect()
    conn = conn_string
    cursor = conn.cursor()

    # cursor.execute('TRUNCATE TABLE rci_interface_aftersale')

    # for row in df.itertuples():
    cursor.execute('''
                INSERT INTO [Infobip_customer_log] (Customer_id,datas,status,reason,decode,interface_dates)
                VALUES (?,?,?,?,?,?)
                ''',
                str(externalId),
                str(payload2),
                # encoded,
                str(res.status),
                str(res.reason),
                # str(res.headers),
                str(data.decode("utf-8")),
                str(date.today()) +' '+ str(get_local_time())
                )

    if res.status == 200 :
            query2 = "Update Infobip_Update_Customer set Status = 'Y', Update_Date_time = getdate() where [Customer_id] = " +"'" + str(externalId) +"'"
            # cursor = conn.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            # query3 = "Update Infobip_Update_Customer set Status = 'E', Update_Date_time = getdate() where [Customer_id] = " +"'" + str(externalId) +"'"
            # # cursor = conn.cursor()
            # cursor.execute(query3)
            # # conn1.commit()
            cursor.execute('''Infobip_Update_Customer_Bad_Request''')
    

    conn.commit()

    conn.close()
                




def filename():
    sql_query = pd.read_sql_query(f''' 
                              select distinct Customer_id  FROM Infobip_Update_Customer where Status = 'N'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['Customer_id'] = df['Customer_id'].astype(str)
    sql_connect().commit()
    
    return df['Customer_id'].values.tolist()




cities = filename()
for city in cities:
# print(filename())
    # payload = json.dumps(query(city),sort_keys=True,default=str,indent=4)

    # payload1 = payload.replace("[", "")
    # payload2 = payload1.replace("]", "")
    API_PATCH(city)
# API_PATCH('101222205')

# print(query(101222205))

