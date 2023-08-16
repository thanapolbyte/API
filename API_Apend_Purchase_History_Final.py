import http.client
import json
# import collections
import pyodbc
import pandas as pd
# import os
import time
from datetime import date, datetime, timedelta
import urllib.request
import time






def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time



# API_KEY = 'App 367574ed8d437adb53294df7de5bded4-78af12e8-d66c-4ef5-bfb4-81c94bd68834'
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

def definitionId():
    
    sql_query = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'Purchase_History'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

# print(API_KEYs())

# o = print(API_KEYs())
API_KEY = (", ".join(API_KEYs()))

url = (", ".join(url()))
print(url)

definitionids = (", ".join(definitionId()))

print(definitionids)

# payload = json.dumps({
#                 "Brand": "Esprit",
#                 "Channel": "Web",
#                 "Total Qty": 10,
#                 "Store Code": "5096",
#                 "Total Amount": 20000,
#                 "Voucher Code": "4459",
#                 "Voucher Name": "abc89999",
#                 "Transaction Date": "2022-05-31T09:13:13Z",
#                 "Transaction Number": "ANE00000000005"
#         } )

def API_APEND(externalId):
    payload = query(externalId)
    payload1 = payload.replace("[", "")
    payload2 = payload1.replace("]", "")
    # payload1 = payload.replace("[", "")
    # payload2 = payload1.replace("]", "")
    # print(json.dumps(payload2,sort_keys=True,default=str,indent=4))
    personid = externalId.split('_')[1]
    conn = http.client.HTTPSConnection(url)
    urls = "https://19wxmd.api.infobip.com/people/2/customAttributes/" + str(definitionids) + "/append?externalId=" + str(personid)
    # urls = str("https://19wxmd.api.infobip.com/people/2/customAttributes/Purchase History/append?externalId=22222226")
    # print(urls)
    urls = urls.replace(" ", "%20")
    print(urls)
    # print(payload2)
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request("POST",urls , payload2, headers)
    res = conn.getresponse()
    conn.close()
    res.reason
    data = res.read()
    print(res.status)
    print(data.decode("utf-8"))
    print(res.reason)
    # print(res.headers)
    # print(payload2)
    # print(res.reason)
    # print(res.headers)
    
    # data = res.read()
    # d1 = (res.headers)
    # # logging.basicConfig(filename='thailand_sales.log', level=logging.INFO)
    # #     # logging.info('Started ' + str(date.today()) + '|' + str(get_local_time()))
    # # logging.info('|' +str(externalId) + '|' + str(d1) + '|status: ' + str(res.status) + '|' + 'Date = ' +str(date.today()) + '|' + str(get_local_time()))

    # conn_string = sql_connect()
    # conn = conn_string
    # cursor = conn.cursor()

    conn_string = sql_connect()
    conn1 = conn_string
    # conn2 = conn_string

    
           
    # else:
    #     print("false")

    cursor = conn1.cursor()
    cursor.execute('''
                INSERT INTO dbo.Infobip_event_log (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'Purchase History',
                str(externalId),
                str(externalId.split('_')[0]),
                str(personid),
                str(payload2),
                str(res.status),
                str(res.reason),
                # str(data.decode("utf-8")),
                str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    if res.status == 200 :
            query2 = "Update Infobip_Putchase_History set status = 'Y', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            query3 = "Update Infobip_Putchase_History set status = 'E', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query3)
            # conn1.commit()
    conn1.commit()
    conn1.close 
    # conn.close()


def query(salecode_personid):
    conn_string = sql_connect()
    conn = conn_string
    # cursor = conn.cursor()
    query = "select Receipt_Number,transactionDate,VoucherCode,VoucherName,Store_Code,Total_Amount,Total_Qty,channel,Brand FROM Infobip_Putchase_History where status = 'N' AND Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(salecode_personid) +"'"
    upload_data_pd = pd.DataFrame()

    sql_query = pd.read_sql_query(query,conn)
    # objects_list =[]
    conn.close()

    
    df = pd.DataFrame(sql_query)

    df['Brand'] = df['Brand'].astype(str)
    # a['Date'] = pd.to_datetime(a['Date'], format='%Y-%m-%d %H:%M:%S').astype(str)
    df['VoucherCode'] = df['VoucherCode'].astype(str)
    df['VoucherName'] = df['VoucherName'].astype(str)
    # df['itemQty'] = df['itemQty'].astype(str)
    # df['itemPrice'] = df['itemPrice'].astype(str)
    df['channel'] = df['channel'].astype(str)
    # df['itemCategory'] = df['itemCategory'].astype(str)
    
    # df['itemSKU'] = df['itemSKU'].astype(str)
    df['Total_Qty'] = df['Total_Qty'].astype(int)
    df['Store_Code'] = df['Store_Code'].astype(str)
    # df['channel'] = df['channel'].astype(str)
    df['Total_Amount'] = df['Total_Amount'].astype(float)
    # df['brand'] = df['brand'].astype(str)
    # df['point'] = df['point'].astype(int)
    # df['transactionDate'] = pd.to_datetime(df['transactionDate'], format='%Y-%m-%dT%H::%M::%S.%f').astype(str)
    df['transactionDate'] = df['transactionDate'].astype(str)
    # df['transactionDate'] = df['transactionDate'].astype(str)
    df['Receipt_Number'] = df['Receipt_Number'].astype(str)

    # upload_data_pd['properties'] = 'properties'
    upload_data_pd['Brand'] = df['Brand']
    
    # upload_data_pd['itemQty'] = df['itemQty']
    # upload_data_pd['itemPrice'] = df['itemPrice']
    # upload_data_pd['itemPrice'] = df['itemPrice'].apply("{:.02f}".format)
    upload_data_pd['Channel'] = df['channel']
    upload_data_pd['Total Qty'] = df['Total_Qty']
    upload_data_pd['Store Code'] = df['Store_Code']
    upload_data_pd['Total Amount'] = df['Total_Amount']
    upload_data_pd['Voucher Code'] = df['VoucherCode']
    upload_data_pd['Voucher Name'] = df['VoucherName']
    upload_data_pd['Transaction Date'] = df['transactionDate']
    upload_data_pd['Transaction Number'] = df['Receipt_Number']
    
    # print(upload_data_pd)
    
    # objects_list.append(upload_data_pd)
    # print(df)
    datas = upload_data_pd.to_json(orient="records", force_ascii=False,indent=4)
    # d2 = datas.replace('"properties : {":null', '"properties" : {')
    print(datas)
    return  datas

def filename():
    sql_query = pd.read_sql_query(f''' 
                              select Infobip_Sale_Code + '_' + Customer_ID as salecode_personid FROM Infobip_Putchase_History where status ='N'
                              order by Infobip_Sale_Code + '_' + Customer_ID
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    # df['sale_code'] = df['sale_code'].astype(str)
    sql_connect().commit()
    return df['salecode_personid'].values.tolist()


# print(query("21951-1535-14799_22222228"))
cities = filename()
for city in cities:
    # time.sleep(3)
    API_APEND(city)

