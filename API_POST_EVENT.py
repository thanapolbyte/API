import http.client
import pyodbc
import pandas as pd
import time
from datetime import date, datetime, timedelta





def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time



# API_KEY = 'App 367574ed8d437adb53294df7de5bded4-78af12e8-d66c-4ef5-bfb4-81c94bd68834'
def sql_connect():
    conn = None
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=SMDC2RCEGDB02;'
                        'Database=RCI_Cegid;'
                        'user=trading;'
                        'password=Tr@ding;'
                        'Trusted_Connection=yes;')
        print('SQL Database connection successful')
    except :
        print('Error: Connection')
    return conn


def API_KEYs():

    sql_query5 = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [RCI_Cegid].[dbo].[Infobit_config] where Types = 'API_KEY'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query5)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

def url():
    
    sql_query = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [RCI_Cegid].[dbo].[Infobit_config] where Types = 'url'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

def definitionId():
    
    sql_query = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [RCI_Cegid].[dbo].[Infobit_config] where Types = 'definitionId'
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


definitionids = (", ".join(definitionId()))

print(definitionids)


# payload = json.dumps({
#             "itemRuleBase":"",
#             "promoCode":"Minor1",
#             "itemQty": 2,
#             "itemPrice":200,
#             "itemDepartment":"Men",
#             "itemCategory":"shirt",
#             "itemName":"oop",
#             "itemSKU": "456325660888",
#             "totalQty":10,
#             "storeCode": "2921",
#             "channel": "offline",
#             "totalAmount":5236.55,
#             "brand": "Anello",
#             "transactionDate": "2022-08-03",
#             "transactionNumber": "REop00000000022"
#         } )
def api_POST(externalId):
    payload = query(externalId)
    payload1 = payload.replace("[", "")
    payload2 = payload1.replace("]", "")
    payload3 = payload2.replace("null,", "{")
    payload4 = payload3.replace("}", "}\n}")
    # payload4 = payload3.replace(":,", "")
    
    personid = externalId.split('_')[1]
    conn = http.client.HTTPSConnection(url)
    # urls = "/people/2/persons?externalId="+ str(externalId)
    urls = "/peopleevents/1/persons/"+ str(personid) + "/definitions/" + str(definitionids) + "/events?personIdentifierType=EXTERNAL_ID"
    # print(urls)
    # urls = "/peopleevents/1/persons/"+ str(personid) + "/definitions/TestPurchaseEvent1/events"
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request("POST", urls, payload4, headers)
    res = conn.getresponse()
    
    res.reason
    data = res.read()
    conn.close()
    print(data.decode("utf-8"))
    # print(payload4)
    # print(externalId.split('_')[0])
    # print(externalId.split('_')[1])
    # # print(payload3)
    # print(res)
    # fff = res.headers
    # print(res.status)
    # print(res.reason)
    # print(res.geturl)
    # print(res.debuglevel)
    # print(fff.find("X-Request-Id"))
    # print(res.info())
    # print(res.msg)
    

    conn_string = sql_connect()
    conn1 = conn_string
    # conn2 = conn_string

    
           
    # else:
    #     print("false")

    cursor = conn1.cursor()
    cursor.execute('''
                INSERT INTO RCI_Cegid.dbo.Infobit_event_test_log (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'Event',
                str(externalId),
                str(externalId.split('_')[0]),
                str(personid),
                str(payload4),
                str(res.status),
                str(res.reason),
                str(data.decode("utf-8")),
                # str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    # all(elem in res.status  for elem in options)
    if res.status == 201 :
            query2 = "Update Infobit_event_test set status = 'Y', Update_date_time = getdate() where sale_code + '_' + personId = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            query3 = "Update Infobit_event_test set status = 'E', Update_date_time = getdate() where sale_code + '_' + personId = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query3)
            # conn1.commit()

    conn1.commit()
    conn1.close  

def filename():
    sql_query = pd.read_sql_query(f''' 
                              select sale_code + '_' + personId as salecode_personid,saleline_id, personId,sale_code FROM Infobit_event_test where status ='N'
                              order by sale_code
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['sale_code'] = df['sale_code'].astype(str)
    sql_connect().commit()
    return df['salecode_personid'].values.tolist()

# 
def query(salecode_personid):
    conn_string = sql_connect()
    conn = conn_string
    # cursor = conn.cursor()
    # # cursor.execute("SELECT [Last Updated Date],[Last Updated By],[Silver Card Date],[Card Expiration Date],[Point],[Classic Card Date],CAST([Accumulate Purchase] AS int) as [Accumulate Purchase] ,[Gold Card Date],[Member Level],[External person ID] FROM test_customer_Infobit  where [External person ID] =" + str(saleline_personid))
    

    upload_data_pd = pd.DataFrame()
    
    # query = "select itemRuleBase,promoCode,itemQty,itemPrice,itemDepartment,itemCategory,itemName,itemSKU,totalQty,storeCode,channel,totalAmount,brand,transactionDate,transactionNumber FROM Infobit_event_test where sale_code + '_' + personId = " +"'" + str(salecode_personid) +"'"
    query = "select itemRuleBase,voucherCode,voucherName,itemDepartment,itemCategory,itemSKU,totalQty,storeCode,channel,totalAmount,brand,point,transactionDate,transactionNumber FROM Infobit_event_test where sale_code + '_' + personId = " +"'" + str(salecode_personid) +"'"

    
    
    sql_query = pd.read_sql_query(query,conn)
    # objects_list =[]
    conn.close()
    df = pd.DataFrame(sql_query)

    df['itemRuleBase'] = df['itemRuleBase'].astype(str)
    # a['Date'] = pd.to_datetime(a['Date'], format='%Y-%m-%d %H:%M:%S').astype(str)
    df['voucherCode'] = df['voucherCode'].astype(str)
    df['voucherName'] = df['voucherName'].astype(str)
    # df['itemQty'] = df['itemQty'].astype(str)
    # df['itemPrice'] = df['itemPrice'].astype(str)
    df['itemDepartment'] = df['itemDepartment'].astype(str)
    df['itemCategory'] = df['itemCategory'].astype(str)
    
    df['itemSKU'] = df['itemSKU'].astype(str)
    df['totalQty'] = df['totalQty'].astype(int)
    df['storeCode'] = df['storeCode'].astype(str)
    df['channel'] = df['channel'].astype(str)
    df['totalAmount'] = df['totalAmount'].astype(float)
    df['brand'] = df['brand'].astype(str)
    df['point'] = df['point'].astype(int)
    # df['transactionDate'] = pd.to_datetime(df['transactionDate'], format='%Y-%m-%dT%H::%M::%S.%f').astype(str)
    df['transactionDate'] = df['transactionDate'].astype(str)
    # df['transactionDate'] = df['transactionDate'].astype(str)
    df['transactionNumber'] = df['transactionNumber'].astype(str)

    upload_data_pd['properties'] = 'properties'
    upload_data_pd['itemRuleBase'] = df['itemRuleBase']
    upload_data_pd['voucherCode'] = df['voucherCode']
    upload_data_pd['voucherName'] = df['voucherName']
    # upload_data_pd['itemQty'] = df['itemQty']
    # upload_data_pd['itemPrice'] = df['itemPrice']
    # upload_data_pd['itemPrice'] = df['itemPrice'].apply("{:.02f}".format)
    upload_data_pd['itemDepartment'] = df['itemDepartment']
    upload_data_pd['itemCategory'] = df['itemCategory']
    # upload_data_pd['itemName'] = df['itemName']
    upload_data_pd['itemSKU'] = df['itemSKU']
    upload_data_pd['totalQty'] = df['totalQty']
    upload_data_pd['storeCode'] = df['storeCode']
    upload_data_pd['channel'] = df['channel']
    # upload_data_pd['totalAmount'] = df['totalAmount'].apply("{:.02f}".format)
    upload_data_pd['totalAmount'] = df['totalAmount']
    upload_data_pd['brand'] = df['brand']
    upload_data_pd['point'] = df['point']
    upload_data_pd['transactionDate'] = df['transactionDate']
    upload_data_pd['transactionNumber'] = df['transactionNumber']
    
    
    # objects_list.append(upload_data_pd)
    # print(df)
    datas = upload_data_pd.to_json(orient="records", force_ascii=False,indent=4)
    
    # d2 = datas.replace('"properties : {":null', '"properties" : {')
    # print(datas)
    return  datas

# print(filename())
# print(query('090112762276222462_22222228'))


# api_POST('090112762276222462_22222228')


cities = filename()
for city in cities:
    api_POST(city)



