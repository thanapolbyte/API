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
                        'Database=MinorPOS;'
                        'user=trading;'
                        'password=Tr@ding;'
                        'Trusted_Connection=yes;')
        # print('SQL Database connection successful')
    except :
        print('Error: Connection')
    return conn


def API_KEYs():

    sql_query5 = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'API_KEY_test'
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
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'definitionId'
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
    # print(data.decode("utf-8"))
    print(res.status)
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
                INSERT INTO MinorPOS.dbo.xxx_Infobip_event_20_log (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'Event',
                str(externalId),
                str(externalId.split('_')[0]),
                str(personid),
                # str(payload4),
                '',
                str(res.status),
                str(res.reason),
                str(data.decode("utf-8")),
                # str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    # all(elem in res.status  for elem in options)
    if res.status == 201 :
            query2 = "Update [xxx_Infobip_Event_Sale_Detail_20] set status = 'Y', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            query3 = "Update [xxx_Infobip_Event_Sale_Detail_20] set status = 'E', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query3)
            # conn1.commit()
            # cursor.execute('''Infobip_Event_Sale_Detail_Bad_Request''')

    conn1.commit()
    conn1.close  

def filename():
    sql_query = pd.read_sql_query(f''' 
                              select Infobip_Sale_Code + '_' + Customer_ID as salecode_personid FROM [xxx_Infobip_Event_Sale_Detail_20] where status ='N'
                              order by Infobip_Sale_Code + '_' + Customer_ID
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    # df['sale_code'] = df['sale_code'].astype(str)
    sql_connect().commit()
    return df['salecode_personid'].values.tolist()

# 
def query(salecode_personid):
    conn_string = sql_connect()
    conn = conn_string

    upload_data_pd = pd.DataFrame()
    
    query = "select itemRuleBase,VoucherCode,VoucherName,itemDepartment,itemCategory,itemSKU,Total_Qty,Store_Code,channel,Total_Amount,Brand,point,transactionDate,Receipt_Number FROM [xxx_Infobip_Event_Sale_Detail_20] where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(salecode_personid) +"'"

    sql_query = pd.read_sql_query(query,conn)
    # objects_list =[]
    conn.close()
    df = pd.DataFrame(sql_query)

    df['itemRuleBase'] = df['itemRuleBase'].astype(str)
    # a['Date'] = pd.to_datetime(a['Date'], format='%Y-%m-%d %H:%M:%S').astype(str)
    df['VoucherCode'] = df['VoucherCode'].astype(str)
    df['VoucherName'] = df['VoucherName'].astype(str)
    # df['itemQty'] = df['itemQty'].astype(str)
    # df['itemPrice'] = df['itemPrice'].astype(str)
    df['itemDepartment'] = df['itemDepartment'].astype(str)
    df['itemCategory'] = df['itemCategory'].astype(str)
    
    df['itemSKU'] = df['itemSKU'].astype(str)
    df['Total_Qty'] = df['Total_Qty'].astype(int)
    df['Store_Code'] = df['Store_Code'].astype(str)
    df['channel'] = df['channel'].astype(str)
    df['Total_Amount'] = df['Total_Amount'].astype(float)
    df['brand'] = df['Brand'].astype(str)
    df['point'] = df['point'].astype(int)
    # df['transactionDate'] = pd.to_datetime(df['transactionDate'], format='%Y-%m-%dT%H::%M::%S.%f').astype(str)
    df['transactionDate'] = df['transactionDate'].astype(str)+"+07:00"
    # df['transactionDate'] = df['transactionDate'].astype(str)
    df['Receipt_Number'] = df['Receipt_Number'].astype(str)

    upload_data_pd['properties'] = 'properties'
    upload_data_pd['itemRuleBase'] = df['itemRuleBase']
    upload_data_pd['voucherCode'] = df['VoucherCode']
    upload_data_pd['voucherName'] = df['VoucherName']
    # upload_data_pd['itemQty'] = df['itemQty']
    # upload_data_pd['itemPrice'] = df['itemPrice']
    # upload_data_pd['itemPrice'] = df['itemPrice'].apply("{:.02f}".format)
    upload_data_pd['itemDepartment'] = df['itemDepartment']
    upload_data_pd['itemCategory'] = df['itemCategory']
    # upload_data_pd['itemName'] = df['itemName']
    upload_data_pd['itemSKU'] = df['itemSKU']
    upload_data_pd['totalQty'] = df['Total_Qty']
    upload_data_pd['storeCode'] = df['Store_Code']
    upload_data_pd['channel'] = df['channel']
    # upload_data_pd['totalAmount'] = df['totalAmount'].apply("{:.02f}".format)
    upload_data_pd['totalAmount'] = df['Total_Amount']
    upload_data_pd['brand'] = df['Brand']
    upload_data_pd['point'] = df['point']
    upload_data_pd['transactionDate'] = df['transactionDate']
    upload_data_pd['transactionNumber'] = df['Receipt_Number']
    
    datas = upload_data_pd.to_json(orient="records", force_ascii=False,indent=4)
    
    # d2 = datas.replace('"properties : {":null', '"properties" : {')
    # print(datas)
    return  datas




if __name__ == '__main__':
    if filename() == [] :
        pass
    else :
        cities = filename()
        for city in cities:
            # time.sleep(3)
            api_POST(city)
# api_POST('09081-1339-11148_22222228')



