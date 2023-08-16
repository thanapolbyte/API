import http.client
import pyodbc
import pandas as pd
import time
from datetime import date, datetime, timedelta
import collections
import json
import sys





def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time


class MyError(Exception):
    def __init__(self, msg):
        print(msg)
        return Exception.__init__(self)
# if __name__ == "__main__":
#     raise MyError("HELP!")

timeout = 10
def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time

def check_db_connection():
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=10.201.84.56;'
                        'Database=MinorPOS;'
                        'user=trading;'
                        'password=Tr@ding;'
                        'Trusted_Connection=yes;')
                        # 'timeout=' + str(timeout) + ';')
        conn.close()
        return 1
    except pyodbc.Error:
        print("Error: Could not connect to database")
        sys.exit(1)



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
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'API_KEY_Test'
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

# print(definitionids)


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
    payload = json.dumps(query(externalId),sort_keys=True,default=str,indent=4)

    # payload1 = payload.replace("[", "")
    # payload2 = payload1.replace("]", "")
    # payload4 = payload3.replace(":,", "")
    # print(payload2)
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
    conn.request("POST", urls, payload, headers)
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
                INSERT INTO MinorPOS.dbo.xxx_Infobip_event_20_log_V1 (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'Event',
                str(externalId),
                str(externalId.split('_')[0]),
                str(personid),
                # str(payload),
                '',
                str(res.status),
                str(res.reason),
                str(data.decode("utf-8")),
                # str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    # all(elem in res.status  for elem in options)
    if res.status == 201 :
            query2 = "Update xxx_Infobip_Event_Sale_Detail_20_V1 set status = 'Y', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            # query3 = "Update xxx_Infobip_Event_Sale_Detail_20_V1 set status = 'E', Update_date_time = getdate() where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(externalId) +"'"
            # cursor = conn1.cursor()
            # cursor.execute(query3)
            # conn1.commit()
            cursor.execute('''Infobip_Event_Sale_Detail_Bad_Request''')

    conn1.commit()
    conn1.close  

def filename():
    sql_query = pd.read_sql_query(f''' 
                              select Infobip_Sale_Code + '_' + Customer_ID as salecode_personid FROM xxx_Infobip_Event_Sale_Detail where status ='N'
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
    cursor = conn.cursor()

    # upload_data_pd = pd.DataFrame()
    
    query = "select itemRuleBase,voucherCode,voucherName,itemDepartment,itemCategory,itemSKU,cast(Total_Qty as int) total_Qty,Store_Code,channel,total_Amount,brand,cast(point as int) point,transactionDate,Receipt_Number FROM xxx_Infobip_Event_Sale_Detail_20_V1 where Infobip_Sale_Code + '_' + Customer_ID = " +"'" + str(salecode_personid) +"'"
    cursor.execute(query) 
    # sql_query = pd.read_sql_query(query,conn)
    rows = cursor.fetchall()
    # objects_list =[]
    objects_list = []
    for row in rows:
    
    # df = pd.DataFrame(sql_query)
        totalAmount= float("{:.2f}".format(row[9]))
        # Accumulate_Purchase= float("{:.2f}".format(row[5]))
        # d2 = collections.OrderedDict()
        # d2["number"] = row[1]
        # d3 = collections.OrderedDict()
        # d3["address"] = row[2]
        d1 = collections.OrderedDict()
        # d["uuuuuu"] = row[3]
        d1["itemRuleBase"] = row[0]
        d1["voucherCode"] = row[1]
        d1["voucherName"]= row[2]
        d1["itemDepartment"]= row[3]
        d1["itemCategory"]=row[4]
        d1["itemSKU"]= row[5]
        d1["totalQty"]= row[6]
        d1["storeCode"]= row[7]
        d1["channel"]= row[8]
        d1["totalAmount"]= totalAmount
        d1["brand"]= row[10]
        d1["point"]= row[11]
        d1["transactionDate"]= row[12]+"+07:00"
        d1["transactionNumber"]= row[13]
        
        
        # d1["Member Level"]= row[8]
        # print(a)
        d = collections.OrderedDict()
        # d["id"] = row[0]
        # d["point"] = row[3]
        # d["phone"] = d1
        d["properties"] = d1
        # objects_list.append(d)
    
    conn.close()
    
    
    # print(objects_list)
    
    return d



# print(filename())
# if __name__ == '__main__':
#     if filename() == [] :
#         pass
#     else :
#         # cities = filename()
#         # for city in cities:
#         #     # time.sleep(3)
#         #     api_POST(city)


#         try :
#             API_KEY = (", ".join(API_KEYs()))
#             url = (", ".join(url()))

#             cities = filename()
#             for city in cities:
#             # time.sleep(3)
#             # print(time.sleep(2))
#                 api_POST(city)
#         except OSError as error :
#             print(error)

if __name__ == '__main__':
    
    if check_db_connection()  == 1:

        if filename() == [] :
            pass
        else :
            try :
                API_KEY = (", ".join(API_KEYs()))
                url = (", ".join(url()))

                cities = filename()
                for city in cities:
            # time.sleep(3)
            # print(time.sleep(2))
                    api_POST(city)
            except OSError as error :
                print(error)
    # raise MyError("HELP!")
    else :
        pass


# print(query('09011-2941-24897_300440133'))
# print(json.dumps(query('09011-2941-24897_300440133'),sort_keys=True,default=str,indent=4))


# cities = filename()
# for city in cities:
#     api_POST(city)

# print(query('09081-1339-11148_22222228'))

# api_POST('20261-1279-15703_22222228')



