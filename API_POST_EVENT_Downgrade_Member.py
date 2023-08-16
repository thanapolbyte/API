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

def check_db_connection():
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=SMDC2RCEGDB02;'
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
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'Downgrade_Member_Level'
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].values.tolist()

# print(API_KEYs())

# o = print(API_KEYs())
# API_KEY = (", ".join(API_KEYs()))

# url = (", ".join(url()))


# definitionids = (", ".join(definitionId()))

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

    payload1 = payload.replace("[", "")
    payload2 = payload1.replace("]", "")
    # payload4 = payload3.replace(":,", "")
    # print(payload2)
    personid = externalId.split('_')[1]
    conn = http.client.HTTPSConnection(url)
    # urls = "/people/2/persons?externalId="+ str(externalId)
    urls = "/peopleevents/1/persons/"+ str(personid) + "/definitions/" + str(definitionids) + "/events?personIdentifierType=EXTERNAL_ID"
    print(urls)
    # urls = "/peopleevents/1/persons/"+ str(personid) + "/definitions/TestPurchaseEvent1/events"
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request("POST", urls, payload2, headers)
    res = conn.getresponse()
    # print(payload2)
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
                INSERT INTO MinorPOS.dbo.Infobip_event_log (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'Downgrade_Member_Level',
                str(externalId),
                str(externalId.split('_')[0]),
                str(personid),
                str(payload2),
                str(res.status),
                str(res.reason),
                str(data.decode("utf-8")),
                # str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    # all(elem in res.status  for elem in options)
    if res.status == 201 :
            query2 = "Update Infobip_Event_Member_Level set status = 'Y', Update_date_time = getdate() where [Event_type] = 'Downgrade_Member_Level' and MFC_IDCARTE +'_'+ Customer_ID = " +"'" + str(externalId) +"'"
            cursor = conn1.cursor()
            cursor.execute(query2)
            # conn1.commit()
    else:
            # query3 = "Update Infobip_Event_Member_Level set status = 'E', Update_date_time = getdate() where [Event_type] = 'Downgrade_Member_Level' and Customer_ID = " +"'" + str(externalId) +"'"
            # cursor = conn1.cursor()
            # cursor.execute(query3)
            # conn1.commit()
            cursor.execute('''Infobip_Event_Member_level_Bad_Request''')

    conn1.commit()
    conn1.close  

def filename():
    sql_query = pd.read_sql_query(f''' 
                              select MFC_IDCARTE +'_'+ Customer_ID as Customer_ID FROM Infobip_Event_Member_Level where status ='N' and [Event_type] = 'Downgrade_Member_Level'
                              order by Customer_ID
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    # df['sale_code'] = df['sale_code'].astype(str)
    sql_connect().commit()
    return df['Customer_ID'].values.tolist()

# 
def query(Customer_ID):
    conn_string = sql_connect()
    conn = conn_string
    cursor = conn.cursor()

    # upload_data_pd = pd.DataFrame()
    
    query = "SELECT [Event_type],[Customer_ID],[fromMemberLevel_code],[fromMemberLevel],[toMemberLevel_code],[toMemberLevel],[cardCreatedDate],[cardExpirationDate] FROM [Infobip_Event_Member_Level] where status = 'N' and [Event_type] = 'Downgrade_Member_Level' and MFC_IDCARTE +'_'+ Customer_ID = " +"'" + str(Customer_ID) +"'"
    # print (query)
    cursor.execute(query) 
    # sql_query = pd.read_sql_query(query,conn)
    rows = cursor.fetchall()
    # objects_list =[]
    objects_list = []
    for row in rows:
    
    # df = pd.DataFrame(sql_query)
        # totalAmount= float("{:.2f}".format(row[9]))
        # Accumulate_Purchase= float("{:.2f}".format(row[5]))
        # d2 = collections.OrderedDict()
        cardCreatedDate = row[6]+"+07:00"
        cardExpirationDate = row[7]+"+07:00"
        # d2["number"] = row[1]
        # d3 = collections.OrderedDict()
        # d3["address"] = row[2]
        d1 = collections.OrderedDict()
        # d["uuuuuu"] = row[3]
        d1["fromMemberLevel"] = row[3]
        d1["toMemberLevel"] = row[5]
        # d1["voucherName"]= row[2]
        # d1["itemDepartment"]= row[3]
        # d1["itemCategory"]=row[4]
        # d1["itemSKU"]= row[5]
        # d1["totalQty"]= row[6]
        # d1["storeCode"]= row[7]
        # d1["channel"]= row[8]
        # d1["totalAmount"]= totalAmount
        # d1["brand"]= row[10]
        # d1["point"]= row[11]
        d1["cardCreatedDate"]= cardCreatedDate
        d1["cardExpirationDate"]= cardExpirationDate
        
        
        # d1["Member Level"]= row[8]
        # print(a)
        d = collections.OrderedDict()
        # d["id"] = row[0]
        # d["point"] = row[3]
        # d["phone"] = d1
        d["properties"] = d1
        objects_list.append(d)
   
    conn.close()
    
    
    # print(objects_list)
    
    return objects_list


# if __name__ == '__main__':
#     if filename() == [] :
#         pass
#     else :
#         cities = filename()
#         for city in cities:
#             api_POST(city)

if __name__ == '__main__':
    
    if check_db_connection()  == 1:

        if filename() == [] :
            pass
        else :
            try :
                API_KEY = (", ".join(API_KEYs()))
                url = (", ".join(url()))
                definitionids = (", ".join(definitionId()))
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

# cities = filename()
# for city in cities:
#     api_POST(city)

# print(query('300129333'))

# api_POST('20261-1279-15703_22222228')



