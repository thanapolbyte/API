from ast import Num
import http.client
import json
from textwrap import indent
from numpy import number

import pyodbc
import pandas as pd
# import logging
import json
import collections
from datetime import date, datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning
import time
# import base64


# API_KEY = 'App 367574ed8d437adb53294df7de5bded4-78af12e8-d66c-4ef5-bfb4-81c94bd68834'

class MyError(Exception):
    def __init__(self, msg):
        print(msg)
        return Exception.__init__(self)
# if __name__ == "__main__":
#     raise MyError("HELP!")


def get_local_time():
    local_time = str(time.ctime(time.time()))[11:20]
    return local_time

def sql_connect():
    conn = None
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=10.201.84.56;'
                        'Database=MinorPOS;'
                        'user=trading;'
                        'password=Tr@ding;'
                        'Trusted_Connection=yes;')
        # print('SQL Database connection successful')
    # except :
    #     print('Error: Connection')
    except OSError as error :
        print(error)
    #     print('Error: Connection')
    return conn

def API_KEYs():
    try :
        sql_query5 = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'API_KEY_test'
                              ''' 
                              ,sql_connect())
        df = pd.DataFrame(sql_query5)
        df['values_type'] = df['values_type'].astype(str)
        sql_connect().commit()
    except OSError as error :
        print(error)
    # finally :
    #     print("gggg")
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

def MemberID(level):
    conn_string = sql_connect()
    conn = conn_string

    
    query = "SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = " +"'" + str(level) +"'"

    sql_query = pd.read_sql_query(query,conn)
    # objects_list =[]
    conn.close()
    df = pd.DataFrame(sql_query)
    # sql_query = pd.read_sql_query(f''' 
    #                           SELECT  [values_type] FROM [dbo].[Infobip_config] where Types = ''' + "'" + str(id) +"'"
    #                           ''' 
    #                           ,sql_connect())
    # df = pd.DataFrame(sql_query)
    # df['Types'] = df['Types'].astype(str)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect().commit()
    return df['values_type'].to_string(index=False)



# print(API_KEY)



# MemberLevel = (MemberID('001'))
# # print(MemberLevel['Types'])



# print(MemberLevel)

def query(id):
    conn_string = sql_connect()
    conn = conn_string
    cursor = conn.cursor()
    query = "SELECT Last_Updated_Date,Last_Updated_By,Card_Expiry_Date,CAST([point] as int) as point,All_Accumulate_Purchase as All_Accumulate_Purchase,[Accumulate_Purchase],[Member_Level],[Customer_id],[Card_Date],[Expiring_Point],[Point_Expiry_Date],[Most_Visited_Brand],[Last_Visited_Date] FROM [Infobip_Update_Customer]  where  Customer_id =" +"'" + str(id) +"'"
    cursor.execute(query) 
    rows = cursor.fetchall()
    
    # data = list(rows)
    objects_list = []
   
    for row in rows:
        All_Accumulate_Purchase= float("{:.2f}".format(row[4]))
        Accumulate_Purchase= float("{:.2f}".format(row[5]))
        Member_level = MemberID(row[6])
        Member_desc = MemberID(row[6]) + " Card Date"
        # d2 = collections.OrderedDict()
        # d2["number"] = row[1]
        # d3 = collections.OrderedDict()
        # d3["address"] = row[2]
        d1 = collections.OrderedDict()
        # d["uuuuuu"] = row[3]
        d1["Last Updated Date"] = row[0]+"+07:00"
        d1["Last Updated By"] = row[1]
        # if MemberID(row[6]):
        d1[Member_desc] = row[8]
        # if row[6] == '001':
        #     d1["Classic Card Date"] = row[8]
        # elif row[6] == '002':
        #     d1["Silver Card Date"] = row[8]
        # elif row[6] == '003':
        #     d1["Gold Card Date"]= row[8]
        d1["Card Expiration Date"]= row[2]
        d1["Point"]= row[3]
        d1["All Accumulate Purchase"]=All_Accumulate_Purchase
        
        d1["Accumulate Purchase"]= Accumulate_Purchase
        d1["Member Level"] = Member_level
        if row[9] is not None:
            d1["Expiring Point"] = row[9]
        # d1["Expiring Point"] = row[9]
        if row[10] is not None:
            d1["Point Expiry Date"]= row[10]
        if row[11] is not None:
            d1["Most Visited Brand"]= row[11]
        if row[12] is not None:
            d1["Last Visited Date"]= row[12]
        # if row[6] == '001':
        #     d1["Member Level"]= 'Classic'
        # elif row[6] == '002':
        #    d1["Member Level"] = 'Silver'
        # elif row[6] == '003':
        #     d1["Member Level"]= 'Gold'

        
        # d1["Member Level"]= row[8]
        # print(a)
        d = collections.OrderedDict()
        # d["id"] = row[0]
        # d["point"] = row[3]
        # d["phone"] = d1
        d["customAttributes"] = d1
        # objects_list.append(d1)
        # d["customAttributes"] = objects_list
    conn.close()
    
    
    # print(objects_list)
    
    return d


def API_PATCH(externalId):
    payload = json.dumps(query(externalId),sort_keys=True,default=str,indent=4)
    return payload
    # payload1 = payload.replace("[", "")
    # payload2 = payload1.replace("]", "")
    # encoded = base64.b64encode(payload2.encode('utf-8'))
    # print(encoded)
    # print(payload2)
    # conn = http.client.HTTPSConnection(url)
    # urls = "/people/2/persons?externalId="+ str(externalId)
    # headers = {
    #     'Authorization': API_KEY,
    #     'Content-Type': 'application/json',
    #     'Accept': 'application/json'
    # }
    # conn.request("PATCH", urls, payload, headers)
    # res = conn.getresponse()
    # res.status
    # data = res.read()
    # # print(res.headers)
    # # print(payload2)
    # print(res.reason)
    # # print(res.headers)
    # # print(payload2)
    # # data = res.read()
    # # d1 = (res.headers)
    # # logging.basicConfig(filename='thailand_sales.log', level=logging.INFO)
    # #     # logging.info('Started ' + str(date.today()) + '|' + str(get_local_time()))
    # # logging.info('|' +str(externalId) + '|' + str(d1) + '|status: ' + str(res.status) + '|' + 'Date = ' +str(date.today()) + '|' + str(get_local_time()))

    # conn_string = sql_connect()
    # conn = conn_string
    # cursor = conn.cursor()

    # # cursor.execute('TRUNCATE TABLE rci_interface_aftersale')

    # # for row in df.itertuples():
    # cursor.execute('''
    #             INSERT INTO [Infobip_customer_log] (Customer_id,datas,status,reason,decode,interface_dates)
    #             VALUES (?,?,?,?,?,?)
    #             ''',
    #             str(externalId),
    #             str(payload),
    #             # encoded,
    #             str(res.status),
    #             str(res.reason),
    #             # str(res.headers),
    #             str(data.decode("utf-8")),
    #             str(date.today()) +' '+ str(get_local_time())
    #             )

    # if res.status == 200 :
    #         query2 = "Update [Infobip_Update_Customer] set Status = 'Y', Update_Date_time = getdate() where [Customer_id] = " +"'" + str(externalId) +"'"
    #         # cursor = conn.cursor()
    #         cursor.execute(query2)
    #         # conn1.commit()
    # else:
    #         # query3 = "Update [Infobip_Update_Customer] set Status = 'E', Update_Date_time = getdate() where [Customer_id] = " +"'" + str(externalId) +"'"
    #         # cursor = conn.cursor()
    #         # cursor.execute(query3)
    #         # conn1.commit()
    #         cursor.execute('''Infobip_Update_Customer_Bad_Request''')
    

    # conn.commit()

    # conn.close()
                




def filename():
    sql_query = pd.read_sql_query(f''' 
                              select Customer_id  FROM [Infobip_Update_Customer] where Status = 'N' ORDER BY Last_Updated_Date DESC
                              ''' 
                              ,sql_connect())
    df = pd.DataFrame(sql_query)
    df['Customer_id'] = df['Customer_id'].astype(str)
    sql_connect().commit()
    
    return df['Customer_id'].values.tolist()
# aa = sql_connect()
# print(len(aa))

# if sql_connect() == '' :
#     print("False")
# else :
#     print("True")




# # print(json.dumps(query(101000007),sort_keys=True,default=str,indent=4))

# if __name__ == '__main__':
    
#     if filename() == [] :
#         pass
#     else :
#         try :
#             API_KEY = (", ".join(API_KEYs()))
#             url = (", ".join(url()))

#             cities = filename()
#             for city in cities:
#             # time.sleep(3)
#             # print(time.sleep(2))
#                 API_PATCH(city)
#         except OSError as error :
#             print(error)
#     # raise MyError("HELP!")


# cities = filename()
# for city in cities:
# # print(filename())
#     # payload = json.dumps(query(city),sort_keys=True,default=str,indent=4)

#     # payload1 = payload.replace("[", "")
#     # payload2 = payload1.replace("]", "")
#     API_PATCH(city)
# API_PATCH('101149514')

# print(query(300504889))

print(API_PATCH(300440658))


print(API_PATCH(300504888))


