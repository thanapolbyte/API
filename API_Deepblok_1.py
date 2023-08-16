import json

import hmac

import hashlib

import base64

import requests

from datetime import datetime

import pyodbc

from decimal import Decimal

import collections

import time

import sys

import pandas as pd




# Constants

BRAND_CODE = "CEGID"

CLIENT_CODE = "MINORPLUS"

KEY_CODE = "MINORPLUS_CEGID"

HASH_KEY = "tNYNKObv5C8BXy2NpuHd9C2li8H/b6Ranqbq88aV9Z9MMRzB4hqiatDCKrfVc0Esisk0inB421ir+SjWLad6PM1m47cpFZVunFOctaEoGTxuGiJvHdpebCTFJEllx5Yto8gmYL/hvfBSkRQfNWo1su1RZwuMqJDBShteualUA+o="

BASE_URL = "https://test-lifestyle-api.deepblok.io/v1/icrm/core"

AUTHEN_URL = BASE_URL + "/authen"

PROMOTION_URL = BASE_URL + "/promotion"




class DecimalEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, Decimal):

            return str(obj)

        return super(DecimalEncoder, self).default(obj)

   

def format_money(value):

    # Format the value as (money format)

    return f"{value:.2f}"




def get_message_signature(client_code, key_code, request, secret):

    uri = '/authen/v1/clientToken/generateNew'

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    method = 'POST'

    token_type = 'HS'

    client_lib_version = "1.0.0"

    # timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    header_info = {

        "clientCode": client_code,

        "clientLibVersion": client_lib_version,

        "keyCode": key_code,

        "method": method,

        "timestamp": timestamp,

        "tokenType": token_type,

        "uri": uri,

    }




    raw_str = (

        client_code +

        client_lib_version +

        key_code +

        method +

        timestamp +

        token_type +

        uri +

        request

    )




    message = json.dumps(header_info)

    secret_buffer = base64.b64decode(secret)

    hmac_value = hmac.new(secret_buffer, raw_str.encode(), hashlib.sha512).hexdigest()




    header = base64.b64encode(message.encode()).decode()

    token = base64.b64encode((header + ":" + hmac_value).encode()).decode()




    return token




def make_request(url, headers, data=None):

    try:

        response = requests.post(url, headers=headers, json=data)

        response.raise_for_status()

        return response

    except requests.exceptions.HTTPError as e:

        raise Exception("HTTP Error:", str(e))

    except requests.exceptions.RequestException as e:

        raise Exception("Error making the request:", str(e))

   

   

def update_status(reqnum, status):

    conn = sql_connect()

   

    cursor = conn.cursor()

   

    cursor.execute("UPDATE DBLK_tender SET status = ? WHERE requestNumber = ?", (status, reqnum))

    cursor.execute("UPDATE DBLK_promotion SET status = ? WHERE requestNumber = ?", (status, reqnum))      

   

    conn.commit()

   

    cursor.close()

    conn.close()

   

   

def insert_log(code, httpStatus, message, server_time, request_ref, insert_datetime, membercode, request_number):

    conn = sql_connect()

   

    cursor = conn.cursor()

   

    cursor.execute("INSERT INTO DBLK_promotion_log (code, httpStatus, message, server_time, request_ref, insert_datetime, membercode, request_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",

                   (code, httpStatus, message, server_time, request_ref, insert_datetime, membercode, request_number))

   

    conn.commit()

   

    cursor.close()

    conn.close()

   




def sql_connect():

    try:

        timeout = 10

        conn = pyodbc.connect('Driver={SQL Server};'

                              'Server=SMDCRPOSDBDEV02;'

                              'Database=MinorPOS;'

                              'user=trading;'

                              'password=Tr@ding;'

                              'Trusted_Connection=yes;'

                              'timeout=' + str(timeout) + ';')

        return conn

    except pyodbc.Error as e:

        print('Error connecting to the database:', str(e))

        return None

   

def get_token():

    body = json.dumps({"brandCode": BRAND_CODE})

    # body = {"brandCode": "CEGID"}

    prepare_request_header = get_message_signature(CLIENT_CODE, KEY_CODE, body, HASH_KEY)

    # print(prepare_request_header)

    headers = {

        "Authorization": "Basic " + prepare_request_header,

        "Content-Type": "application/json",

    }

    # print(headers)

    # print(body)

    data = {"brandCode": "CEGID"}

    authen_response = make_request(AUTHEN_URL, headers=headers, data=data)




    if authen_response.status_code == 200:

        authen_result = authen_response.json()

        auth_token = authen_result["data"]["authToken"]

        date_of_expire = authen_result["data"]["dateOfExpire"]

        return auth_token

    else:

        raise Exception(authen_response.text)

# print(get_token())

def get_request_numbers():

    query = "SELECT DISTINCT requestNumber FROM DBLK_tender WHERE status = 'N'"

    sql_query = pd.read_sql_query(query, sql_connect())

    df = pd.DataFrame(sql_query)

    df['requestNumber'] = df['requestNumber'].astype(str)

    sql_connect().commit()

   

    return df['requestNumber'].values.tolist()

   

def get_items(request_number):

 

    conn_string = sql_connect()

    conn = conn_string

    cursor = conn.cursor()




    query = """

    SELECT distinct p.memberCode, p.requestNumber, p.billingImage, p.brandName, p.branchId, p.branchName, p.description, p.channel, p.staffID, p.staffName, p.terminalID, p.businessDate, p.purchaseDate, p.orderNumber, p.channelLocation, p.categoryName, p.itemCode, p.itemName, p.itemServicetype, p.quantity, p.unitPrice, p.discount, p.department, p.netAmount, p.totalAmountBeforeDiscount, p.discountAmount, p.totalAmountAfterDiscount, t.paymentChannel, t.paymentRef, t.paymentType, t.ref1, t.ref2, t.requestIPEndUser, t.requestNumberRef, t.requestType

    FROM DBLK_tender t

    LEFT JOIN DBLK_promotion p ON t.requestNumber = p.requestNumber collate thai_ci_as

    WHERE p.status = 'N' AND p.requestNumber = ?

    """




    cursor.execute(query, request_number)

    row = cursor.fetchone()




    if row:

        d1 = {

            "memberCode": row[0],

            "requestNumber": row[1],

            "billingImage": row[2],

            "brandName": row[3],

            "branchID": row[4],

            "branchName": row[5],

            "description": row[6],

            "channel": row[7],

            "staffID": row[8],

            "staffName": row[9],

            "terminalID": row[10],

            "businessDate": str(row[11]),

            "purchaseDate": str(row[12]),

            "orderNumber": row[13],

            "channelLocation": row[14],

            "items": [],

            "totalAmountBeforeDiscount": float(row[24]),

            "discountamount": float(row[25]),

            "totalAmountafterDiscount": float(row[26]),

            "orderTender": [],

            "paymentChannel": row[27],

            "paymentRef": row[28],

            "paymentType": row[29],

            "promotion": [],

            "ref1": row[30],

            "ref2": row[31],

            "requestIPEndUser": row[32],

            "requestNumberRef": row[33],

            "requestType": row[34],

        }




        # Fetch items data

        items_query = """

        SELECT categoryName, itemCode, itemName, itemServicetype, quantity, unitPrice, discount, department, netAmount

        FROM DBLK_promotion

        WHERE status = 'N' AND requestNumber = ?

        """




        cursor.execute(items_query, request_number)

        item_rows = cursor.fetchall()




        items_list = []

        for item_row in item_rows:

            item = {

                "categoryName": item_row[0],

                "itemCode": item_row[1],

                "itemName": item_row[2],

                "itemServiceType": item_row[3],

                "quantity": float(item_row[4]),

                "unitPrice": float(item_row[5]),

                "discount": float(item_row[6]),

                "department": item_row[7],

                "netAmount": float(item_row[8])

            }

            items_list.append(item)




        d1["items"] = items_list




        # Fetch orderTender data

        order_tender_query = """

        SELECT tenderAmount, tenderCode, tenderPaymentRef1, tenderPaymentRef2

        FROM DBLK_tender

        WHERE status = 'N' AND requestNumber = ?

        """




        cursor.execute(order_tender_query, request_number)

        order_tender_rows = cursor.fetchall()




        order_tender_list = []

        for order_tender_row in order_tender_rows:

            order_tender = {

                "tenderAmount": float(order_tender_row[0]),

                "tendercode": order_tender_row[1],

                "tenderPaymentRef1": order_tender_row[2],

                "tenderPaymentRef2": order_tender_row[3]

            }

            order_tender_list.append(order_tender)




        d1["orderTender"] = order_tender_list

        # print(order_tender_list)

        cursor.close()

        conn.close()




        return json.dumps(d1, indent=4, cls=DecimalEncoder)

    else:

        cursor.close()

        conn.close()




        return None    




# Prepare request for promotion

auth_token = get_token()

# print(auth_token)




# items = get_request_numbers()

# for req in items:

#     promotion_request = get_items(req)

#     print(promotion_request)




promotion_headers = {

    "Authorization": "Basic " + auth_token,

    "Content-Type": "application/json",

    "Content-Language": "en"

}




items = get_request_numbers()

for req in items:

    promotion_request = get_items(req)

    # time.sleep(0.25)

    # print(promotion_request)

    # promotion_response = make_request(PROMOTION_URL, headers=promotion_headers, data=promotion_request)

    promotion_response = requests.request("POST", PROMOTION_URL, headers=promotion_headers, data=promotion_request)

    # Prepare parameter for insert_log function

    response_data = promotion_response.json()

    promo_req = json.loads(promotion_request)

    code = response_data['code']

    httpStatus = response_data['httpStatus']

    message = response_data['message']

    server_time = response_data['serverTime']

    request_ref = response_data['requestRef']

    insert_datetime = datetime.now()

    membercode = promo_req['memberCode']

    # if promotion_response.status_code == 200:

    if httpStatus == 200 and message == 'Success.':

        print(f"Data pushed successfully for requestNumber: {req}")        

        # Update status function

        update_status(req, 'Y')

        print(f"Successfully update status for requestNumber: {req}")

        # Insert log function

        insert_log(code, httpStatus, message, server_time, request_ref, insert_datetime, membercode, req)

    else:

        print(f"Failed to push data for requestNumber: {req}")        

        # Update status function

        update_status(req, 'E')

        print(f"Failed to update status for requestNumber: {req}")

        # Insert log function

        insert_log(code, httpStatus, message, server_time, request_ref, insert_datetime, membercode, req)



