import http.client
from pickle import FALSE
import pyodbc
import pandas as pd
import time
from datetime import date, datetime, timedelta
import collections
import json










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


def sql_connect1():
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


# def API_KEYs():

#     sql_query5 = pd.read_sql_query(f''' 
#                               SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'API_KEY'
#                               ''' 
#                               ,sql_connect())
#     df = pd.DataFrame(sql_query5)
#     df['values_type'] = df['values_type'].astype(str)
#     sql_connect().commit()
#     return df['values_type'].values.tolist()

def url():
    
    sql_query = pd.read_sql_query(f''' 
                              SELECT  [Types],[values_type] FROM [dbo].[Infobip_config] where Types = 'url'
                              ''' 
                              ,sql_connect1())
    df = pd.DataFrame(sql_query)
    df['values_type'] = df['values_type'].astype(str)
    sql_connect1().commit()
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

url = (", ".join(url()))

# url = "https://19wxmd.api.infobip.com/people/2/custom/merge"

API_KEY = 'App 97ff7734bca71b474425c51af38401d8-056071b4-e526-420f-bd5d-b7f9ba0525f5'

# print(definitionids)


# payload = '{      "persons": [      {          "externalId": "300000003",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf202430c8e63eebd07646871695ec723"              } ]           }      } ]  }'
payload1 ='{      "persons": [      {          "externalId": "300000003",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf202430c8e63eebd07646871695ec723"              } ]           }      } ]  }'
payload2 ='{      "persons": [      {          "externalId": "300000001",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1ac1d809deafd73b12d6bdbd0a198204"              } ]           }      } ]  }'
payload3 ='{      "persons": [      {          "externalId": "300107007",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U5e7b7494cfae28717a409ebb3b232e6e"              } ]           }      } ]  }'
payload4 ='{      "persons": [      {          "externalId": "300086631",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8972c2f7afc85cd020f64c0cdaf68a36"              } ]           }      } ]  }'
payload5 ='{      "persons": [      {          "externalId": "300317425",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub57c0ae0af39f64c253479628e172e83"              } ]           }      } ]  }'
payload6 ='{      "persons": [      {          "externalId": "111644864",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U57695ad7999a50742b3c26d101cb37b1"              } ]           }      } ]  }'
payload7 ='{      "persons": [      {          "externalId": "300002133",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8a0844ab1175b0fc7081a51366158a11"              } ]           }      } ]  }'
payload8 ='{      "persons": [      {          "externalId": "101440537",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U0318eb85a37a0b82d06e28265db69efe"              } ]           }      } ]  }'
payload9 ='{      "persons": [      {          "externalId": "101044879",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uc59397ad5ba87b9d6475e07008e2a04d"              } ]           }      } ]  }'
payload10 ='{      "persons": [      {          "externalId": "101178562",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ucf44fb7a332f4309ffdf89b426a38533"              } ]           }      } ]  }'
payload11 ='{      "persons": [      {          "externalId": "111798716",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U890459592fb663b28dfbdce1391b2513"              } ]           }      } ]  }'
payload12 ='{      "persons": [      {          "externalId": "300352275",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud552ed768c9c097b5756d69755147145"              } ]           }      } ]  }'
payload13 ='{      "persons": [      {          "externalId": "300035915",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uc473cb776d208d3eada51ff0092f486f"              } ]           }      } ]  }'
payload14 ='{      "persons": [      {          "externalId": "101143329",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U94488a532acf5b9c9f09f6520d2dd51f"              } ]           }      } ]  }'
payload15 ='{      "persons": [      {          "externalId": "101388886",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf29bf2e347e4ad0a4cce1f5943c4c4fe"              } ]           }      } ]  }'
payload16 ='{      "persons": [      {          "externalId": "300031564",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U679a0921864b64ea6787dc331c6092f4"              } ]           }      } ]  }'
payload17 ='{      "persons": [      {          "externalId": "300355495",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufd3c893cf53918950e6814cdb9400b61"              } ]           }      } ]  }'
payload18 ='{      "persons": [      {          "externalId": "300355711",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf2a7e663446fd69110a9d97e6b2fad5d"              } ]           }      } ]  }'
payload19 ='{      "persons": [      {          "externalId": "300019552",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua5a3357594375ee2924e535137d0f6db"              } ]           }      } ]  }'
payload20 ='{      "persons": [      {          "externalId": "101097280",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud45c9eaf66e37e3f806aee26fe658ad8"              } ]           }      } ]  }'
payload21 ='{      "persons": [      {          "externalId": "121964112",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U04583bd719104466ff16489c72e30bfd"              } ]           }      } ]  }'
payload22 ='{      "persons": [      {          "externalId": "300095250",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufce18d4d6466ddeb1a19046cf31dccd3"              } ]           }      } ]  }'
payload23 ='{      "persons": [      {          "externalId": "111822871",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U43bc930c7735cd70b967078d49a08cc3"              } ]           }      } ]  }'
payload24 ='{      "persons": [      {          "externalId": "300276471",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8f0382ce012032ea4de0e24f2c344004"              } ]           }      } ]  }'
payload25 ='{      "persons": [      {          "externalId": "101004918",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U26399b327271e39d00d837cc74578e1b"              } ]           }      } ]  }'
payload26 ='{      "persons": [      {          "externalId": "101326334",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U5a177cee3837dba55e5a1cba57ad6fa1"              } ]           }      } ]  }'
payload27 ='{      "persons": [      {          "externalId": "300145264",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue6bbc7f31d5afad244f75a669a4e0031"              } ]           }      } ]  }'
payload28 ='{      "persons": [      {          "externalId": "101002533",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9c809fab4503373a9eb2f9f0c7447f1b"              } ]           }      } ]  }'
payload29 ='{      "persons": [      {          "externalId": "111898115",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U338fe8df4cd355384d126f50a3eddb02"              } ]           }      } ]  }'
payload30 ='{      "persons": [      {          "externalId": "101295039",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U326c4ff3e27ff57f54689e9ff802f580"              } ]           }      } ]  }'
payload31 ='{      "persons": [      {          "externalId": "101207763",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U52f128de5e2b510bed5493a9aa66c87b"              } ]           }      } ]  }'
payload32 ='{      "persons": [      {          "externalId": "300358759",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uda01903532ba40639a5c15275dfcf30a"              } ]           }      } ]  }'
payload33 ='{      "persons": [      {          "externalId": "121839991",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U73160f21df70d8abcfbb1924b27b956c"              } ]           }      } ]  }'
payload34 ='{      "persons": [      {          "externalId": "101075027",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udb82e68c95381d1ca123603c94fb2fb9"              } ]           }      } ]  }'
payload35 ='{      "persons": [      {          "externalId": "101005553",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf21cf67ca6c358158b254797ce9d4a4c"              } ]           }      } ]  }'
payload36 ='{      "persons": [      {          "externalId": "101503351",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udd7e95eb60b97743572d847166a15fed"              } ]           }      } ]  }'
payload37 ='{      "persons": [      {          "externalId": "101009906",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9ded289afdf55e06af916283799435e8"              } ]           }      } ]  }'
payload38 ='{      "persons": [      {          "externalId": "121904960",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U93020c3cf289f4d4b5bda62ab88a32aa"              } ]           }      } ]  }'
payload39 ='{      "persons": [      {          "externalId": "300035523",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U4f20e9baf0d5a88239c7bb819d89d176"              } ]           }      } ]  }'
payload40 ='{      "persons": [      {          "externalId": "111771862",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U403be6ad4290d75d645dc6b936e2b21d"              } ]           }      } ]  }'
payload41 ='{      "persons": [      {          "externalId": "111837343",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uda7b5122fe8f3edc6d70410fe42cf676"              } ]           }      } ]  }'
payload42 ='{      "persons": [      {          "externalId": "111868597",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub1783102215afef144fa662de36d6f06"              } ]           }      } ]  }'
payload43 ='{      "persons": [      {          "externalId": "300360236",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U63b00e1ea200532cdd3ade12ae8327c5"              } ]           }      } ]  }'
payload44 ='{      "persons": [      {          "externalId": "121937758",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud6d97dba4fe5ca5864ed0d7adda5dae8"              } ]           }      } ]  }'
payload45 ='{      "persons": [      {          "externalId": "300093632",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf7167b16039e8663ff3cfa08d23ae4a7"              } ]           }      } ]  }'
payload46 ='{      "persons": [      {          "externalId": "300177263",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub1760fa47802d80ae0464693afebf947"              } ]           }      } ]  }'
payload47 ='{      "persons": [      {          "externalId": "111974804",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U4cbe9cf0d9ead8825c85f399700b7c7e"              } ]           }      } ]  }'
payload48 ='{      "persons": [      {          "externalId": "121926931",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U19a3dda72db31ff6cd396545d816c908"              } ]           }      } ]  }'
payload49 ='{      "persons": [      {          "externalId": "300360456",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U0787da235d9c5c0d9dc35b198fb1f8ad"              } ]           }      } ]  }'
payload50 ='{      "persons": [      {          "externalId": "101001512",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf6a9bd6ad56c005134daa0810d000e89"              } ]           }      } ]  }'
payload51 ='{      "persons": [      {          "externalId": "101031741",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U17dd10128236127deb4682c1d12ab185"              } ]           }      } ]  }'
payload52 ='{      "persons": [      {          "externalId": "101231760",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue2d5a373a9328606e960cc35b2154832"              } ]           }      } ]  }'
payload53 ='{      "persons": [      {          "externalId": "111852480",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua3c47f8984672d318984a649a947bfcf"              } ]           }      } ]  }'
payload54 ='{      "persons": [      {          "externalId": "101031522",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf7fe466e7604423078dbdf0dce479934"              } ]           }      } ]  }'
payload55 ='{      "persons": [      {          "externalId": "121919001",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud575d00b91827317b2cf2059cfaa4bb2"              } ]           }      } ]  }'
payload56 ='{      "persons": [      {          "externalId": "121836009",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U3d4732c76ab5d2c31e0ee57bdf6e0192"              } ]           }      } ]  }'
payload57 ='{      "persons": [      {          "externalId": "300360958",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U21a1dcf50c08cdfe75ec12efa67e89ad"              } ]           }      } ]  }'
payload58 ='{      "persons": [      {          "externalId": "300361513",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9344c9382488b43aae07c5f12ca4c76d"              } ]           }      } ]  }'
payload59 ='{      "persons": [      {          "externalId": "300080537",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U6f55a2a65029d61f91659f023143c5a2"              } ]           }      } ]  }'
payload60 ='{      "persons": [      {          "externalId": "300233004",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufaf089290e2e1e052a93e6b9aa25d84d"              } ]           }      } ]  }'
payload61 ='{      "persons": [      {          "externalId": "101361296",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U04606a24945b1b9b879cc0b38207a777"              } ]           }      } ]  }'
payload62 ='{      "persons": [      {          "externalId": "300079972",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U343775cdecbb6bce1c3047344531a550"              } ]           }      } ]  }'
payload63 ='{      "persons": [      {          "externalId": "111792871",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U4ba2b91b26090206fdf80572daaaa1a7"              } ]           }      } ]  }'
payload64 ='{      "persons": [      {          "externalId": "101048058",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf2728a1d35dbb95e35833717f6294b9c"              } ]           }      } ]  }'
payload65 ='{      "persons": [      {          "externalId": "300048306",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ubb5487c3bdbeb0eb754e283ab598b220"              } ]           }      } ]  }'
payload66 ='{      "persons": [      {          "externalId": "300367126",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufeac57ff5c722601f2845b6a6869ad81"              } ]           }      } ]  }'
payload67 ='{      "persons": [      {          "externalId": "300367400",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ueb473750d652496f06bd414e77cac016"              } ]           }      } ]  }'
payload68 ='{      "persons": [      {          "externalId": "111868256",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U3337d157be7c5e74fffd3f1eb4306327"              } ]           }      } ]  }'
payload69 ='{      "persons": [      {          "externalId": "300367936",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uaa053e20ba62cfaca6358aaa8e4ec7d6"              } ]           }      } ]  }'
payload70 ='{      "persons": [      {          "externalId": "121826154",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U7712495f6a83f90f08da6dd1d197b5d2"              } ]           }      } ]  }'
payload71 ='{      "persons": [      {          "externalId": "300368860",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U0295b74eafc99f8e544411ba5dca299f"              } ]           }      } ]  }'
payload72 ='{      "persons": [      {          "externalId": "101462145",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udcb776498a62ec1c46df34fe78a48d9b"              } ]           }      } ]  }'
payload73 ='{      "persons": [      {          "externalId": "300373633",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U5bcf9f602a91ad8a84d3223395aca36e"              } ]           }      } ]  }'
payload74 ='{      "persons": [      {          "externalId": "300373922",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uc077dc774dca51cd53d16bef52eed956"              } ]           }      } ]  }'
payload75 ='{      "persons": [      {          "externalId": "121966004",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U54e8f7233f38a9277c6e14c27d38ed6a"              } ]           }      } ]  }'
payload76 ='{      "persons": [      {          "externalId": "101121319",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U00a47456d975add4b2236894031a4e97"              } ]           }      } ]  }'
payload77 ='{      "persons": [      {          "externalId": "300369085",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U52c69384f8b9552bf7ded1636329eeb5"              } ]           }      } ]  }'
payload78 ='{      "persons": [      {          "externalId": "300376166",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ued69913f81f101dbcb29e92798a3207f"              } ]           }      } ]  }'
payload79 ='{      "persons": [      {          "externalId": "300327518",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8f75b4753db4a3779418f6333713fc6e"              } ]           }      } ]  }'
payload80 ='{      "persons": [      {          "externalId": "300194870",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufb8e82dfa0a985a35122400ffb6090e7"              } ]           }      } ]  }'
payload81 ='{      "persons": [      {          "externalId": "111987051",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U2c905f5dc28063514e9bbcd537cc4f44"              } ]           }      } ]  }'
payload82 ='{      "persons": [      {          "externalId": "101087478",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U74d96892ae0d5401116f18ffd40325cb"              } ]           }      } ]  }'
payload83 ='{      "persons": [      {          "externalId": "300010693",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U672381ed9937fd205ed9f977d8c1b722"              } ]           }      } ]  }'
payload84 ='{      "persons": [      {          "externalId": "300377357",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uedbe795ec6dd412573600ce050e97ff0"              } ]           }      } ]  }'
payload85 ='{      "persons": [      {          "externalId": "300127154",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ucfaafec64df6b20ef17257cfcc168de4"              } ]           }      } ]  }'
payload86 ='{      "persons": [      {          "externalId": "121937163",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua7a17b31fd7314c8f31f5a57e7032cbd"              } ]           }      } ]  }'
payload87 ='{      "persons": [      {          "externalId": "300262665",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U513b5a1d772114c7bf2da17e331c8084"              } ]           }      } ]  }'
payload88 ='{      "persons": [      {          "externalId": "121926218",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U94f26c2ab897fad392866891a75696b1"              } ]           }      } ]  }'
payload89 ='{      "persons": [      {          "externalId": "300176563",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uef06fe15cd1608128eca4d69229081e9"              } ]           }      } ]  }'
payload90 ='{      "persons": [      {          "externalId": "101584890",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue36c313c8e3cf0ac2e4a1aa94bc62813"              } ]           }      } ]  }'
payload91 ='{      "persons": [      {          "externalId": "101014514",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufce732df844f617df312065a984898a4"              } ]           }      } ]  }'
payload92 ='{      "persons": [      {          "externalId": "300022649",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U34464c31fd4c9fc8ffb026fad0e8df05"              } ]           }      } ]  }'
payload93 ='{      "persons": [      {          "externalId": "101123147",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub8c7357bd2354c2ab4e3a20f8c45a4b3"              } ]           }      } ]  }'
payload94 ='{      "persons": [      {          "externalId": "300062331",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U89e642125215c73bfaa9abc2db1ca0d5"              } ]           }      } ]  }'
payload95 ='{      "persons": [      {          "externalId": "121956107",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U6e9087a6c94255a00e1310f5628c0286"              } ]           }      } ]  }'
payload96 ='{      "persons": [      {          "externalId": "101383135",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1116ef7fa9e819b54b88b46dce4deb78"              } ]           }      } ]  }'
payload97 ='{      "persons": [      {          "externalId": "300180245",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1d69255223c378dbc3a38c58cc43928a"              } ]           }      } ]  }'
payload98 ='{      "persons": [      {          "externalId": "121830074",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U649ace40bdaf25f4b0d851fcbc0cf0cc"              } ]           }      } ]  }'
payload99 ='{      "persons": [      {          "externalId": "300378069",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uabc31db9469b38ed4c2b71acb6013ac8"              } ]           }      } ]  }'
payload100 ='{      "persons": [      {          "externalId": "300105965",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U3f1040215adfe274205bf85f1813715e"              } ]           }      } ]  }'
payload101 ='{      "persons": [      {          "externalId": "300382232",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud88ad8ada37af0ac295852e38176cb91"              } ]           }      } ]  }'
payload102 ='{      "persons": [      {          "externalId": "300383267",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1ab6f6cdf94cba84165f6133c2c4c9a2"              } ]           }      } ]  }'
payload103 ='{      "persons": [      {          "externalId": "300383877",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud6dd8437852f600ea43081c27101bdd3"              } ]           }      } ]  }'
payload104 ='{      "persons": [      {          "externalId": "121918724",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub7ff9088f88b19bc38345f50de1d0b1d"              } ]           }      } ]  }'
payload105 ='{      "persons": [      {          "externalId": "300097466",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U5e2e01ce0901599991ac35dd0da698bf"              } ]           }      } ]  }'
payload106 ='{      "persons": [      {          "externalId": "121940557",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub360251eaf5022b5146e5d761ca31237"              } ]           }      } ]  }'
payload107 ='{      "persons": [      {          "externalId": "121920949",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udfb02c3160a6533c59a1fa4b58b02e16"              } ]           }      } ]  }'
payload108 ='{      "persons": [      {          "externalId": "300076592",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uffa2d2178ea240eecbca4749cec33d55"              } ]           }      } ]  }'
payload109 ='{      "persons": [      {          "externalId": "121903952",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udc13f9986ae4df3dc85929bbf6711815"              } ]           }      } ]  }'
payload110 ='{      "persons": [      {          "externalId": "101425711",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U308b79f7a968eee9f26c468b02c8d183"              } ]           }      } ]  }'
payload111 ='{      "persons": [      {          "externalId": "300386479",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua61bac203186fb00e552acba692c1eff"              } ]           }      } ]  }'
payload112 ='{      "persons": [      {          "externalId": "300382910",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U6ab409f07d0d438b096bcb28fc923f57"              } ]           }      } ]  }'
payload113 ='{      "persons": [      {          "externalId": "101141264",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U7f8b892929177ba9e9bfd652289d4a44"              } ]           }      } ]  }'
payload114 ='{      "persons": [      {          "externalId": "300389000",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uaf8c8758a11ed2e0e453824ff2eb45df"              } ]           }      } ]  }'
payload115 ='{      "persons": [      {          "externalId": "300390318",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud5adf19644107ae871f3d81675628649"              } ]           }      } ]  }'
payload116 ='{      "persons": [      {          "externalId": "300390581",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufb9f071b5930fc6222815efd9620002f"              } ]           }      } ]  }'
payload117 ='{      "persons": [      {          "externalId": "300390584",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua7499ac89e743fe4e5d23456b79f39f1"              } ]           }      } ]  }'
payload118 ='{      "persons": [      {          "externalId": "101574165",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Udb627d57ecf86c0e8e735a027ff27839"              } ]           }      } ]  }'
payload119 ='{      "persons": [      {          "externalId": "300124411",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua5601c95063a7d7ba75fa2cda4dd43c7"              } ]           }      } ]  }'
payload120 ='{      "persons": [      {          "externalId": "101105657",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U6cf70b17d7007df335a1d05dced9ad16"              } ]           }      } ]  }'
payload121 ='{      "persons": [      {          "externalId": "121936894",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ubca24e6d5fb210ca142962f464e2ac23"              } ]           }      } ]  }'
payload122 ='{      "persons": [      {          "externalId": "300359023",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9b8bace27bd13d1f3e4f3f245557eca6"              } ]           }      } ]  }'
payload123 ='{      "persons": [      {          "externalId": "300396494",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub8aef570bcd832a4d9019f028d1e365c"              } ]           }      } ]  }'
payload124 ='{      "persons": [      {          "externalId": "300203585",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U0b29ad7742d227568fae70491f1aba92"              } ]           }      } ]  }'
payload125 ='{      "persons": [      {          "externalId": "111829017",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub5ccba2f1d5f1928383edd9ea2743d8e"              } ]           }      } ]  }'
payload126 ='{      "persons": [      {          "externalId": "111784626",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U91ccb7b85d9ca82a130db0e9a84d53d7"              } ]           }      } ]  }'
payload127 ='{      "persons": [      {          "externalId": "111829951",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua10057d0bc60b445badff747b40f2ae1"              } ]           }      } ]  }'
payload128 ='{      "persons": [      {          "externalId": "101271529",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9e62f7e89874f0d56a1979cb880b3847"              } ]           }      } ]  }'
payload129 ='{      "persons": [      {          "externalId": "300175471",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U150e7220b75feec9a7f4d6f05969f89c"              } ]           }      } ]  }'
payload130 ='{      "persons": [      {          "externalId": "300235611",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U34c925725041c4c5c58abce48e1f3047"              } ]           }      } ]  }'
payload131 ='{      "persons": [      {          "externalId": "101420227",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua87f43904b74fadefccda9d05a18fceb"              } ]           }      } ]  }'
payload132 ='{      "persons": [      {          "externalId": "101399861",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9388d41aa3a93fbd9328780bb55728b1"              } ]           }      } ]  }'
payload133 ='{      "persons": [      {          "externalId": "121911434",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ubc0796a4bebf8374343aeee4ced3bc15"              } ]           }      } ]  }'
payload134 ='{      "persons": [      {          "externalId": "300405595",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U2d4f0cc9d8f8df2c5ad607962a07ef7c"              } ]           }      } ]  }'
payload135 ='{      "persons": [      {          "externalId": "111984933",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ucb3794962180377bb5b6d437ddf1cafa"              } ]           }      } ]  }'
payload136 ='{      "persons": [      {          "externalId": "101153623",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U3a23d5fecebe36685f4b7dc09a4da779"              } ]           }      } ]  }'
payload137 ='{      "persons": [      {          "externalId": "300193967",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1158e85f0e7f3857011a760141e178bb"              } ]           }      } ]  }'
payload138 ='{      "persons": [      {          "externalId": "300314828",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U276db66c77391eac02824a72544fa471"              } ]           }      } ]  }'
payload139 ='{      "persons": [      {          "externalId": "300088614",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua27c2a79bde189b6d7ff1b8d35966dca"              } ]           }      } ]  }'
payload140 ='{      "persons": [      {          "externalId": "300313672",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U7721eb06057faaf661addf524e92a25a"              } ]           }      } ]  }'
payload141 ='{      "persons": [      {          "externalId": "300411957",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ud011d4785db63e6b317cea9811cc75ea"              } ]           }      } ]  }'
payload142 ='{      "persons": [      {          "externalId": "300413149",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub44dfc11c15d939996cfd65a32b2667f"              } ]           }      } ]  }'
payload143 ='{      "persons": [      {          "externalId": "300389758",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8367ec09baa10a09d9d532ac29ba5aa9"              } ]           }      } ]  }'
payload144 ='{      "persons": [      {          "externalId": "101210519",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U972ff5f9f7e1590fed22dffaec0320dc"              } ]           }      } ]  }'
payload145 ='{      "persons": [      {          "externalId": "300205554",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1110ace9412d2f479c11e0279b9f83bc"              } ]           }      } ]  }'
payload146 ='{      "persons": [      {          "externalId": "300041250",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uccb7622b15e1baf30b4812623f8e25db"              } ]           }      } ]  }'
payload147 ='{      "persons": [      {          "externalId": "300000818",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua804224908b4ab50a30c59bbecdb8b76"              } ]           }      } ]  }'
payload148 ='{      "persons": [      {          "externalId": "101360214",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U70849f6f2211bf67c5f3bc75e91ebe1f"              } ]           }      } ]  }'
payload149 ='{      "persons": [      {          "externalId": "300419749",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U23e4863833e65dfde323cb092182713c"              } ]           }      } ]  }'
payload150 ='{      "persons": [      {          "externalId": "101058094",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U619ae86c375db04e5022303b61a34d1f"              } ]           }      } ]  }'
payload151 ='{      "persons": [      {          "externalId": "300420277",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf96619603cf8cdb4cdcc203e98608d2a"              } ]           }      } ]  }'
payload152 ='{      "persons": [      {          "externalId": "300005221",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua713c70ceb27aa00226bf7d401eb87ce"              } ]           }      } ]  }'
payload153 ='{      "persons": [      {          "externalId": "300421367",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U4ff7cd32ac9546dc9dfe66e28faeaecf"              } ]           }      } ]  }'
payload154 ='{      "persons": [      {          "externalId": "101373196",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uba396301b9beebeb1e2001aa72147452"              } ]           }      } ]  }'
payload155 ='{      "persons": [      {          "externalId": "101553734",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ufdbd5b39e60a36c102191091617a59fe"              } ]           }      } ]  }'
payload156 ='{      "persons": [      {          "externalId": "300391221",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uf7af41d7c220bdebdf09849c1610cd69"              } ]           }      } ]  }'
payload157 ='{      "persons": [      {          "externalId": "101383419",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U018eab3ea9f9008f037dbb6421b56eb5"              } ]           }      } ]  }'
payload158 ='{      "persons": [      {          "externalId": "300206837",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U115f3c518bf3e88ae53c7bb39e41f46e"              } ]           }      } ]  }'
payload159 ='{      "persons": [      {          "externalId": "111786291",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue4641e2d2a18cdcf1773e927a4ced5cc"              } ]           }      } ]  }'
payload160 ='{      "persons": [      {          "externalId": "101563847",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uadad911be5e10b1dcc8630af55a9cd36"              } ]           }      } ]  }'
payload161 ='{      "persons": [      {          "externalId": "121956093",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U3e29ee15e2aaf27bc695fc91335643e8"              } ]           }      } ]  }'
payload162 ='{      "persons": [      {          "externalId": "300357038",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1b6e3c460a297cb67066f64cd72cf25c"              } ]           }      } ]  }'
payload163 ='{      "persons": [      {          "externalId": "300425130",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U441cf21488824fff5411c5646ca8ff5a"              } ]           }      } ]  }'
payload164 ='{      "persons": [      {          "externalId": "101603489",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua6149e3c35c09c9e5fb2f967f440461b"              } ]           }      } ]  }'
payload165 ='{      "persons": [      {          "externalId": "300426766",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U180baea3bcb27072047e870eb6cb0c50"              } ]           }      } ]  }'
payload166 ='{      "persons": [      {          "externalId": "300427077",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua51da27b9dfae63088739d2523078618"              } ]           }      } ]  }'
payload167 ='{      "persons": [      {          "externalId": "300182420",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ucc5880f80073993f9a288e41589aa451"              } ]           }      } ]  }'
payload168 ='{      "persons": [      {          "externalId": "300428079",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub120a439740c4763155fd60f6f131c5f"              } ]           }      } ]  }'
payload169 ='{      "persons": [      {          "externalId": "111851102",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U5b349bf8a720eda1bd7543c8a31f62d6"              } ]           }      } ]  }'
payload170 ='{      "persons": [      {          "externalId": "300428942",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U586beefff1e3bc72019f169bcbbcbbf4"              } ]           }      } ]  }'
payload171 ='{      "persons": [      {          "externalId": "300308065",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ub2c6b3ed3105967468249ce2b96180c4"              } ]           }      } ]  }'
payload172 ='{      "persons": [      {          "externalId": "300074076",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue4a5f63443448eb360c685a981b0a99c"              } ]           }      } ]  }'
payload173 ='{      "persons": [      {          "externalId": "300013323",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue001bd662d65335d79741bdf72e6e5d0"              } ]           }      } ]  }'
payload174 ='{      "persons": [      {          "externalId": "300431349",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U2259bf2950877d2b6309e11e7e051a97"              } ]           }      } ]  }'
payload175 ='{      "persons": [      {          "externalId": "300431776",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U4c78a5e4b1d886cbbacc4fc9c5d81cce"              } ]           }      } ]  }'
payload176 ='{      "persons": [      {          "externalId": "121706337",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1690ed32ae5a95cb16a741e0d14dd62e"              } ]           }      } ]  }'
payload177 ='{      "persons": [      {          "externalId": "300432073",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U0ebb054dca0e21330eb18a6244a95159"              } ]           }      } ]  }'
payload178 ='{      "persons": [      {          "externalId": "111897480",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ubbc074f2631f82a8b760b062cf117f3c"              } ]           }      } ]  }'
payload179 ='{      "persons": [      {          "externalId": "111615484",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua5a1e070b1c58f325d126a0ee4cb110d"              } ]           }      } ]  }'
payload180 ='{      "persons": [      {          "externalId": "121933979",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Uc58761c97cf2359e3d6d253f164e76d8"              } ]           }      } ]  }'
payload181 ='{      "persons": [      {          "externalId": "300433276",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U32e095140199d388c90f9a31bb8d5fc0"              } ]           }      } ]  }'
payload182 ='{      "persons": [      {          "externalId": "300425126",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1eea56f861da62ba9ff09e58da9ab7d0"              } ]           }      } ]  }'
payload183 ='{      "persons": [      {          "externalId": "121825620",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue7686daf0bdffb1e795adf3337fbba52"              } ]           }      } ]  }'
payload184 ='{      "persons": [      {          "externalId": "300435148",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U8fccdbda407c3a34d9d17c3f71b715be"              } ]           }      } ]  }'
payload185 ='{      "persons": [      {          "externalId": "300196369",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ueda0ab031d6423a0de6779ab4460c48c"              } ]           }      } ]  }'
payload186 ='{      "persons": [      {          "externalId": "101603703",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue282334dc425c52c0c68d6ba6e1c9381"              } ]           }      } ]  }'
payload187 ='{      "persons": [      {          "externalId": "300391963",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ua86464cc84c1007fb9310993c012dd28"              } ]           }      } ]  }'
payload188 ='{      "persons": [      {          "externalId": "101075021",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ue86d4e51443515d1b9e70370254d66b2"              } ]           }      } ]  }'
payload189 ='{      "persons": [      {          "externalId": "300109834",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U1fefe119b867becee7ba9ed0132a8f59"              } ]           }      } ]  }'
payload190 ='{      "persons": [      {          "externalId": "300325243",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "Ubd01f13a9b2bea470756b51a8f3af087"              } ]           }      } ]  }'
payload191 ='{      "persons": [      {          "externalId": "300438658",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U9b6dacc0ef21e1e541025b339e056387"              } ]           }      } ]  }'
payload192 ='{      "persons": [      {          "externalId": "101579390",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U7d4c8e6edda8d883136abe1d7a4c18e4"              } ]           }      } ]  }'
payload193 ='{      "persons": [      {          "externalId": "300440658",          "contacts": {              "line": [ {                  "applicationId": "1656930502",                  "userId": "U842815e50949e73adbf8b4ddf6eeaf01"              } ]           }      } ]  }'

def api_POST():
    
    conn = http.client.HTTPSConnection(url)

    urls = "https://19wxmd.api.infobip.com/people/2/custom/merge"
    headers = {
        'Authorization': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request("POST", urls, payload79, headers)
    res = conn.getresponse()
    
    res.reason
    data = res.read()
    conn.close()
    conn_string = sql_connect()
    conn1 = conn_string
    cursor = conn1.cursor()
    cursor.execute('''
                INSERT INTO MinorPOS.dbo.Infobip_event_log (Types,External_ID,Saleline_id,Customer_id,datas,status,reason,decode,insert_dates)
                VALUES (?,?,?,?,?,?,?,?,?)
                ''',
                'line',
                '',
                '',
                '79',
                str(payload79),
                str(res.status),
                str(res.reason),
                str(data.decode("utf-8")),
                # str(res.headers),
                str(date.today()) +' '+ str(get_local_time())\
                )
    conn1.commit()
    conn1.close  
api_POST()




