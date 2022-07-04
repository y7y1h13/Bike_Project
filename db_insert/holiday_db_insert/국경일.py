import requests
import json
import requests
import pandas as pd
import pymysql
from datetime import datetime
from sqlalchemy import create_engine
from collections import defaultdict


engine = create_engine('mysql+pymysql://bike:dbgusals1@localhost:3306/use_bike')

now = datetime.now()
month = now.month if now.month > 9 else f'0{now.month}'
url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo'
params = {'serviceKey': 'rqhjv1LU12E5PvOh9i4ytUPxBvLJgSCJ14gUQ2t7SngOmCUd7UDHOnrSJFu9BKbepq4n52jX4gCsFm1mIIMMow==',
          '_type': 'json', 'pageNo': '1', 'numOfRows': '10', 'solYear': f'{now.year}', 'solMonth': f'{month}'}

response = requests.get(url, params=params)
json_ob = json.loads(response.content)
json_ar = json_ob.get('response').get('body').get('items').get('item')

dic = defaultdict(list)
for i in json_ar:
    dic['Date'].append(i['locdate'])
    dic['dateName'].append(i['dateName'])
df = pd.DataFrame(dic)
df.to_sql(name='holiday', con=engine, if_exists='append', index=False)
