import json
import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime
from collections import defaultdict


engine = create_engine('mysql+pymysql://bike:dbgusals1@etl.cgskizjipfsf.ap-northeast-2.rds.amazonaws.com:3306/use_bike')
url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst'
now = datetime.now()
try:
    for h in range(now.hour + 1, 24):
        dic = defaultdict(list)
        today = date.today()
        yesterday = date.today() - timedelta(1)
        if h < 10:
            params = {'serviceKey': 'rqhjv1LU12E5PvOh9i4ytUPxBvLJgSCJ14gUQ2t7SngOmCUd7UDHOnrSJFu9BKbepq4n52jX4gCsFm1mIIMMow==',
                      'pageNo': '1', 'numOfRows': '1000', 'dataType': 'JSON', 'base_date': f'{yesterday.strftime("%Y%m%d")}',
                      'base_time': f'0{h}00', 'nx': '60', 'ny': '127'}
        else:
            params = {
                'serviceKey': 'rqhjv1LU12E5PvOh9i4ytUPxBvLJgSCJ14gUQ2t7SngOmCUd7UDHOnrSJFu9BKbepq4n52jX4gCsFm1mIIMMow==',
                'pageNo': '1', 'numOfRows': '1000', 'dataType': 'JSON', 'base_date': f'{yesterday.strftime("%Y%m%d")}',
                'base_time': f'{h}00', 'nx': '60', 'ny': '127'}

        response = requests.get(url, params=params)
        json_ob = json.loads(response.content)
        json_ar = json_ob.get('response').get('body').get('items').get('item')
        dic['DATE'].append(f'{yesterday.strftime("%Y-%m-%d")} {h}')
        for i in json_ar:
            dic[i['category']].append(i['obsrValue'])
        df = pd.DataFrame(dic)
        df.to_sql(name='weather', con=engine, if_exists='append', index=False)
except:
    print('에러')