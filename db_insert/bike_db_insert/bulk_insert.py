import time
import json
import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import date, timedelta
from collections import defaultdict

start = time.time()

engine = create_engine('mysql+pymysql://bike:dbgusals1@etl.cgskizjipfsf.ap-northeast-2.rds.amazonaws.com:3306/use_bike')


try:
    for h in range(24):
        for i in range(1, 2002, 1000):
            dic = defaultdict(list)
            today = date.today()
            yesterday = date.today() - timedelta(1)

            if i == 2001:
                url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{2001}/{2872}/{yesterday.strftime("%Y%m%d")}{h}'

            else:
                url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{i}/{i + 999}/{yesterday.strftime("%Y%m%d")}{h}'

            response = requests.get(url)
            json_ob = json.loads(response.content)
            json_ar = json_ob.get('getStationListHist')
            json_ar1 = json_ar.get('row')

            for j in json_ar1:
                stationName = j.get('stationName')
                if stationName[0].isdigit():
                    stationName = stationName[5:].lstrip().rstrip()
                dic['Date'].append(f'{yesterday.strftime("%Y-%m-%d")}" "{h}')
                # dic['Date'].append(j.get('stationDt'))
                dic['stationId'].append((j.get('stationId'))[3:])
                dic['stationName'].append(stationName)
                dic['parkingBike'].append(j.get('parkingBikeTotCnt'))
                dic['shared'].append(j.get('shared'))
                dic['rackTotCnt'].append(j.get('rackTotCnt'))
                dic['location'].append(f"{j.get('stationLatitude')},{j.get('stationLongitude')}")
                # dic['stationLatitude'].append(j.get('stationLatitude'))
                # dic['stationLongitude'].append(j.get('stationLongitude'))
            df = pd.DataFrame(dic)
            df.to_sql(name='bike', con=engine, if_exists='append', index=False)
    end = time.time()
    print(f"{end - start:.5f} sec")
except:
    print('오류')