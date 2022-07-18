import json
import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime
from collections import defaultdict


class Insert:
    def __init__(self):
        self.now = datetime.now()
        self.engine = create_engine(
            'mysql+pymysql://bike:dbgusals1@etl.cgskizjipfsf.ap-northeast-2.rds.amazonaws.com:3306/use_bike')
        self.h = self.now.hour + 1
        self.today = date.today()
        self.yesterday = self.today - timedelta(1)

    def bike_insert(self):
        try:
            for i in range(1, 2002, 1000):
                dic = defaultdict(list)

                if i == 2001:
                    url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{2001}/{2872}/{self.yesterday.strftime("%Y%m%d")}{self.h}'

                else:
                    url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{i}/{i + 999}/{self.yesterday.strftime("%Y%m%d")}{self.h}'

                response = requests.get(url)
                json_ob = json.loads(response.content)
                json_ar = json_ob.get('getStationListHist')
                json_ar1 = json_ar.get('row')

                for j in json_ar1:
                    stationName = j.get('stationName')
                    if stationName[0].isdigit():
                        stationName = stationName[5:].lstrip().rstrip()
                    dic['Date'].append(f'{self.yesterday.strftime("%Y-%m-%d")} {format(self.h, "02")}')
                    dic['stationId'].append((j.get('stationId'))[3:])
                    dic['stationName'].append(stationName)
                    dic['parkingBike'].append(j.get('parkingBikeTotCnt'))
                    dic['shared'].append(float(j.get('shared')) * 0.01)
                    dic['rackTotCnt'].append(j.get('rackTotCnt'))
                    dic['location'].append(f"{j.get('stationLatitude')},{j.get('stationLongitude')}")

                df = pd.DataFrame(dic)
                df.to_sql(name='bike', con=self.engine, if_exists='append', index=False)
        except BaseException:
            print('오류')

    def weather_insert(self):
        try:
            url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst'
            dic = defaultdict(list)
            params = {
                    'serviceKey': 'rqhjv1LU12E5PvOh9i4ytUPxBvLJgSCJ14gUQ2t7SngOmCUd7UDHOnrSJFu9BKbepq4n52jX4gCsFm1mIIMMow==',
                    'pageNo': '1', 'numOfRows': '1000', 'dataType': 'JSON',
                    'base_date': f'{self.yesterday.strftime("%Y%m%d")}',
                    'base_time': f'{format(self.h, "02")}00', 'nx': '60', 'ny': '127'}

            response = requests.get(url, params=params)
            json_ob = json.loads(response.content)
            json_ar = json_ob.get('response').get('body').get('items').get('item')
            dic['DATE'].append(f'{self.yesterday.strftime("%Y-%m-%d")} {self.h}')
            for i in json_ar:
                dic[i['category']].append(i['obsrValue'])
            df = pd.DataFrame(dic)
            df.to_sql(name='weather', con=self.engine, if_exists='append', index=False)
        except:
            print('에러')

    def main(self):
        self.bike_insert()
        self.weather_insert()


if __name__ == '__main__':
    Insert().main()
