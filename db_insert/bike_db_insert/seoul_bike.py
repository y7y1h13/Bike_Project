import json
import requests
import pymysql
from sqlalchemy import text, create_engine
from datetime import date, timedelta

engine = create_engine('mysql+pymysql://bike:dbgusals1@etl.cgskizjipfsf.ap-northeast-2.rds.amazonaws.com:3306/use_bike')

for h in range(24):
    for i in range(1, 2002, 1000):
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

        sql = """
        insert into bike(Date, stationId, stationName, rackTotCnt, parkingBike, shared, stationLatitude, stationLongitude)
        values(
        :Date,
        :stationId,
        :stationName,
        :rackTotCnt,
        :parkingBike,
        :shared,
        :stationLatitude,
        :stationLongitude
        )
        """

        for i in json_ar1:
            stationName = i.get('stationName')
            stationId = i.get('stationId')
            parkingBike = i.get('parkingBikeTotCnt')
            shared = i.get('shared')
            rackTotCnt = i.get('rackTotCnt')
            stationLatitude = i.get('stationLatitude')
            stationLongitude = i.get('stationLongitude')

            if stationName[0].isdigit():
                stationName = stationName[5:].lstrip().rstrip()

            dt = {"Date": f'{yesterday.strftime("%Y-%m-%d")}" "{h}', "stationId": stationId,
                  "stationName": stationName,
                  "parkingBike": parkingBike, 'shared': shared, 'rackTotCnt': rackTotCnt,
                  'stationLatitude': stationLatitude, 'stationLongitude': stationLongitude}
            engine.execute(text(sql), **dt)
