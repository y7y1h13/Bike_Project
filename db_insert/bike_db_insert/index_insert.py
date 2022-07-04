import json
import requests
import pymysql
from sqlalchemy import text, create_engine

engine = create_engine('mysql+pymysql://bike:dbgusals1@etl.cgskizjipfsf.ap-northeast-2.rds.amazonaws.com:3306/use_bike')

try:
    for i in range(1, 2002, 1000):
    
        if i == 2001:
            url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{2001}/{2872}/20220620'
    
        else:
            url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeListHist/{i}/{i + 999}/20220620'
    
        response = requests.get(url)
        json_ob = json.loads(response.content)
        json_ar = json_ob.get('getStationListHist')
        json_ar1 = json_ar.get('row')
    
        sql = """
                insert into id_index(stationId, stationName)
                values(
                :stationId,
                :stationName
                )
                """
    
        for i in json_ar1:
            id = i.get('stationId')
            name = i.get('stationName')
            if name[0].isdigit():
                name = name[5:].lstrip().rstrip()
            dt = {"stationId": id,
                  "stationName": name}
            engine.execute(text(sql), **dt)
except:
    print('오류')