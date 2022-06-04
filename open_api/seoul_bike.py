import json
import requests
import pymysql
from sqlalchemy import text, create_engine
from datetime import datetime

engine = create_engine('mysql+pymysql://bike:dbgusals1@localhost:3306/use_bike')

for i in range(1, 2002, 1000):

    url = f'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeList/{i}/{i + 999}/{datetime.today().strftime("%Y%m%d")}'

    response = requests.get(url)
    json_ob = json.loads(response.content)
    json_ar = json_ob.get('rentBikeStatus')
    json_ar1 = json_ar.get('row')

    sql = """
    insert into bike(stationName, parking_Bike, shared)
    values(
    :station,
    :parking,
    :shared)
    """

    for i in json_ar1:
        station = i.get('stationName')
        parking = i.get('parkingBikeTotCnt')
        share = i.get('shared')
        dt = {"station": station[5:].lstrip().rstrip(), "parking": parking, 'shared': share}
        engine.execute(text(sql), **dt)