import json
import requests

url = 'http://openapi.seoul.go.kr:8088/707950714679377934386e64746352/json/bikeList/2001/2603/20220530'

response = requests.get(url)
json_ob = json.loads(response.content)
print(json_ob)