import requests
import base64
import pandas

TOKEN = "your_mixpanel_token" # Mixpanel token
CSV_PATH = "distinctId-to-delete.csv" # CSV file path

# convert to mixpanel request data format and base64 encode
def encodeMixJson(distinctIds): 
	js = map(lambda id: """{"$token": "%s", "$distinct_id": "%s", "$delete": ""}""" % (TOKEN, id), distinctIds)
	return base64.b64encode(str(js))

# read csv file and call mixpanel engage api
readCSV = pandas.read_csv(CSV_PATH) 
distinctIds = list(readCSV.distinct_id.values)
sliceDistinctIds = [distinctIds[x : x + 50] for x in range(0, len(distinctIds), 50)] # max 50 profile per api request
data = map(lambda ids: encodeMixJson(ids), sliceDistinctIds)
responses = map(lambda d: requests.post("http://api.mixpanel.com/engage/?data=%s" % d), data)

for res in responses: print res.json # result log 