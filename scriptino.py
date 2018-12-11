#!/usr/bin/env python
import requests
from elasticsearch import Elasticsearch
from QueryGen import q1, q2
import json


es= Elasticsearch([{"host" : "192.168.92.101", "port" : 9200}])

query_dict = {}
query_dict['query1'] = q1.body
query_dict['query2'] =q2.body
res = es.search(index="lora-device_packet-deduplication", doc_type='device_packet', body=query_dict['query2'], size=100)
json_string=json.dumps(res, indent=4, separators=(',',':'))



# Writing response on a file just for backup
file = open('response.txt', 'w')
file.write(json_string)
file.close()
with open('response.txt', 'r') as file:
    data = json.load(file)



#While working local

# file.write(json_string)
# file.close()

#THIS WORKS
print("%d responses found" % res['hits']['total'])
#creato new dict: key is GW and value is a set of IDs
response={}

def addToDict(response, GW_id, Dev_id):
    response[GW_id].append(Dev_id)



#file = open('idToGW.txt', 'w')
for doc in res['hits']['hits']:
    GW_id = str(doc['_source']['gateway'])
    Dev_id = str(doc['_id'])
    try:
        addToDict(response, GW_id,Dev_id)
    except KeyError, e:
        response[GW_id] = [doc['_id']]
        print repr(e)

print(len(response[GW_id]))
for key in response.keys():
    print("key: "+ key)

    #print("id: %s - GW: %s \n" % (doc['_id'] , doc['_source']['gateway']))
