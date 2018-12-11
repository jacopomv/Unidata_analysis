import requests
from elasticsearch import Elasticsearch
from QueryGen import q1, q2, q3
import json

es= Elasticsearch([{"host" : "192.168.92.101", "port" : 9200}])

query_dict = {}
response={}

query_dict['query1'] = q1.body
query_dict['Spec_GW'] =q2.body
query_dict['match_all'] =q3.body
indexToquery="lora-device_packet-deduplication"
doc_typeToQuery='device_packet'
size=50 #max is 10000



def queryDB():
    res = es.search(index=indexToquery, doc_type=doc_typeToQuery, body=query_dict['match_all'], size=size)
    jsonTostring=json.dumps(res, indent=4, separators=(',',':'))
    return res

def addToDict(dict, key, value):
    dict[key].append(value)

def scanDoc():
    returnedQuery=queryDB()
    for field in returnedQuery['hits']['hits']:
        GW_id = str(field['_source']['gateway'])
        Dev_id = str(field['_source']['uid'])
        Dev_eui = str(field['_source']['dev_eui'])

    #Prints to the screen the output
        #print "DEV ID: %s - DEV_EUI: %s " % (field['_id'], field['_source']['dev_eui'])

        try:
            addToDict(response, GW_id, Dev_id)
        except KeyError, e:
            response[GW_id] = [Dev_id]
            #print repr(e)

#write out on a file in order to have a easy readable result
def easyToRead(file_name):
    with open(file_name, 'w') as file:
        for k,v in response.items():
            file.write("GW ID: %s,\n DEV ID: %s \n" % (k,v))
            print "done!"
            print "Values of key: %s are: %i " % (k, len(response[k]))

def checkResult():
    return 0


def main():

    scanDoc()
    easyToRead("easyreadable.txt")
    checkResult()




if __name__== "__main__":
  main()
