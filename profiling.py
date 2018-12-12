import requests
from elasticsearch import Elasticsearch
from QueryGen import q1, q2, q3
import json

es= Elasticsearch([{"host" : "192.168.92.101", "port" : 9200}])

query_dict = {}
response={}

GW_id=""
Dev_id=""
Dev_eui=""
freq=""
sizePkt=""
datr=""
typePKT=""

query_dict['query_GWToSF'] = q1.body
query_dict['Spec_GW'] = q2.body
query_dict['match_all'] = q3.body
indexToquery="lora-device_packet-deduplication"
doc_typeToQuery='device_packet'
size=150000 #max is 500000
# --- settings changed in the index 'lora-device_packet-deduplication'
#with PUT lora-device_packet-deduplication/_settings
# {
#   "max_result_window" : 500000
# }



def queryDB():
    res = es.search(index=indexToquery, doc_type=doc_typeToQuery, body=query_dict['Spec_GW'], size=size)
    jsonTostring=json.dumps(res, indent=4, separators=(',',':'))
    return res

def addToDict(dict, key, value):
    dict[key].append(value)

def scanDoc():
    returnedQuery=queryDB()
    for field in returnedQuery['hits']['hits']:
        global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT
        GW_id = str(field['_source']['gateway'])
        Dev_id = str(field['_source']['uid'])
        Dev_eui = str(field['_source']['dev_eui'])
        freq = str(field['_source']['freq'])
        sizePkt=str(field['_source']['size'])
        datr = str(field['_source']['datr'])
        typePKT = str(field['_source']['type'])

    #Prints to the screen the output
        #print "DEV ID: %s - DEV_EUI: %s " % (field['_id'], field['_source']['dev_eui'])
        try:
            addToDict(response, datr, Dev_id)
        except KeyError, e:
            response[datr] = [Dev_id]
            #print repr(e)

#write out on a file in order to have a easy readable result
def easyToRead(file_name):
    with open(file_name, "a+") as file:
        file.write(" \n GW: %s \n Total size query: %i \n" % (GW_id,size))
        for k,v in response.items():
            file.write("Values for the key: %s are: %i , percentage of packets is: %s \n" % (k, len(response[k]), "{:.2%}".format(float(len(response[k]))/size)))
            #file.write("DATR: %s,\n DEV ID: %s \n" % (k,v))
            #print "done!"
            print "Values for the key: %s are: %i , percentage is: %s " % (k, len(response[k]), "{:.2%}".format(float(len(response[k]))/size))
    file.close()

def checkResult():
    return 0


def main():

    scanDoc()
    easyToRead("easyreadable.txt")
    checkResult()



if __name__== "__main__":
  main()
