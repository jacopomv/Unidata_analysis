import requests
from elasticsearch import Elasticsearch
from QueryGen import q1, q2, q3
import json
import sys

es= Elasticsearch([{"host" : "192.168.92.101", "port" : 9200}])

query_dict = {}
dict_response={}

defineQ=0
returnedQuery=None

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

#size=150000 #max is 500000
# --- settings changed in the index 'lora-device_packet-deduplication'
#with PUT lora-device_packet-deduplication/_settings
# {
#   "max_result_window" : 500000
# }



def queryDB():
    res = es.search(index=indexToquery, doc_type=doc_typeToQuery, body=query_dict['match_all'], size=size)
    jsonTostring=json.dumps(res, indent=4, separators=(',',':'))
    return res

def addToDict(dict, key, value):
    dict[key].append(value)


def assignFields(chiave, valore):
    global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT
    for field in returnedQuery['hits']['hits']:
        GW_id = str(field['_source']['gateway'])
        Dev_id = str(field['_source']['uid'])
        Dev_eui = str(field['_source']['dev_eui'])
        freq = str(field['_source']['freq'])
        sizePkt=str(field['_source']['size'])
        datr = str(field['_source']['datr'])
        typePKT = str(field['_source']['type'])

        try:
            addToDict(dict_response, chiave, valore)
        except KeyError, e:
            dict_response[chiave] = [valore]




# def scanDoc(chiave, valori):
#     global returnedQuery
#     global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT, dict_response
#     returnedQuery=queryDB()
#     for field in returnedQuery['hits']['hits']:
#         GW_id = str(field['_source']['gateway'])
#         Dev_id = str(field['_source']['uid'])
#         Dev_eui = str(field['_source']['dev_eui'])
#         freq = str(field['_source']['freq'])
#         sizePkt=str(field['_source']['size'])
#         datr = str(field['_source']['datr'])
#         typePKT = str(field['_source']['type'])
#         try:
#             addToDict(dict_response, chiave, valori)
#         except KeyError, e:
#             dict_response[chiave] = [valori]
#     print "GWWW: "+GW_id
#     print "DATR: "+datr
#     print "typePKT: "+typePKT


#write out on a file in order to have a easy readable result
def easyToRead(file_name):
    with open(file_name, "a+") as file:
        file.write(" \n GW: %s \n Total size query: %i \n" % (GW_id,size))
        for k,v in sorted(dict_response.items()):
            #file.write("Values for the key: %s are: %i , percentage of packets is: \t%s \n" % (k, len(dict_response[k]), "{:.2%}".format(float(len(dict_response[k]))/size)))
            #file.write("DATR: %s,\n DEV ID: %s \n" % (k,v))
            #print "done!"
            print "Values for the key: %s are: %i , percentage is: %s " % (k, len(dict_response[k]), "{:.2%}".format(float(len(dict_response[k]))/size))
    file.close()
def checkResult():
    #response_d = sorted((value, key) for (key,value) in d.items())
    for key, value in sorted(dict_response.iteritems(), key=lambda (k,v): (v,k)):
        print "%s: %s" % (key, len(dict_response[key]))

# def checkResult():
#     for k, v in dict_response.items():
#         print "chiave: %s, valore: %i" % (k, len(dict_response[k]))
#     #dict_response_d = sorted((value, key) for (key,value) in d.items())
#     for key, value in sorted(dict_response.iteritems(), key=lambda (k,v): (v,k)):
#         print"sto qua"
#         print "key: %s: value lenght %i" % (key, len(dict_response[key]))
#     # for k in sort_dict_response:
#     #     #print "done!"
#     #     #print "Values for the key: %s are: %i" % (k, len(dict_response[k]))
#     #     print("{} : {}".format(k, len(dict_response[k])))


def main():
    global size, defineQ, returnedQuery
    size = int(sys.argv[2])
    returnedQuery = queryDB()

    if len(sys.argv) <= 3:
        if int(sys.argv[1])==1:
            assignFields(sizePkt, Dev_id)
        elif int(sys.argv[1])==2:
            assignFields(GW_id,Dev_id)
        elif int(sys.argv[1])==3:
            assignFields(datr, Dev_id)

        print "Size of the query:", size
        print "Type of query selected: ", int(sys.argv[1])
    else:
        print "usage: ./profiling.py <type of query> <size of query> "
        exit(-1)

    checkResult()




if __name__== "__main__":
  main()
