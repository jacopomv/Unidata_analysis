import requests
from elasticsearch import Elasticsearch
from datetime import datetime
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
datePKT=None

query_dict['query_GWToSF'] = q1.body
query_dict['Spec_GW'] = q2.body
query_dict['match_all'] = q3.body
indexToquery="lora-device_packet-deduplication"
doc_typeToQuery='device_packet'

query_type=""

#size=150000 #max is 500000
# --- settings changed in the index 'lora-device_packet-deduplication'
#with PUT lora-device_packet-deduplication/_settings
# {
#   "max_result_window" : 500000
# }



def queryDB():
    res = es.search(index=indexToquery, doc_type=doc_typeToQuery, body=query_dict[query_type], size=size)
    jsonTostring=json.dumps(res, indent=4, separators=(',',':'))
    return res

def addToDict(dict, key, value):
    dict[key].append(value)

def scanDoc():
    global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT, response, datePKT
    returnedQuery=queryDB()

    alba=datetime.strptime("00:00", '%H:%M')
    mattina=datetime.strptime("08:00", '%H:%M')
    sera=datetime.strptime("16:00", '%H:%M')
    notte=datetime.strptime("23:59", '%H:%M')

    fascia_mattina="MATTINA"
    fascia_pome="POME"
    fascia_notte="NOTTE"

    for field in returnedQuery['hits']['hits']:
        GW_id = str(field['_source']['gateway'])
        Dev_id = str(field['_source']['uid'])
        Dev_eui = str(field['_source']['dev_eui'])
        freq = str(field['_source']['freq'])
        sizePkt=str(field['_source']['size'])
        datr = str(field['_source']['datr'])
        typePKT = str(field['_source']['type'])
        datePKT = datetime.strptime(field['_source']['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')
        date_format=datePKT.strftime("%b %d, %Y - %H:%M:%S")


        # print alba.hour
        # print mattina.hour
        # print datePKT.hour
        #print notte.hour
        #print notte.hour <datePKT.hour<mattina.hour

        if alba.hour < datePKT.hour< mattina.hour :
            try:
                addToDict(response, fascia_mattina, Dev_id)
            except KeyError, e:
                response[fascia_mattina] = [Dev_id]
        elif mattina.hour <datePKT.hour<sera.hour:
            try:
                addToDict(response, fascia_pome, Dev_id)
            except KeyError, e:
                response[fascia_pome] = [Dev_id]
        elif sera.hour <datePKT.hour<notte.hour:
            try:
                addToDict(response, fascia_notte, Dev_id)
            except KeyError, e:
                response[fascia_notte] = [Dev_id]





    # #Prints to the screen the output
    #     #print "DEV ID: %s - DEV_EUI: %s " % (field['_id'], field['_source']['dev_eui'])
    #     try:
    #         addToDict(response, date_format, Dev_id)
    #     except KeyError, e:
    #         response[date_format] = [Dev_id]
    #         #print repr(e)

#write out on a file in order to have a easy readable result
def easyToRead(file_name):
    with open(file_name, "a+") as file:
        file.write(" \n GW: %s \n Total size query: %i \n" % (GW_id,size))
        for k,v in sorted(response.items()):
            #file.write("Values for the key: %s are: %i , percentage of packets is: \t%s \n" % (k, len(response[k]), "{:.2%}".format(float(len(response[k]))/size)))
            #file.write("DATR: %s,\n DEV ID: %s \n" % (k,v))
            #print "done!"
            print "Values for the key: %s are: %i , percentage is: %s " % (k, len(response[k]), "{:.2%}".format(float(len(response[k]))/size))
    file.close()

def checkResult():
    #response_d = sorted((value, key) for (key,value) in d.items())
    sorted(response.keys())
    #for key, value in sorted(response.iteritems(), key=lambda (k,v): (v,k)):
    for key in response:
        print "%s: %s" % (key, len(response[key]))
    # print datePKT.hour
    # print type(datePKT)
    # for k in sort_response:
    #     #print "done!"
    #     #print "Values for the key: %s are: %i" % (k, len(response[k]))
    #     print("{} : {}".format(k, len(response[k])))


def main():
    global size, query_type
    query_type = raw_input("Insert query type: ")
    size = input("Insert size of the query: ")
    scanDoc()
    #easyToRead("Packet2dim.txt")
    checkResult()



if __name__== "__main__":
  main()
