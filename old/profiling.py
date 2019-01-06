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
month=None
tot=0

query_dict['GWToSF'] = q1.body
query_dict['SpecGW'] = q2.body
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
    global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT, response, datePKT, month
    returnedQuery=queryDB()

    alba=datetime.strptime("00:00", '%H:%M')
    mattina=datetime.strptime("08:00", '%H:%M')
    sera=datetime.strptime("16:00", '%H:%M')
    notte=datetime.strptime("23:59", '%H:%M')
    day=datetime.strptime("11-12", '%m-%d')
    month=datetime.strptime("08", '%m')

    fascia_mattina="Morning (00-08)"
    fascia_pome="Afternoon (08-16)"
    fascia_notte="Night (16-23)"
    ex_day_labour="One single labour day"
    ex_day_holy="One single holiday day"
    ex_month="One single month"


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

        datePKT_hour_min=datePKT.strptime(str(datePKT.hour)+":"+str(datePKT.minute),'%H:%M')
        datePKT_day=str(datePKT.day)
        datePKT_month=datePKT.strptime(str(datePKT.month), "%m")
        datePKT_week=datePKT.strptime(str(datePKT.month), "%m")



        # print alba.hour
        # print mattina.hour
        # print datePKT.hour
        #print notte.hour
        #print notte.hour <datePKT.hour<mattina.hour
        #print day4
        #print datePKT
        if month.month == datePKT.month:
            try:
                addToDict(response, GW_id, Dev_eui)
            except KeyError, e:
                response[GW_id] = [Dev_eui]

        # DIVION BY PART OF THE DAY
        # if alba <= datePKT_hour_min<= mattina :
        #     try:
        #         addToDict(response, fascia_mattina, Dev_eui)
        #     except KeyError, e:
        #         response[fascia_mattina] = [Dev_eui]
        # elif mattina <=datePKT_hour_min<=sera:
        #     try:
        #         addToDict(response, fascia_pome, Dev_eui)
        #     except KeyError, e:
        #         response[fascia_pome] = [Dev_eui]
        # elif sera <=datePKT_hour_min<=notte:
        #     try:
        #         addToDict(response, fascia_notte, Dev_eui)
        #     except KeyError, e:
        #         response[fascia_notte] = [Dev_eui]





    # #Prints to the screen the output
    #     #print "DEV ID: %s - DEV_EUI: %s " % (field['_id'], field['_source']['dev_eui'])
    #     try:
    #         addToDict(response, date_format, Dev_id)
    #     except KeyError, e:
    #         response[date_format] = [Dev_id]
    #         #print repr(e)

#write out on a file in order to have a easy readable result
def easyToRead(file_name):
    global tot
    with open(file_name, "a+") as file:
        file.write(" \n GW: %s \n Total size query: %i \n Month: %s\n SF: %s\n" % (GW_id,size, month.strftime("%B"), datr))
        for k, value in sorted(response.items(), key=lambda x:int(x[0])):
            tot=tot+len(response[k])
            file.write("Pkts for the day: %s are: %i \t\n" % (k, len(response[k])))#, "{:.2%}".format(float(len(response[k]))/size)))
            #file.write("DATR: %s,\n DEV ID: %s \n" % (k,v))
            #print "done!"

            print "Values for the key: %s are: %i " % (k, len(response[k]))#, "{:.2%}".format(float(len(response[k]))/size))

        #file.write("Total packet analysed: %i \t\n" %(tot))
        if len(response.keys()) is not 0:
            file.write("Day average: %i \n" % (int(tot)/len(response.keys())))
        else: file.write("Empty dictionary")
    file.close()

def checkResult():
    tot=0
    #response_d = sorted((value, key) for (key,value) in response.items())
    #sorted(response.keys())
    print " \n GW: %s \n Total size query: %i \n Month: %s\n SF: %s" % (GW_id,size, month.strftime("%B"), datr)
    for key, value in sorted(response.items(), key=lambda x:x[0]):
        tot=tot+len(response[key])
    #for key in response:
        #print "sto per printare"
        print "%s: %s" % (key, len(response[key]))
    if len(response.keys()) is not 0:
        print "Media al giorno: %i " % (int(tot)/len(response.keys()))
    else: print "Empty dictionary"
    #, "{:.2%}".format(float(len(response[k]))/size))

    # print datePKT.hour
    # print type(datePKT)
    # for k in sort_response:
    #     #print "done!"
    #     #print "Values for the key: %s are: %i" % (k, len(response[k]))
    #     print("{} : {}".format(k, len(response[k])))


def main():
    global size, query_type
    try:
        query_type = raw_input("Insert query type: ")
    except KeyError, e:
        print "Invalid query type"

    size = input("Insert size of the query: ")
    scanDoc()
    #easyToRead("GWSF2Day.txt")
    checkResult()



if __name__== "__main__":
  main()
