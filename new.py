import requests
from elasticsearch import Elasticsearch
from datetime import datetime
from QueryGen import q1, q2, q3
import json
import sys

#creating empty dictionaries
query_dict = {}
dict_response={}
month_dict={}
week_dict={}
day_dict={}

#Query global variables
es= Elasticsearch([{"host" : "192.168.92.101", "port" : 9200}])
query_type=""
query_size=0
query_dict['GWToSF'] = q1.body
query_dict['SpecGW'] = q2.body
query_dict['match_all'] = q3.body
indexToquery="lora-device_packet-deduplication"
doc_typeToQuery='device_packet'
#Remember about query size: max is 500000
# --- settings changed in the index 'lora-device_packet-deduplication'
#with PUT lora-device_packet-deduplication/_settings
# {
#   "max_result_window" : 500000
# }


#Query fields global variables
GW_id=""
Dev_id=""
Dev_eui=""
freq=""
sizePkt=""
datr=""
typePKT=""
tot=0
#query date fields
month=None
week=None
hour=None
day2year=None
datePKT=None



#func performing the query to the ES database
def queryDB():
    global indexToquery
    global doc_typeToQuery
    global query_type
    global query_size

    #Make the request
    res = es.search(index=indexToquery, doc_type=doc_typeToQuery, body=query_dict[query_type], size=query_size)
    #Retrieving the result with a json
    jsonTostring=json.dumps(res, indent=4, separators=(',',':'))
    return res

#Silly func adding a value to a key inside a dict
def addToDict(dict, key, value):
    dict[key].append(value)

# This func retrieve the fields from the json returned from the query in order to fulfill the profiling requests
def scanDoc():
    global GW_id, Dev_id, Dev_eui, freq, sizePkt, datr, typePKT, dict_response, datePKT, month, week, hour, day2year
    #Call the query
    returnedQuery=queryDB()

    #scanning the json returned
    for field in returnedQuery['hits']['hits']:
        GW_id = str(field['_source']['gateway'])
        Dev_id = str(field['_source']['uid'])
        Dev_eui = str(field['_source']['dev_eui'])
        freq = str(field['_source']['freq'])
        sizePkt=str(field['_source']['size'])
        datr = str(field['_source']['datr'])
        typePKT = str(field['_source']['type'])

        #type: datetime
        datePKT = datetime.strptime(field['_source']['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')
        #type: String
        date_format=datePKT.strftime("%b %d, %Y - %H:%M:%S")

        #creating variables to store data relatively on month, week and day of the pkt
        month=datePKT.strftime("%m") #month as number
        week=datePKT.strftime("%W") #week number as Monday
        hour=datePKT.strftime("%X") #14:21:00
        day2year=datePKT.strftime("%j") #day of the year

        try:
            #adding to dict in order to store data relatively on month, week and day of the pkt
            addToDict(month_dict, month, Dev_id)
            addToDict(week_dict, week, Dev_id)
            addToDict(day_dict, day2year, Dev_id)

        except KeyError, e:
            month_dict[month] = [Dev_id]
            week_dict[week] = [Dev_id]
            day_dict[day2year] = [Dev_id]

#Func writing result to file "file_name"
def writeToFile(file_name):
    global tot
    #Opening the file in appending mode in order to write all the query results on it
    with open(file_name, "a+") as file:
        #file.write(" \n GW: %s \n Total size query: %i \n Month: %s\n SF: %s\n" % ())

        #Ordering the output by kes
        for key, value in sorted(dict_response.items(), key=lambda x:int(x[0])):
            tot=tot+len(dict_response[k])
            file.write("Pkts for the day: %s are: %i \t\n" % (k, len(dict_response[k])))
            #Format to use a precentage value
            # "{:.2%}".format(float(len(dict_response[k]))/query_size)))
            print "Values for the key: %s are: %i " % (k, len(dict_response[k]))#, "{:.2%}".format(float(len(response[k]))/query_size))

        #Write total packet analysed
        #file.write("Total packet analysed: %i \t\n" %(tot))

        #Write on the file the average of pkts
        if len(dict_response.keys()) is not 0:
            file.write("Day average: %i \n" % (int(tot)/len(dict_response.keys())))
        else: file.write("Empty dictionary")
    file.close()

#Func showing to console the results before being written to file, takes the dictionary to print out as input
def printOutResult(dictionary):
    tot=0
    #Print to console the key-value pair ordered by key
    for key, value in sorted(dictionary.items(), key=lambda x:x[0]):
        tot=tot+len(dictionary[key])
        print "%s: %s" % (key, len(dictionary[key]))
    # if len(dictionary.keys()) is not 0:
    #     print "Media al giorno: %i " % (int(tot)/len(dictionary.keys()))
    # else: print "Empty dictionary"


def main():
    global query_size, query_type

    try:
        query_size = int(sys.argv[1])
        query_type = str(sys.argv[2])
    except IndexError, er:
        print repr("Usage: arg1=query_size and arg2=query_type" )
        exit(0)


    scanDoc()
    #writeToFile("GWSF2Day.txt")
    printOutResult(month_dict)

if __name__== "__main__":
  main()
