from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import pandas as pd


dict={}
def main():
    s = Search(using=Elasticsearch("http://192.168.92.101:9200"), index="lora-device_packet-pre_deduplication") \
    \
    .params(request_timeout=30) \
    .params(size=10000)
    # .update_from_dict({
    #    "query" : {
    #       "constant_score" : {
    #          "filter" : {
    #             "bool" : {
    #               "must" : [
    #                  { "term" : {"gateway" : "1c497beffecab36d"}},
    #                  { "term" : {"datr" : "sf12bw125"}}
    #               ],
    #            }
    #          }
    #       }
    #    }
    # })

    query= Q("match", gateway="58A0CBEFFE014E4C") & Q("match", datr="sf7bw125")
    #query ={"match_all": {}}
    s=s.query(query)

    print("Executing the query")
    response = s.execute()
    print("Receiving response...")

    # for hit in response:
    #     print(hit.meta.score, hit.dev_addr)
    # query = Q("match", dev_addr="00B1009C", size="33", rssi="-97")

    print('Total %d hits found.' % response.hits.total)
    print("Query executed...")
    lora_pkt_dedup = []
    T2=()
    T1=()
    print('Total %d hits found.' % response.hits.total)
    for h in response:
        try:
            T1 =(h.dev_addr,)
            T2=(h.lsnr,)+ (h.size,)+ (h.datr,)+ (h.rssi,)+ (h.gateway,)+ (h.freq,)+ (h.type,)
            dict.setdefault(h.dev_addr, []).append(T2)
        except AttributeError:
            T1 = ("None",)
            T2 = (h.lsnr,) + (h.size,)+ (h.datr,)+ ("None",)+ (h.gateway,)+ (h.freq,)+ (h.type,)
            dict.setdefault("None", []).append(T2)
    #T3=(T1,T2)
    df= pd.DataFrame.from_dict(dict, orient='index')
    df.to_csv('myfile.csv')
    #print(len(dict.keys()))
    #print(len(dict.items()))
    #printOutResult(dictionary=dict)

    #print ("this is a tuple: %s" % (T,))
    # for hit in response:
    #     print(hit.dev_addr)

def printOutResult(dictionary):
    tot=0
    #Print to console the key-value pair ordered by
    #print"GW: %s and SF: %s\n" % (GW_id, datr)
    #print "GW: %s has Null dev_eui values are : %i, datr is: %s " % (GW_id,count, datr)
    #for key, value in sorted(dictionary.items(), key=lambda x:len(x[1]), reverse=True):
    for key, value in sorted(dictionary.items(), key=lambda x:x[0], reverse=False):

        #tot=tot+len(dictionary[key])
        print "%s : %s" % (key, dictionary[key])
    # if len(dictionary.keys()) is not 0:
    #     print "Media al giorno: %i " % (int(tot)/len(dictionary.keys()))
    # else: print "Empty dictionary"
main()
