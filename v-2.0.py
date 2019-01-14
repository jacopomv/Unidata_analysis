import datetime
import pickle
import logging
import time

from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from elasticsearch import Elasticsearch
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

import json
import sys


MAX_SIZE=10

def getDevicePacketPreDeduplication(url,eui_list=None,gw_list=["58a0cbeffe014e4c"]):
    t = time.time()
    index="lora-device_packet-pre_deduplication"
    s = Search(using=Elasticsearch([url]),index=index)
    #s = s.filter('range', created_at={'gte': range_gte, 'lte': range_lte})
    query = {"match_all": {}}

    if eui_list==None and gw_list==None:
       query = {"match_all": {}}
    else:
        if eui_list!=None:
            query = Q("match", dev_eui=eui_list[0])
            for ii in eui_list:
                query = query | Q("match", dev_eui=ii)
        if gw_list != None:
            query = Q("match", gateway=gw_list[0])
            for ii in gw_list:
                query = query | Q("match", gateway=ii)

    s = s.query(query)
    response=s.execute()

    print('Total %d hits found.' % response.hits.total)
    print('Total query loops: {}.'.format(round(response.hits.total/MAX_SIZE)))

    lora_pkt_dedup=[]
#    for d in s.params(size=MAX_SIZE).source(['gateway']).scan():

    #for d in s.params(size=MAX_SIZE).scan():
    #filtering for dev_addr?
    for d in s.params(size=MAX_SIZE).source(['dev_addr']).scan():
        lora_pkt_dedup.append(d)

    df = pd.DataFrame((d.to_dict() for d in lora_pkt_dedup))
    elapsed = time.time() - t
    print("Elapsed Time: {}".format(elapsed))
    return df


def main():
    url = "http://192.168.92.101:9200"
    #range_gte = "2018-10-27T00:00.00Z"
    #range_lte = "2018-10-27T00:59.99Z"
    #range_lte = "2018-10-27T05:59.99Z"
    #range_lte = "2018-10-27T00:59.99Z"



    get_from_db=True
    if get_from_db:
        print("Get Device Packet Pre-Deduplicated")
        df = getDevicePacketPreDeduplication(
            url=url, eui_list=None,gw_list=None)
        #df = getDevicePacketPreDeduplication(
        #    url=url, range_gte=range_gte, range_lte=range_lte)
    else:
        d_pickle = pickle.load( open("data/df_rssi_pre_dedup-2018-10-27T00_00.00Z-2018-11-03T23_59.00Z.pkl", "rb" ) )
        df=d_pickle[0]

    #type:
    df_filt_gw=df
    #dev_addr_list = np.array(np.unique(df_filt_gw.dev_addr))
    print("dev_addr_list:{}".format(len(df_filt_gw)))
    print(type(df_filt_gw))
    #df.to_csv('out.csv')




if __name__== "__main__":
  main()
