from __future__ import print_function
import time
import datetime
from time import sleep
from random import random
from threading import Timer
import pickle
import sys
from kafka import KafkaProducer
import uuid  #generate 128bit numbers to uniquely identify objects
import numpy as np
import datetime
import math
import prestodb  #for database
import calendar,yaml
from datetime import timedelta




i_temp = ""
#tango_list = []
#time_list = []
#coordinates_list = []
final_list = []
#store_id = None
emp_id = None
inserted_at_li = []
tango_id_li = []
store_id_li = []
person_cood_li = []

with open('loitering.yaml', 'r') as ymlFile:
    cfg = yaml.load(ymlFile)

hive_host = cfg['loitering']['hive_host']
kafka_host = cfg['loitering']['kafka_host']
kafka_producer_topic = cfg['loitering']['kafka_producer_topic']
timediff = cfg['loitering']['timediff']

timestamp = datetime.datetime.utcnow()
one_hour = datetime.datetime.utcnow() - timedelta(hours=timediff)
end_date_time = int(calendar.timegm(timestamp.utctimetuple()))
start_date_time = int(calendar.timegm(one_hour.utctimetuple()))

conn = prestodb.dbapi.connect(
    host=hive_host,
    port=8889,
    user='hive',
    catalog='hive',
    schema='retail_dev',
)

sql = "select inserted_at ,tango_id , store_id, person_cood from person_meta where inserted_at<=" + str(
    start_date_time) + " and " \
                       "inserted_at>=" + str(end_date_time) + ""

cur = conn.cursor()
cur.execute(sql)
newlist = cur.fetchall()

emp_centre = {}
time_loit_db = {}
emp=[]
b=[]
time_list=[]
i_temp=""


def dist(x1, y1, x2, y2):
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

for i,sub_list in enumerate(newlist):
    inserted_at_li.append(sub_list[0])
    tango_id_li.append(sub_list[1])
    store_id_li.append(sub_list[2])
    person_cood_li.append(sub_list[3])
    
for i in range(len(person_cood_li)):
    if i_temp==str(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S')):
        time_list.append(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S'))
    else:
        time_list.append(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S'))
    i_temp=datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S')
        
    
for i in range(len(person_cood_li)):
    emp_centre[i] =((person_cood[1][i][0]+person_cood[1][i][2])/2,person_cood[1][i][1],abs((person_cood[1][i][2]-person_cood[1][i][0])))
if len(emp_centre)>1:
    distance=sys.maxsize
    for (k, v) in emp_centre.items():
        emp_id1 = k
        x1, y1, width = v[0], v[1], v[2]
        for i, (key, coord) in enumerate(emp_centre.items()):
            wait_time=random.randint(1,9)
            if key == k:
                continue
            temp = dist(x1, y1, coord[0], coord[1])
            normalisation = (width + coord[2]) / 2
            ndist = temp / normalisation
            #print("distance-----",ndist)
            if temp < distance:
                #distance = temp
                emp_id2 = key
                x=list([emp_id1,emp_id2])
                emp.append(x)
                for i in emp:
                    #print(i)
                    y=list([i[1],i[0]])
                    if(y not in b and i not in b):
                        b.append(i)
                        #print("KKKKKKKKKKKK",b)
            loit=emp_id1+emp_id2
            
            if emp_id1!=emp_id2 and x in b:
                if ndist < 1:
                    print(emp_id1,"and",emp_id2,"could be loittering")
                    print("\n")
                    #start = time.time()
                    #time.sleep(wait_time)
                    #done = time.time()
                    #elapsed = done - start
                    #print("time-----",elapsed)
                    if loit not in time_loit_db.keys():
                        time_loit_db[loit] = datetime.datetime.strptime(sub_list[2][0], '%Y-%m-%d %H:%M:%S')

                    if loit in time_loit_db.keys():
                        current_time = datetime.datetime.strptime(sub_list[2][0], '%Y-%m-%d %H:%M:%S')
                        loitering_time =((current_time-datetime.datetime.strptime(time_loit_db[loit],'%Y-%m-%d %H:%M:%S')).seconds)/60

                    if loitering_time > 2:
                        print(emp_id1,"and",emp_id2,"loittering!")
                        print("\n")
                        print("time spent loittering -----",loitering_time,"seconds")
                        print("\n")
                    else:
                        print(emp_id1,"and",emp_id2,"not loittering anymore")
                        print("\n")
                else:
                    print(emp_id1,"and",emp_id2,"not loittering")
                    print("\n")


                



    