import time
import datetime
import sys
from kafka import KafkaProducer
import numpy as np
import datetime
import math
import prestodb
import calendar
from datetime import timedelta


i_temp = ""
tango_list = []
time_list = []
coordinates_list = []
final_list = []
store_id = None
emp_id = None
inserted_at_li = []
tango_id_li = []
store_id_li = []
person_cood_li = []

#with open('loitering.yaml', 'r') as ymlFile:
 #   cfg = yaml.load(ymlFile)

hive_host = "ec2-35-172-192-179.compute-1.amazonaws.com"
kafka_host = "ec2-18-235-19-181.compute-1.amazonaws.com:9092"
timediff = 150
timestamp = datetime.datetime.utcnow()
one_hour = datetime.datetime.utcnow() - timedelta(hours=timediff)
start_date_time = int(calendar.timegm(timestamp.utctimetuple()))
end_date_time = int(calendar.timegm(one_hour.utctimetuple()))
conn = prestodb.dbapi.connect(
    host=hive_host,
    port=8889,
    user='hive',
    catalog='hive',
    schema='retail_prod',
)
sql = "select inserted_at ,tango_id , store_id, person_cood from edge_data where inserted_at<=" + str(
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
loit_emp=[]
print(len(newlist))

def dist(x1, y1, x2, y2):
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

for i,sub_list in enumerate(newlist):
    inserted_at_li.append(sub_list[0])
    tango_id_li.append(sub_list[1])
    store_id_li.append(sub_list[2])
    person_cood_li.append(sub_list[3])

for i in range(len(newlist)):
    if i_temp == str(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S')):
        tango_list.append(tango_id_li[i])
        person_co = list(person_cood_li[i].split(','))
        coordinates_list.append(np.array(person_co, dtype=np.int))
        time_list.append(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S'))
        store_id = store_id_li[i]
    else:
        temp_list = []
        temp_list.append(tango_list)
        temp_list.append(coordinates_list)
        temp_list.append(time_list)
        final_list.append(temp_list)
       	tango_list = []
        time_list = []
       	coordinates_list = []
       	tango_list.append(tango_id_li[i])
       	person_co = list(person_cood_li[i].split(','))
       	coordinates_list.append(np.array(person_co, dtype=np.int))
       	time_list.append(datetime.datetime.utcfromtimestamp(int(inserted_at_li[i])).strftime('%Y-%m-%d %H:%M:%S'))
       	store_id = store_id_li[i]
time=[]
loitering_time=0
current_time=0
loit=0
#print(final_list)
for i, (sub_list) in enumerate(final_list):
    #count = 0
    #print("iteration",count,"---------------------------------------------")
    for i, (sub_sub_list) in enumerate(sub_list[0]):
        if sub_sub_list[0]=='E':
            emp_centre[sub_sub_list] = (int((sub_list[1][i][0] + sub_list[1][i][2]) / 2), int(sub_list[1][i][1]),
                                        int((sub_list[1][i][2] - sub_list[1][i][0])))
        #print(emp_centre)

#emp_centre[sub_sub_list] =((sub_list[1][i][0]+[1][i][2])/2,sub_list[1][i][1],abs((sub_list[1][i][2]-sub_list[1][i][0])))
    if len(emp_centre) > 0:
        distance=sys.maxsize
        #print(distance)
        for (k, v) in emp_centre.items():
            emp_id1 = k
            x1, y1, width = v[0], v[1], v[2]
            for i, (key, coord) in enumerate(emp_centre.items()):
                if key == k:
                    continue
                loit_p=loit
                loitering_time_p=loitering_time
                current_time_p=current_time
                temp = dist(x1, y1, coord[0], coord[1])
                       #print(temp,"***********")
                normalisation = (width + coord[2]) / 2
                ndist = temp / normalisation
                #print("distance-----",ndist)
                if temp < distance:
                    distance = temp
                    emp_id2 = key
                    x=list([emp_id1,emp_id2])
                    emp.append(x)
                    for i in emp:
                        y=list([i[1],i[0]])
                        if(y not in b and i not in b):
                            b.append(i)
                    #print("KKKKKKKKKKKK",b)
                loit=emp_id1+emp_id2
                
                #print("===========",loit)

                if emp_id1!=emp_id2 and x in b:
                           #print("********",loit)
                           #print("---------------------------------------------")
                    if ndist < 2:
                        #print(emp_id1,"and",emp_id2,"could be loittering")
                               #flag=0
                        if loit not in time_loit_db.keys():
                            time_loit_db[loit] = datetime.datetime.strptime(sub_list[2][0], '%Y-%m-%d %H:%M:%S')

                        if loit in time_loit_db.keys():
                            current_time = datetime.datetime.strptime(sub_list[2][0], '%Y-%m-%d %H:%M:%S')
                            loitering_time =((current_time-time_loit_db[loit]).seconds)/60
                            #print("'''''''''''''''",loitering_time)

                            if loitering_time > 0.5 and loit!=loit_p and loitering_time!=loitering_time_p and current_time!=current_time_p :
                                tempp=list([emp_id1,emp_id2])
                                loit_emp.append(tempp)
                                time.append(loitering_time)
                                #print(emp_id1,"and",emp_id2,"loittered for",loitering_time,"from",current_time,"to",time_loit_db[loit])
                                print(emp_id1,"and",emp_id2,"loitered for",loitering_time,"from",time_loit_db[loit],"to",current_time)
                                loit_emp.sort()
                                finalOutput = []
                                finalOutput.append( loit_emp[0] )
                                for i in loit_emp[1:]:
                                    flag = 0
                                    for j in finalOutput:
                                        if i[0] in j:
                                            flag = 0
                                            if i[1] not in j:
                                                j.append( i[1])
                                                break
                                        else:
                                            flag = 1
                                    if( flag == 1):
                                        finalOutput.append( i )

                                for i in range(len(finalOutput)):
                                    if len(finalOutput[i]) > 2:
                                        print( finalOutput[i],"are loitering as a group")
                                    
