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
dist_dict={}
#with open('loitering.yaml', 'r') as ymlFile:
 #   cfg = yaml.load(ymlFile)

hive_host = "ec2-35-172-192-179.compute-1.amazonaws.com"
kafka_host = "ec2-18-235-19-181.compute-1.amazonaws.com:9092"
timediff = 300
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
t_list=[]


def dist(x1, y1, x2, y2):
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def diff(t1,t2):
    if t1>t2:
        return t1-t2
    else:
        return t2-t1

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
time_loit_db_prev=datetime.datetime(1,1,1)
li=[]

for x in tango_id_li:
    if x not in li:
        li.append(x)
print(li)

p=datetime.datetime(1,1,1)
q=datetime.datetime(1,1,1)




county=0
for i, (sub_list) in enumerate(final_list):
   #print(county)
#   print("-------------------------loop1-----------------------------------------------------------------")
   #print(sub_list[0],"--------0")
   #print(sub_list[1],"---------------1")
   #print(sub_list[2],"-----------2")
   
   for i, (sub_sub_list) in enumerate(sub_list[0]):
       #print(1)
 #      print("############################################loop2#######################################")
       time=datetime.datetime.strptime(sub_list[2][0], '%Y-%m-%d %H:%M:%S')

       if sub_sub_list[0]=='E' and sub_sub_list not in emp_centre.keys():
           emp_centre[sub_sub_list] =[[(int((sub_list[1][i][0] + sub_list[1][i][2]) / 2), int(sub_list[1][i][1]),
                                        int((sub_list[1][i][2] - sub_list[1][i][0]))),time]]

       elif sub_sub_list[0]=='E' and sub_sub_list in emp_centre.keys():
           dup=[(int((sub_list[1][i][0] + sub_list[1][i][2]) / 2), int(sub_list[1][i][1]),
                                        int((sub_list[1][i][2] - sub_list[1][i][0]))),time]

           if dup not in emp_centre[sub_sub_list]:
               emp_centre[sub_sub_list].append(dup)
  # if len(emp_centre) > 0:
#       distance=sys.maxsize
       for (k, v) in emp_centre.items():
               #print(2)
               emp_id1 = k

               for i in v:
                   x1, y1, width1=i[0][0],i[0][1],i[0][2]
                   #print(i[0][0])
                   hourly=i[1].hour

                   for j, (key, coord) in enumerate(emp_centre.items()):

                       if emp_id1==key or ('key+emp_id',hourly) in dist_dict.keys():
                           continue

                       for k in coord:
                            #print(county)
                           if k[1].hour==hourly:
                               county=county+1

                       print(county,"---------emp_coordinate to cust coordinates")
'''
                               x2,y2,width2=k[0][0],k[0][1],k[0][2]
                               temp = dist(x1, y1, x2, y2)
                               normalisation = (width1 + width2) / 2
                               ndist = temp / normalisation
                               loit=emp_id1+key
                               #loit_r=key+emp_id1
                               if ndist < 1:
#                                   print(emp_id1,"and",key,"were at a close proximity of",ndist,"with both at time",i[1],"and",k[1],"respectively")
                                   if (loit,hourly) not in dist_dict.keys() and (emp_id1,i[1],key,k[1]) not in dist_dict.values() and (emp_id1,k[1],key,i[1]) not in dist_dict.values()  :
                                       dist_dict[loit,hourly]=[emp_id1,i[1],key,k[1]]
                                   elif (loit,hourly) in dist_dict.keys() and (emp_id1,i[1],key,k[1]) not in dist_dict.values()  :
                                       if (emp_id1,k[1],key,i[1]) not in dist_dict.values():
                                           dist_dict[loit,hourly].append([emp_id1,i[1],key,k[1]])
                               print(dist_dict)

                               for i in dist_dict.values(): 
                                   if i[1] < p and i[1] > q or i[1] > p and i[1] < q and p!=datetime.datetime.strptime(1,1,1):
                                       loitering_time=abs(loitering_time-diff(i[1],i[3]))
                                   else:
                                       loitering_time=diff(i[1],i[3])
                                   p=i[1]
                                   q=i[3]
                               print(emp_id,"and",key,"loitered for",loitering_time)
'''
