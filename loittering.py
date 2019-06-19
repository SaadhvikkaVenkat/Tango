from __future__ import print_function
import time
import math
import sys
import datetime
from time import sleep
from random import random
from threading import Timer
import random
import numpy as np
people=[]
for i in range(10):
    x = np.random.randint(0,100,(10,4))
    people.append(x)


def dist(x1, y1, x2, y2):
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

#########LOITERING IDENTIFICATION FOR MORE THAN 2 PEOPLE 

emp_centre = {}
emp=[]
b=[]
#for i, (person) in enumerate(people):

 #   count = 0
  #  for i, (sub_sub_list) in enumerate(person[0]):
for i in range(len(people)):
    #print(i,"+++++")
    #print(people[i],"=======")
        #if sub_sub_list[0] == 'E':
        
    emp_centre[i] = ((people[1][i][0] + people[1][i][2]) / 2, people[1][i][1],abs((people[1][i][2] - people[1][i][0])))
    #print(emp_centre)
if len(emp_centre) > 0:
    distance = sys.maxsize
    for (k, v) in emp_centre.items():
        #print("%%%%%%%%%%",k)
        #print("##########",v)
        emp_id1 = k
        x1, y1, width = v[0], v[1], v[2]
        for i, (key, coord) in enumerate(emp_centre.items()):
            wait_time=random.randint(1,9)
            #print("########",key)
            #print("^^^^^^^^",value)
            if key == k:
                continue
            temp = dist(x1, y1, coord[0], coord[1])
            #start = time.time()
            #time.sleep()
            normalisation = (width + coord[2]) / 2
            ndist = temp / normalisation
            #print("distance-----",ndist)

            #if temp == distance:
             #   continue
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
            if emp_id1!=emp_id2 and x in b:
                if ndist < 1:
                    print(emp_id1,"and",emp_id2,"could be loittering")
                    print("\n")
                    start = time.time()
                    time.sleep(wait_time)
                    done = time.time()
                    elapsed = done - start
                    #print("time-----",elapsed)
                    if elapsed > 6:
                        print(emp_id1,"and",emp_id2,"loittering!")
                        print("\n")
                        print("time spent loittering -----",elapsed,"seconds")
                        print("\n")
                    else:
                        print(emp_id1,"and",emp_id2,"not loittering anymore")
                        print("\n")
                else:
                    print(emp_id1,"and",emp_id2,"not loittering")
                    print("\n")


                



    
