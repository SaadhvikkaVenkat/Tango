##################
#ORIGINAL#
##################

def Intersection(list1, list2):
    return list( set(list1).intersection(list2) )

def isIntersection( list1, list2 ):
    temp = list(set(list1).intersection(list2))
    if len( temp ) == 0:
        return False
    return True
       
def addList( depth, i ):
    if i[1] not in depth.keys() :
        depth[ i[1] ] = [i[0],i[1]]
    else:
        depth[ i[1] ].append( i[0] )

a = [[0, 7], [0, 11], [1, 2], [1, 3], [1, 4], [1, 6], [1, 7], [1, 9], [1, 13], [2, 3], [2, 4], [2, 5], [2, 7], [2, 9], [2, 10], [2, 14], [3, 4], [3, 5], [3, 7], [3, 8], [3, 10], [3, 14], [4, 7], [4, 9], [5, 7], [5, 8], [5, 9], [5, 14], [6, 13], [7, 8], [7, 11], [7, 14], [8, 9], [9, 11], [9, 14], [10, 12]]

#a = [[0, 1], [1, 2], [2, 3], [2, 4], [2, 5], [3, 4], [3, 5], [4,5]]
depth = { }

previous = -1

current = -1

for i in a:
    current = i[0]
    if previous == current :
        depth[current].append( i[1] )
    elif previous != current :
        if current not in depth.keys() :
            depth[current] = [i[0], i[1]]
        else:
            depth[current].append( i[1] )
        previous = current
    addList( depth, i )
#print( depth )
n=[]

import itertools
for k in depth.keys():
    for i in range(3, len( depth[k] ) + 1 ):
        for j in list( itertools.combinations( depth[k], i ) ):
            commanFriends = depth[j[0]]
            flag = 0
            for l in j[1:]:
                if l in commanFriends:
                    if( isIntersection( commanFriends, depth[l] ) ):
                        commanFriends = Intersection( commanFriends, depth[l] )
                    else:
                        flag = 1
                        break
                else:
                    flag = 1
            if( flag == 0 and len(j) > 2 and j not in n ):
                n.append(j)
print(n)

output = list(set(map(lambda x: tuple(sorted(x)),n)))
out=[]
print(len(output))
print(len(n))
for i in output:
    out.append(list(i))
print(out)

sets=[set(l) for l in out]
new=[l for l,s in zip(out,sets) if not any(s< other for other in sets)]
print(new)
