'''
__Author__ : Shashank Gangadhara
'''

import socket
import json


################### Dictionary Declarations ##############

RangeToNode = {} # Maps Ranges to Nodes

NodeToRange = {} # Maps Nodes to Ranges

ContentHits = {} # Maps Contents and how many tines it has been requested

################### Dictionary Declarations ##############




def hash_Function(Name):

    code = 0
    for letter in Name:
        code += ord(letter)
    return code%360



def return_Node_For_Value(value):


    if RangeToNode:
        for key in RangeToNode.keys():
            if value >= key[0] and value <= key[1]:
                return RangeToNode[key],key



def New_Node_Request(Node,Point,port):


   if len(RangeToNode) == 0:

       RangeToNode[(Point,(Point-1))] = Node
       NodeToRange[Node] = (Point,(Point-1))


   else:

       currentNode, range = return_Node_For_Value(Point)

       del RangeToNode[range]
       del NodeToRange[currentNode]

       data = {}

       RangeToNode[(range[0],(Point-1))] = currentNode

       RangeToNode[(Point,range[1])] = Node

       NodeToRange[currentNode] = (range[0],(Point-1))
       NodeToRange[Node] = (Point,range[1])

       data["starting point"] = Point
       data["end point"] = range[1]
       data["left Node"] = currentNode
       rightNode,rightNodeRange = return_Node_For_Value(range[1]+1)
       data["right node"] = rightNode
       data = json.dumps(data)
       data= data.encode('utf-8')
       IP = Node
       sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sendingSock.sendto(data, (IP, port))



def Node_Failed_Notification(FailedNode, NotifyingNode, port)

    FailedNodeRange = NodeToRange[FailedNode]
    NotifyingNodeRange = NodeToRange[NotifyingNode]
    del RangeToNode[FailedNode]
    del NodeToRange[FailedNodeRange]
    del RangeToNode[NotifyingNode]
    del NodeToRange[NotifyingNodeRange]

    RangeToNode[(NotifyingNodeRange[0],FailedNodeRange[1])] = NotifyingNode
    NodeToRange[NotifyingNode] = (NotifyingNodeRange[0],FailedNodeRange[1])

    NewNeighbour,Range = return_Node_For_Value(FailedNodeRange[1])

    data = {}
    data["New Neighbour"] = NewNeighbour
    data["starting point"] = NotifyingNodeRange[0]
    data["end point"] = FailedNodeRange[1]

    data = json.dumps(data)
    data = data.encode("utf-8")
    IP = NotifyingNode
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))





# print(y)