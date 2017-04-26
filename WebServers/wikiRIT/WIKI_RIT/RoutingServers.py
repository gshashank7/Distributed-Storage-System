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

################### FLAGS ##############

AnyChangeinNode = False

AnyChangeinContent = False

################### FLAGS ##############


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

   print("Got a new node joining request at point " + str(Point))


   if len(RangeToNode) == 0:

       print("Adding new Node")
       print("This is the first node joining to the system")

       startingPoint = Point

       endPoint = (Point -1)%360

       RangeToNode[(startingPoint,endPoint)] = Node
       NodeToRange[Node] = (startingPoint,endPoint)

       print("Added Node")
       print("The range of the first node is " + str(NodeToRange[Node]))

       data = {}

       data["flag"] = "JoinReply"
       data["starting point"] = Point
       data["end point"] = range[1]
       data["old starting point"] = "empty"
       data["left Node"] = 'empty'

       print("Sending this information to the new Node")

       data["right node"] = 'empty'
       data = json.dumps(data)
       data = data.encode('utf-8')
       IP = Node
       sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sendingSock.sendto(data, (IP, port))


   else:


       currentNode, range = return_Node_For_Value(Point)

       print("The current Node at using that address space is " + str(currentNode))
       print("Adding new node")

       del RangeToNode[range]
       del NodeToRange[currentNode]

       data = {}

       RangeToNode[(range[0],((Point-1)%360))] = currentNode
       RangeToNode[(Point,range[1])] = Node

       NodeToRange[currentNode] = (range[0],(Point-1))
       NodeToRange[Node] = (Point,range[1])

       rightNode, rightNodeRange = return_Node_For_Value(range[1] + 1)


       print("New range of Current Node : " + str(NodeToRange[currentNode]))
       print("Range of New Node is  : " + str(NodeToRange[Node]))
       print("Left Node of the new node is : " + str(currentNode))
       print("Right Node of the new node is " + str(rightNode))

       data["flag"] = "JoinReply"
       data["starting point"] = Point
       data["end point"] = range[1]
       data["old starting point"] = range[0]
       data["left Node"] = currentNode

       print("Sending this information to the new Node")

       data["right node"] = rightNode
       data = json.dumps(data)
       data= data.encode('utf-8')
       IP = Node
       sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sendingSock.sendto(data, (IP, port))



def Node_Failed_Notification(FailedNode, NotifyingNode, port):

    print("Received information that the Node - "+ str(FailedNode) + " has failed")

    print("Range of Failed node = " + str(NodeToRange[FailedNode]))

    print("Range of Notifying node = " + str(NodeToRange[NotifyingNode]))

    print("Performing operations to handle this situation ")

    FailedNodeRange = NodeToRange[FailedNode]
    NotifyingNodeRange = NodeToRange[NotifyingNode]

    del RangeToNode[FailedNode]
    del NodeToRange[FailedNodeRange]
    del RangeToNode[NotifyingNode]
    del NodeToRange[NotifyingNodeRange]

    RangeToNode[(NotifyingNodeRange[0],FailedNodeRange[1])] = NotifyingNode
    NodeToRange[NotifyingNode] = (NotifyingNodeRange[0],FailedNodeRange[1])

    NewNeighbour,Range = return_Node_For_Value(FailedNodeRange[1])

    print("New range of " + str(NotifyingNode) + " is " + str(NodeToRange[NotifyingNode]))

    print("Sending this information to " + str(NotifyingNode))

    data = {}
    data['flag'] = "FailureReply"
    data["New Neighbour"] = NewNeighbour
    data["starting point"] = NotifyingNodeRange[0]
    data["end point"] = FailedNodeRange[1]

    data = json.dumps(data)
    data = data.encode("utf-8")
    IP = NotifyingNode
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))



def request_For_Article(article):

    hashValue = hash_Function(article)

    node = return_Node_For_Value(hashValue)

    ContentHits[article]+=1

    data = {}

    data ["flag"] = "ArticleRequest"
    data["article"] = article

    IP = node
    port = 8006

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

def Send_Insertion_Request (article):

    hashValue = hash_Function(article)

    node = return_Node_For_Value(hashValue)

    data = {}
    data["flag"] = "ArticleInsert"
    data["article"] = article

    IP = node
    port = 8006

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

    ContentHits [article] = 0


def receive_From_Routing_Server():
    IP = socket.gethostname()
    port = 5005

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))

    while True:

        data, addr = receivingSock.recvfrom(1024)

        tempCheck = json.loads(data.decode('utf-8'))

        if tempCheck['flag'] == "syncNode":

           RangeToNode= tempCheck["RangeToNode"]

           NodeToRange = tempCheck["NodeToRange"]

        elif tempCheck['flag'] == "Content":

            ContentHits = tempCheck["ContentHits"]


def receive_From_Data_Servers():

    print("started receiving")

    IP = socket.gethostname()
    port = 5006

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))

    while True:
        data, addr = receivingSock.recvfrom(1024)

        tempCheck = json.loads(data.decode('utf-8'))

        if tempCheck['flag'] == "Register":
            point = tempCheck["point"]

            port = tempCheck["port"]

            New_Node_Request(addr[0],point,port)

        elif tempCheck['flag'] == "FailureNotice":

            FailedNode = tempCheck['FailedNode']

            NotifyingNode = addr[0]

            port = tempCheck[port]

            Node_Failed_Notification(FailedNode,NotifyingNode,port)




def main():


    receive_From_Data_Servers()




if __name__ == '__main__':
    main()