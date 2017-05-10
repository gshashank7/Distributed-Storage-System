'''
__Author__ : Shashank Gangadhara
'''

import socket
import json
import _thread
import random
from fractions import gcd






#################################Digital Signature Class####################################

class DigitalSignature():



    def __init__(self,p,q):

        self.p = p
        self.q = q

        self.n = self.p* self.q
        self.phiOfn = (self.p-1) * (self.q-1)
        self.efound = False

        while self.efound != True:

            self.e = random.randint(0, self.phiOfn)

            if gcd(self.e, self.phiOfn) == 1:
                self.efound = True


        self.d = 0

        while (self.e*self.d) % self.phiOfn != 1:

            self.d+=1


    def hash_Function(message):

        code = 0
        for letter in message:
            code += ord(letter)
        return code % 360


    def encrypt(self,m):

        m = self.hash_Function(m)

        y = (m**self.d) % self.n

        return y

    def authenticate(self,m,y,e,n):

        z = (y**e) % n

        if z == m:

            return True
        else:

            return False

#################################Digital Signature Class####################################






################### Dictionary Declarations ##############

RangeToNode = {} # Maps Ranges to Nodes

NodeToRange = {} # Maps Nodes to Ranges

ContentHits = {} # Maps Contents and how many tines it has been requested

################### Dictionary Declarations ##############

################### FLAGS ##############

AnyChangeinNode = False

AnyChangeinContent = False

################### FLAGS ##############


#####################Variables#####################

ListOfPrimeNumbers = [2,3,5,7,11,13,17,19,23,29,31
,37,41,43,47,53,59,61,67,71
,73,79,83,89,97,101,103,107,109,113
,127,131,137,139,149,151,157,163,167,173
,179,181,191,193,197,199,211,223,227,229
,233,239,241,251,257,263,269,271,277,281
,283,293,307,311,313,317,331,337,347,349
,353,359,367,373,379,383,389,397,401,409
,419,421,431,433,439,443,449,457,461,463
,467,479,487,491,499,503,509,521,523,541
,547,557,563,569,571,577,587,593,599,601
,607,613,617,619,631,641,643,647,653,659
,661,673,677,683,691,701,709,719,727,733
,739,743,751,757,761,769,773,787,797,809
,811,821,823,827,829,839,853,857,859,863
,877,881,883,887,907,911,919,929,937,941
,947,953,967,971,977,983,991,997]


p = random.choice(ListOfPrimeNumbers)

q = p
while q!=p:
    q = random.choice(ListOfPrimeNumbers)


DS = DigitalSignature(p,q)

#####################Variables#####################


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

       startingPoint = 0

       endPoint = (0 -1)%360

       RangeToNode[(startingPoint,endPoint)] = Node
       NodeToRange[Node] = (startingPoint,endPoint)

       print("Added Node")
       print("The range of the first node is " + str(NodeToRange[Node]))

       data = {}

       data["flag"] = "JoinReply"
       data["Starting Point"] = startingPoint
       data["End Point"] = endPoint
       data["Old Starting Point"] = -1
       data["Left Node"] = "empty"
       data["Right Node"] = "empty"

       print("Sending this information to the new Node")

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

       rightNode, rightNodeRange = return_Node_For_Value((range[1] + 1)%360)


       print("New range of Current Node : " + str(NodeToRange[currentNode]))
       print("Range of New Node is  : " + str(NodeToRange[Node]))
       print("Left Node of the new node is : " + str(currentNode))
       print("Right Node of the new node is " + str(rightNode))

       data["flag"] = "JoinReply"
       data["Starting Point"] = Point
       data["End Point"] = range[1]
       data["Old Starting Point"] = range[0]
       data["Left Node"] = currentNode
       data["Right Node"] = rightNode

       print("Sending this information to the new Node")

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
    data['flag'] = "Failure Reply"
    data["New Neighbour"] = NewNeighbour
    data["Starting Point"] = NotifyingNodeRange[0]
    data["End Point"] = FailedNodeRange[1]

    data = json.dumps(data)
    data = data.encode("utf-8")
    IP = NotifyingNode
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))



def request_For_Article(article):


    hashValue = hash_Function(article)

    node1 = return_Node_For_Value(hashValue)

    endPointofThisNode = NodeToRange[node1]

    endPointofThisNode = endPointofThisNode[1]

    node2 = return_Node_For_Value(endPointofThisNode)

    nodes = [node1,node2]

    node = random.choice(nodes)

    ContentHits[article]+=1

    data = {}



    data ["flag"] = "ArticleRequest"


    data["Article"] = article
    data["DS"] = DS.encrypt(article)

    data = json.dumps(data)
    data = data.encode("utf-8")

    IP = node
    port = 8006

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))



def Send_Insertion_Request (article,update, content):

    hashValue = hash_Function(article)

    node = return_Node_For_Value(hashValue)

    data = {}

    if update == True:

        data["flag"] = "Update"
    else:
        data["flag"] = "Insert"

    data["Article"] = article
    data['Content'] = content
    data['Hash'] = hashValue
    data['DS'] = DS.encrypt(article)


    data = json.dumps(data)
    data = data.encode("utf-8")

    IP = node
    port = 8006

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

    ContentHits [article] = 0






# def receive_From_Routing_Server():
#
#     IP = socket.gethostname()
#     port = 5005
#
#     receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     receivingSock.bind((IP, port))
#
#     while True:
#
#         data, addr = receivingSock.recvfrom(1024)
#
#         tempCheck = json.loads(data.decode('utf-8'))
#
#         if tempCheck['flag'] == "syncNode":
#
#            RangeToNode= tempCheck["RangeToNode"]
#
#            NodeToRange = tempCheck["NodeToRange"]
#
#         elif tempCheck['flag'] == "Content":
#
#             ContentHits = tempCheck["ContentHits"]





def send_List_To_Web_Server():


    IP = "129.21.69.21"
    port = 7007

    data = ContentHits
    data = json.dumps(data)
    data = data.encode("utf-8")
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

    print("Sent the list")



def receive_From_Web_Server():

    IP = "129.21.156.120"
    port = 5007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))

    print("Started Receiving from web server")

    while True:

        data, addr = receivingSock.recvfrom(1024)

        print("Received something")
        tempCheck = json.loads(data.decode('utf-8'))

        print(tempCheck)

        if tempCheck['flag'] == "Insert":

            print("Received Insertion request")

            content = tempCheck["Content"]
            article = tempCheck["Article"]
            ContentHits[article] = 0
            Send_Insertion_Request(article,False,content)


        elif tempCheck['flag'] == "Read":

            article = tempCheck["Article"]

            request_For_Article(article)

        elif tempCheck['flag'] == "Update":

            article = tempCheck["Article"]

            Send_Insertion_Request(article,False,content)

        elif tempCheck['flag'] == "List":

            print("Received List Request")
            send_List_To_Web_Server()






def receive_From_Data_Servers():

    print("started receiving")

    IP = "129.21.159.137"
    port = 5006

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))



    while True:
        data, addr = receivingSock.recvfrom(1024)

        print("Some data received of type " + str(type(data)))

        tempCheck = json.loads(data.decode('utf-8'))

        print(tempCheck)

        if tempCheck['flag'][0] == "Register":
            point = tempCheck["point"][0]

            port = int(tempCheck["port"][0])

            New_Node_Request(addr[0],point,port)

        elif tempCheck['flag'] == "Failure Notice":

            FailedNode = tempCheck['Failed Node'][0]

            NotifyingNode = addr[0]

            port = tempCheck[port][0]

            Node_Failed_Notification(FailedNode,NotifyingNode,port)




def main():


    _thread.start_new_thread(receive_From_Data_Servers,())

    receive_From_Web_Server()



if __name__ == '__main__':
    main()