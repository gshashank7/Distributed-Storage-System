'''
__Author__ : Shashank Gangadhara
'''

import socket
import json
import _thread
import random
from fractions import gcd
import time





#################################Digital Signature Class####################################

class DigitalSignature():
    '''
    This class has the functions to generate private keys and authenticate.
    '''

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

    def hash_Function(self,message):

        code = 0
        for letter in message:
            code += ord(letter)
        return code % 360

    def encrypt(self,m):

        m = self.hash_Function(m)

        y = (m**self.d) % self.n

        return y

    def authenticate(self,m,y,e,n):

        print("PUBLIC KEYS OF WEB SERVER ARE : " + str(e) + " AND " + str(n))

        print("SIGNATURE SENT WITH THE MESSAGE : " + str(y))

        m = self.hash_Function(m)

        print("HASH OF MESSAGE : " + str(m))

        print("DECRYPTING THE SIGNATURE USING PUBLIC KEYS")

        z = (y**e) % n

        print("DECRYPTED VALUE : " + str(z))


        if z == m:

            print("THE HASH MATCHES WITH THE DECRYPTED VALUE")

            return True


        else:

            print("THE HASH DOESN'T MATCH WITH THE DECRYPTED VALUE")

            return False

#################################Digital Signature Class####################################






################### Dictionary Declarations ##############

RangeToNode = {} # Maps Ranges to Nodes

NodeToRange = {} # Maps Nodes to Ranges

ContentHits = {} # Maps Contents and how many times it has been requested

NodeToPK = {}

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
while q==p:
    q = random.choice(ListOfPrimeNumbers)


DS = DigitalSignature(p,q)


WebServerE = None
WebServerN = None
#####################Variables#####################


def hash_Function(Name):
    '''
    This function takes the ASCII value of each character of the article name and mods with with 360
     and returns the hash value of that article
    '''
    code = 0
    for letter in Name:
        code += ord(letter)
    return code%360



def return_Node_For_Value(value):
    '''
    :param value: the hash value
    :return:  Node

    This function will return the node which owns the address space where the hash value lies
    '''

    if RangeToNode:
        for key in RangeToNode.keys():
            if value >= key[0] and value <= key[1]:
                return RangeToNode[key],key



def New_Node_Request(Node,Point,port):

#   :param Node:  The IP address of the new node that is joining
#   :param Point: The point at which the node wants to join
#   :param port:  Port of the newly joining node
#   :return:  None

#  This function is called when a new node wants to join the system, This function will divide the address space and
#  send the information to the node that is currently owning that address space.



   print("ADDING NEW NODE")
   print("NODE IS : " + str(Node))

   if len(RangeToNode) == 0:



       print("THIS IS THE FIRST NODE JOINING THE SYSTEM")

       startingPoint = 0

       endPoint = (0 -1)%360

       RangeToNode[(startingPoint,endPoint)] = Node
       NodeToRange[Node] = (startingPoint,endPoint)

       print("ADDED NODE AND THE RANGE IS : " +str(NodeToRange[Node]))

       data = {}

       data["flag"] = "JoinReply"
       data["Starting Point"] = startingPoint
       data["End Point"] = endPoint
       data["Old Starting Point"] = -1
       data["Left Node"] = "empty"
       data["Right Node"] = "empty"

       print("SENDING THIS INFORMATION TO THE NODE ")

       time.sleep(0.4)
       data = json.dumps(data)
       data = data.encode('utf-8')
       IP = Node
       sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sendingSock.sendto(data, (IP, port))


   else:


       currentNode, range = return_Node_For_Value(Point)


       print("CURRENT NODE HAVING THE ADDRESS SPACE IS " + str(currentNode))


       del RangeToNode[range]
       del NodeToRange[currentNode]

       data = {}

       RangeToNode[(range[0],((Point-1)%360))] = currentNode
       RangeToNode[(Point,range[1])] = Node

       NodeToRange[currentNode] = (range[0],(Point-1))
       NodeToRange[Node] = (Point,range[1])

       rightNode, rightNodeRange = return_Node_For_Value((range[1] + 1)%360)


       print("NEW CURRENT NODE RANGE : " + str(NodeToRange[currentNode]))
       print("NEW NODE RANGE : " + str(NodeToRange[Node]))
       print("LEFT NEIGHBOUR : " + str(currentNode))
       print("RIGHT NEIGHBOUR : " + str(rightNode))

       data["flag"] = "JoinReply"
       data["Starting Point"] = Point
       data["End Point"] = range[1]
       data["Old Starting Point"] = range[0]
       data["Left Node"] = currentNode
       data["Right Node"] = rightNode

       print("SENDING THIS INFORMATION TO THE NODE ")

       data = json.dumps(data)
       data= data.encode('utf-8')
       IP = Node
       sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sendingSock.sendto(data, (IP, port))



def Node_Failed_Notification(FailedNode, NotifyingNode, port):
    '''

    :param FailedNode:  The node that has failed
    :param NotifyingNode:  The node that is notifying this information
    :param port: the port through the notifying node is communicating
    :return: None

    This function is called when any node notifies that a node has failed.

    '''

    print("FAILED NODE : "+ str(FailedNode))

    print("FAILED NODE RANGE : " + str(NodeToRange[FailedNode]))

    print("NOTIFYING NODE RANGE" + str(NodeToRange[NotifyingNode]))

    print("HANDLING THE SITUATION")

    FailedNodeRange = NodeToRange[FailedNode]
    NotifyingNodeRange = NodeToRange[NotifyingNode]

    del RangeToNode[FailedNodeRange]
    del NodeToRange[FailedNode]
    del RangeToNode[NotifyingNodeRange]
    del NodeToRange[NotifyingNode]

    RangeToNode[(NotifyingNodeRange[0],FailedNodeRange[1])] = NotifyingNode
    NodeToRange[NotifyingNode] = (NotifyingNodeRange[0],FailedNodeRange[1])

    NewNeighbour,Range = return_Node_For_Value(FailedNodeRange[1]+1)

    print("NEW RANGE OF " + str(NotifyingNode) + " : " + str(NodeToRange[NotifyingNode]))
    print("NEW NEIGHBOR IS : " +str(NewNeighbour))

    print("SENDING THIS INFORMATION TO : " + str(NotifyingNode))

    data = {}
    data['flag'] = "FailureReply"
    data["NewNeighbor"] = NewNeighbour
    data["StartingPoint"] = NotifyingNodeRange[0]
    data["EndPoint"] = FailedNodeRange[1]

    data = json.dumps(data)
    data = data.encode("utf-8")
    IP = NotifyingNode
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))



def request_For_Article(article):


    hashValue = hash_Function(article)

    node = return_Node_For_Value(hashValue)

    # endPointofThisNode = NodeToRange[node1]
    #
    # endPointofThisNode = endPointofThisNode[1]
    #
    # node2 = return_Node_For_Value(endPointofThisNode)
    #
    # nodes = [node1,node2]
    #
    # node = random.choice(nodes)

    ContentHits[article]+=1

    data = {}



    data ["flag"] = "Read"


    data["Article"] = article
    data["DS"] = DS.encrypt(article)

    data = json.dumps(data)
    data = data.encode("utf-8")

    IP = node[0]

    # print("Sending Request for Article - " + str(article) + " to node " + str(IP))

    port = 9000

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))



def Send_Insertion_Request (article,update, content):

    '''

    :param article: Name of the article
    :param update: If it is an update or a new article
    :param content: The content of the new article
    :return: conformation that the article has been inserted successfully
    '''

    hashValue = hash_Function(article)

    node = return_Node_For_Value(hashValue)
    # print("Sending request to node - " +str(node))
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
    # print("")
    # print(data)
    data = data.encode("utf-8")

    IP = node[0]
    port = 9000
    # print("")
    # print(data)

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))


    # print("Request for Article " + article + "sent to data servers")





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





def send_My_Public_Key(IP,port):
    '''
    This is the function that is called as soon as the code is started to exchange the public keys
    :param IP: The IP address of the node that the public key is should be sent to
    :param port: the port to which the information needs to be sent
    :return: Node
    '''

    print("SENDING MY PUBLIC KEYS TO WEB SERVER")

    data = {}

    data['flag'] = "PK"

    data['e'] = DS.e

    data['n'] = DS.n

    data = json.dumps(data)

    data = data.encode("utf-8")

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sendingSock.sendto(data, (IP, port))

    print("SENT MY PUBLIC KEYS")



def send_List_To_Web_Server():
    '''
    This function just forwards the list of articles that is present in the data servers to the web server
    :return:  None
    '''

    IP = "129.21.69.21"
    port = 7007

    data = ContentHits
    data = json.dumps(data)
    data = data.encode("utf-8")
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

def send_Article_Response_To_Web_Server(article,content):
    '''
    This function forwards the received article to the web server
    :param article: Name of the article that the web server requested
    :param content: the content of the article requested by he web server in JSON format
    :return: None
    '''

    print("FORWARDING THIS INFORMATION TO WEB SERVER")
    data = {}
    IP = "129.21.69.21"
    port = 7007
    data['Article'] = article
    data['Content'] = content
    data['y'] = DS.encrypt(article)
    data = json.dumps(data)
    data = data.encode("utf-8")
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

    print("INFORMATION SENT")


def send_Conformation_To_Web_Server(IP,port):
    '''

    :param IP: The IP address of the web server
    :param port: the port to which the confirmation needs to be sent to
    :return: None
    '''
    data = {}

    data['flag'] = "Inserted"
    data = json.dumps(data)
    data = data.encode("utf-8")
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))




def receive_From_Web_Server():
    '''
    This function continuously listens to the web server and receives the data from it and responds to any request it wants
    :return: None
    '''

    global webServerE
    global webServerN

    IP = "129.21.156.120"
    port = 5007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))

    print("LISTENING TO WEB SERVER")

    while True:

        data, addr = receivingSock.recvfrom(1024)

        tempCheck = json.loads(data.decode('utf-8'))


        if tempCheck['flag'] == "PK":

            print("")
            print("RECEIVED PUBLIC KEY FROM WEB SERVER")

            webServerE = tempCheck['e']
            webServerN = tempCheck['n']
            send_My_Public_Key(addr[0],7007)


        elif tempCheck['flag'] == "Insert":



            content = tempCheck["Content"]
            article = tempCheck["Article"]

            y = tempCheck["y"]

            print("")
            print("NEW ARTICLE INSERT REQUEST RECEIVED")

            print("")

            print("-----------------------------------------------")
            print("AUTHENTICATING USING RSA SIGNATURE")

            if DS.authenticate(article,y,webServerE,webServerN) == True:

                print("AUTHENTICATED SUCCESSFULLY")
                print("-----------------------------------------------")
                print("")

                ContentHits[article] = 0

                print("INSERTING CONTENT AT THE APPROPRIATE DATA NODE")

                Send_Insertion_Request(article,False,content)

                send_Conformation_To_Web_Server(addr[0],7007)
                print("CONFORMATION SENT TO WEB SERVER")

            else:

                print("AUTHENTICATION FAILED")
                print("-----------------------------------------------")

        elif tempCheck['flag'] == "Read":

            article = tempCheck["Article"]

            y = tempCheck["y"]

            print("")
            print("READ REQUEST RECEIVED")

            print("")

            print("-----------------------------------------------")
            print("AUTHENTICATING USING RSA SIGNATURE")

            if DS.authenticate(article, y, webServerE, webServerN) == True:


                print("AUTHENTICATED SUCCESSFULLY")
                print("-----------------------------------------------")
                print("")

                print("FETCHING CONTENT FROM THE APPROPRIATE DATA NODE")
                request_For_Article(article)

            else:

                print("AUTHENTICATION FAILED")
                print("-----------------------------------------------")


        elif tempCheck['flag'] == "Update":

            article = tempCheck["Article"]

            content = tempCheck["Content"]

            y = tempCheck["y"]

            print("")
            print("UPDATE REQUEST RECEIVED")

            print("")

            print("-----------------------------------------------")
            print("AUTHENTICATING USING RSA SIGNATURE")

            if DS.authenticate(article, y, webServerE, webServerN) == True:

                print("AUTHENTICATED SUCCESSFULLY")
                print("-----------------------------------------------")
                print("")

                print("UPDATING CONTENT AT THE APPROPRIATE DATA NODE")

                Send_Insertion_Request(article,True,content)

                send_Conformation_To_Web_Server(addr[0], 7007)

                print("CONFORMATION SENT TO WEB SERVER")

            else:

                print("AUTHENTICATION FAILED")
                print("-----------------------------------------------")


        elif tempCheck['flag'] == "List":

            print(" ")
            print("REQUEST FOR LIST OF ARTICLES RECEIVED")
            send_List_To_Web_Server()
            print("SENT THE LIST OF ARTICLES TO WEB SERVER")


def receive_From_Data_Servers():
    '''
    This function continuously listens to the data severs and responds to all the requests by data servers
    :return: None
    '''

    print("LISTENING TO DATA NODES")

    # IP = socket.gethostbyname(socket.gethostname())
    IP = '129.21.156.120'

    port = 5006



    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((IP, port))



    while True:
        data, addr = receivingSock.recvfrom(1024)



        tempCheck = json.loads(data.decode('utf-8'))



        if tempCheck['flag'][0] == "Register":



            point = tempCheck["point"][0]

            port = int(tempCheck["port"][0])

            IP = tempCheck["IP"][0]


            print(" ")
            print("RECEIVED NEW NODE JOINING REQUEST")

            New_Node_Request(IP,point,port)

        elif tempCheck['flag'] == "FailureNotice":

            FailedNode = tempCheck['FailedNode'][0]


            print(FailedNode)

            NotifyingNode = tempCheck["IP"][0]

            print(NotifyingNode)

            port = tempCheck["port"][0]

            print("")
            print("RECEIVED NODE FAILED INFORMATION")

            Node_Failed_Notification(FailedNode,NotifyingNode,port)


        elif tempCheck['flag'] == "ArticleResponse":



            article = tempCheck['Article']
            content = tempCheck['Content']

            print("")
            print("RECEIVED RESPONSE FROM DATA SERVER FOR : " + str(article))

            send_Article_Response_To_Web_Server(article,content)







def main():
    '''
    This is where the program starts by starting two threads for receiving form the data server and the web serves respectively
    :return:
    '''

    _thread.start_new_thread(receive_From_Data_Servers,()) #

    receive_From_Web_Server()



if __name__ == '__main__':
    main()