import random

from django.shortcuts import render
from django.template import RequestContext
from models import article
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login as auth_login
from django.core.urlresolvers import reverse
from django.shortcuts import redirect
import json
import socket
from fractions import gcd
<<<<<<< HEAD

=======
import time
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
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


    def hash_Function(self,message):

        code = 0
        for letter in message:
            code += ord(letter)
        return code % 360


    def encrypt(self,m):

        m = self.hash_Function(m)
<<<<<<< HEAD

        y = (m**self.d) % self.n

        return y

    def authenticate(self,m,y,e,n):

        m = self.hash_Function(m)

        z = (y**e) % n

        if z == m:

            return True
        else:

            return False

#################################Digital Signature Class####################################

=======

        y = (m**self.d) % self.n

        return y

    def authenticate(self,m,y,e,n):

        m = self.hash_Function(m)

        z = (y**e) % n

        if z == m:

            return True
        else:

            return False

#################################Digital Signature Class####################################

>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
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

<<<<<<< HEAD

p = random.choice(ListOfPrimeNumbers)

q = p
while q!=p:
=======
sendingIP = '129.21.156.120'
p = random.choice(ListOfPrimeNumbers)

q = p
while q==p:
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
    q = random.choice(ListOfPrimeNumbers)


DS = DigitalSignature(p,q)

def article_list(request):
    print 'article list started'
    articles = sendData('List')
    return render(request, 'article_list.html', {'articles': articles.keys()}, context_instance=RequestContext(request))

reqSent = 0
publicKeys = {'e': 0, 'n': 0}

def login(request):
    global reqSent
    if request.method == "POST":
        username = request.POST.get('username');
        password = request.POST.get('password');
        print(username)
        print(password)
        user = authenticate(username=username)
        if user is not None:
            auth_login(request, user)
            if(reqSent == 0):
                reqSent = 1
                sendKeys()
            print 'redirecting to article list'
            return redirect('article_list')
        else:
            print "user not authenticated"
            return render(request, 'login.html', {"message": "cannot login"}, context_instance=RequestContext(request))
    else:
        return render(request, 'login.html', {}, context_instance=RequestContext(request))

def createUser(request):
    return render(request, 'create_user.html', {}, context_instance=RequestContext(request))

def saveUser(request):
    username = request.POST.get('username');
    password = request.POST.get('password');
    print(username)
    print(password)
    user = User.objects.create_user(username=username, password=password)
    return render(request, 'login.html', {"user": user}, context_instance=RequestContext(request))

def updateArticle(request, article_name):
    if request.method == "POST":
        user = authenticate(username=request.user)
        if user is not None:
            article_title = request.POST.get('article_title')
            article_content = request.POST.get('article_content')
            print article_content
            sendCreate('Update', article_title, article_content)
            return redirect('article_list')
        else:
            print("Not authorized")
            return render(request, 'article.html', {"article_data": ""},
                          context_instance=RequestContext(request))

def createArticle(request):
    if request.method == "POST":
        user = authenticate(username=request.user)
        if user is not None:
            article_title = request.POST.get('article_title')
            article_content = request.POST.get('article_content')
            sendCreate('Insert', article_title, article_content)
            return redirect('article_list')
        else:
            print("Not authorized")
            return render(request, 'create_article.html', {"message": "unauthorized access"}, context_instance=RequestContext(request))
    else:
        return render(request, 'create_article.html', {}, context_instance=RequestContext(request))

def getArticle(request, article_name):
    print article_name
    article_data = sendData('Read', article_name)
    return render(request, 'article.html', {"article_data": article_data}, context_instance=RequestContext(request))

def sendData(flag, article=None, content = None):
    # send data to python server for content manager
    print 'send data for ' + flag
    if flag != 'List':
        print article
        y = DS.encrypt(article)

    data = {}
    data["flag"] = flag
    data["Article"] = article
    data['Content'] = content
    if flag != 'List':
        data['y'] = y

    data = json.dumps(data)
    data = data.encode("utf-8")
    ##129.21.22.196
    IP = sendingIP
    port = 5007

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))
    print 'data sent'
    print data
    # receive the data from content manager
    selfIP = "129.21.69.21"
    portForReceive = 7007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((selfIP, portForReceive))

    datar, addr = receivingSock.recvfrom(1024)
    print 'received data'
<<<<<<< HEAD
    print data
    map = json.loads(data.decode('utf-8'))
    if (flag != 'List'):
        auth = DS.authenticate(map['Article'], map['y'], publicKeys['e'], publicKeys['n'])
=======
    map = json.loads(datar.decode('utf-8'))
    print map
    if (flag != 'List'):
        print publicKeys['e']
        print publicKeys['n']
        print '#'*50
        print 'Public Keys \n'
        print str(publicKeys['e']) + ', ' + str(publicKeys['n'])
        auth = DS.authenticate(map['Article'], map['y'], publicKeys['e'], publicKeys['n'])
        print '#' * 50
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
        if(auth==True):
            return map
        else:
            print 'Authentication failed for article ' + map['Article']
    else:
        return map

def sendCreate(flag, article, content = None):
    # send data to python server for content management
    print 'send data for ' + flag
    print article
<<<<<<< HEAD
    y = DS.encrypt(str(article))
=======
    y = DS.encrypt(article)
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
    print 'Encryption done'
    data = {}
    data["flag"] = flag
    data["Article"] = article
    data['Content'] = content
    data['y'] = y

    data = json.dumps(data)
    data = data.encode("utf-8")
    print data
    IP = sendingIP
    port = 5007

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))
<<<<<<< HEAD
=======
    selfIP = "129.21.69.21"
    portForReceive = 7007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((selfIP, portForReceive))

    data, addr = receivingSock.recvfrom(1024)
    print data
    print 'received data'
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76

def sendKeys():
    data = {}
    data['flag'] = 'PK'
    data['e'] = DS.e
    data['n'] = DS.n
    data = json.dumps(data)
    data = data.encode("utf-8")

<<<<<<< HEAD
    IP = '129.21.156.120'
=======
    print '#'*50
    print 'Sending public keys'
    print str(DS.e) + ', ' + str(DS.n)

    IP = sendingIP
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
    port = 5007
    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))

    selfIP = "129.21.69.21"
    portForReceive = 7007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((selfIP, portForReceive))
    print 'receiving'
    data, addr = receivingSock.recvfrom(1024)
    print 'received data'
    print data
    map = json.loads(data.decode('utf-8'))
    publicKeys['e'] = map['e']
    publicKeys['n'] = map['n']
<<<<<<< HEAD
=======
    print 'Public keys received'
    print str(publicKeys['e']) + ', ' + str(publicKeys['n'])
    print '#' * 50
>>>>>>> 0fdd42b2ba3bddd22d956094ffbfd5b5d3cefc76
    return map
