from django.shortcuts import render
from django.template import RequestContext
from models import article
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login as auth_login
from django.core.urlresolvers import reverse
from django.shortcuts import redirect
import json
import socket

servers = {}

def article_list(request):
    print 'article list started'
    articles = sendData('List')
    return render(request, 'article_list.html', {'articles': articles.keys()}, context_instance=RequestContext(request))

def login(request):
    if request.method == "POST":
        username = request.POST.get('username');
        password = request.POST.get('password');
        print(username)
        print(password)
        user = authenticate(username=username)
        if user is not None:
            auth_login(request, user)
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
    data = {}
    data["flag"] = flag
    data["Article"] = article
    data['Content'] = content

    data = json.dumps(data)
    data = data.encode("utf-8")

    IP = '129.21.156.120'
    port = 5007

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))
    print 'data sent'
    # receive the data from content manager
    selfIP = "129.21.69.21"
    portForReceive = 7007

    receivingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receivingSock.bind((selfIP, portForReceive))

    data, addr = receivingSock.recvfrom(1024)
    print 'received data'
    print data
    map = json.loads(data.decode('utf-8'))
    return map

def sendCreate(flag, article, content = None):
    # send data to python server for content management
    print 'send data for ' + flag
    data = {}
    data["flag"] = flag
    data["Article"] = article
    data['Content'] = content

    data = json.dumps(data)
    data = data.encode("utf-8")

    IP = '129.21.156.120'
    port = 5007

    sendingSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sendingSock.sendto(data, (IP, port))
