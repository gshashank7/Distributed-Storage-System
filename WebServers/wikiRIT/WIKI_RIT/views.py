from django.shortcuts import render
from django.template import RequestContext
from models import article
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login as auth_login
from django.core.urlresolvers import reverse
from django.shortcuts import redirect

def article_list(request):
    articles = article.objects.all()
    return render(request, 'article_list.html', {'articles': articles}, context_instance=RequestContext(request))

def login(request):
    if request.method == "POST":
        username = request.POST.get('username');
        password = request.POST.get('password');
        print(username)
        print(password)
        user = authenticate(username=username)
        if user is not None:
            auth_login(request, user)
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

def createArticle(request):
    if request.method == "POST":
        user = authenticate(username=request.user)
        if user is not None:
            article_title = request.POST.get('article_title')
            article_content = request.POST.get('article_content')
            article = { "type": "create", "article_title" : article_title, "article_content": article_content}
            print article
            return redirect('article_list')
        else:
            print("Not authorized")
            return render(request, 'create_article.html', {"message": "unauthorized access"}, context_instance=RequestContext(request))
    else:
        return render(request, 'create_article.html', {}, context_instance=RequestContext(request))
