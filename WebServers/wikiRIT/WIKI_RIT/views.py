from django.shortcuts import render
from django.template import RequestContext
from models import article

def article_list(request):
    articles = article.objects.all()
    return render(request, 'article_list.html', {'articles': articles}, context_instance=RequestContext(request))

def homepage(request):
    return render(request, 'homepage.html', {}, context_instance=RequestContext(request))

def listArticles(request):
    return render(request, 'homepage.html', {}, context_instance=RequestContext(request))