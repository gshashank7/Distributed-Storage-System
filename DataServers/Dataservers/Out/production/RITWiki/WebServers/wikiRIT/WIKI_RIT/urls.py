from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^article/$', views.article_list, name='article_list'),
    url(r'^homepage/$', views.homepage, name='homepage'),
    url(r'^/$', views.listArticles, name='listArticles')
]