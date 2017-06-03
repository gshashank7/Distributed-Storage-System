from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^articles/$', views.article_list, name='article_list'),
    url(r'^create_article/$', views.createArticle, name='create_article'),
    url(r'^login/$', views.login, name='login'),
    url(r'^create_user/$', views.createUser, name='create_user'),
    url(r'^save_user/$', views.saveUser, name='save_user'),
    url(r'^article/(?P<article_name>.+?)/$', views.getArticle, name='getArticle'),
    url(r'^edit_article/(?P<article_name>.+?)/$', views.updateArticle, name='getArticle'),
]
