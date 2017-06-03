from django.db import models
from django.utils import timezone

# author: Nikhilesh Kshirsagar

class article(models.Model):
    article_title = models.CharField(max_length=500)
    article_content = models.TextField()
    created_date = models.DateTimeField(default=timezone.now)
    updated_date = models.DateTimeField(blank=True, null=True)
    author = models.ForeignKey('auth.User')
    image_name = models.TextField(blank=True, null=True)
    hash = models.TextField(blank=True, null=True)

    # def __str__(self):
    #     return self.article_title

