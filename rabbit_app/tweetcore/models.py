from django.db import models


class TwUsers(models.Model):
    tw_id = models.CharField(max_length=200)
    tw_screen = models.CharField(max_length=50)
    tw_account_created_at = models.DateTimeField('date user created')
    follower = models.BooleanField('False for main account, True for follower')


class Followers(models.Model):
    tw_user = models.ForeignKey(TwUsers, on_delete=models.CASCADE)
    following_tw_id = models.CharField(max_length=200)
    following_position = models.IntegerField()
