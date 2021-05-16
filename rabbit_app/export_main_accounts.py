import pandas as pd
from tweetcore.tasks import users_master
import django

django.setup()


def export_user(configuration: dict = None,
                tw_id: str = None,
                screen_name: str = None):
    user_info = users_master.get_user_info(configuration=configuration,
                                           tw_id=tw_id,
                                           screen_name=screen_name)
    from tweetcore.models import TwUsers
    user = TwUsers()
    user.tw_id = user_info['id_str']
    user.tw_screen = user_info['screen_name']
    user.tw_account_created_at = user_info['created_at']
    user.name = user_info['name']
    user.protected = user_info['protected']
    user.verified = user_info['verified']
    user.save()


if __name__ == "__main__":
    from tweetcore import credentials_refactor

    conf = credentials_refactor.return_credentials()
    followers = pd.read_csv('data/followers.csv')

    for i in range(followers.shape[0]):
        screen_name = followers[followers.index == i]["followers"].values[0]
        export_user(configuration=conf,
                    screen_name=screen_name)
