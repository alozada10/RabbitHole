import time
import pandas as pd
from tweetcore.tasks import users_master
import warnings
import django

warnings.filterwarnings("error")
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


def export_followers_init(tw_ids: list = None):
    from tweetcore.models import TwUsers
    export = []
    for tw_id in tw_ids:
        user = TwUsers(tw_id=tw_id)
        export.append(user)
    TwUsers.objects.bulk_create(export)


if __name__ == "__main__":
    start_time = time.time()
    from tweetcore import credentials_refactor

    conf = credentials_refactor.return_credentials()
    accounts = pd.read_csv('data/main_accounts.csv')

    for i in range(accounts.shape[0]):
        screen = accounts[accounts.index == i]["tw_screen_name"].values[0]
        print(f'--- {screen} ---')
        export_user(configuration=conf,
                    screen_name=screen)
        print(f"--- %s minutes ---" % round((time.time() - start_time) / 60, 2))
        print(f'--- {screen} exported---\n\n')
