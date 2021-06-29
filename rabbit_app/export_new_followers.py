from datetime import datetime
import pandas as pd
import time
from tweetcore.tasks import users_master
from export_main_accounts import export_followers_init
from tweetcore.lib.postgres_target import download_data
import django

django.setup()


def check_user_exists(tw_id: str = None) -> bool:
    from tweetcore.models import TwUsers
    count = TwUsers.objects.filter(tw_id=tw_id).count()
    return not count == 0


def check_users_exists(tw_ids: list = None) -> list:
    from tweetcore.models import TwUsers
    result = list(TwUsers.objects.filter(tw_id__in=tw_ids).values_list('tw_id'))
    result = [re[0] for re in result]
    final = [uid for uid in tw_ids if uid not in result]

    return final


def export_followers(users: dict = None,
                     date: datetime.date = datetime.now()):
    from tweetcore.models import Followers
    new_users = check_users_exists(tw_ids=list(users.keys()))

    if len(new_users) > 0:
        export_followers_init(tw_ids=new_users)
    else:
        pass
    export = []
    for tw_id in list(users.keys()):
        follower = Followers(tw_id=tw_id,
                             date_db_added=date,
                             **users[tw_id])
        export.append(follower)
    Followers.objects.bulk_create(export)


def update_followers(configuration: dict = None,
                     tw_id: str = None):
    start = users_master.get_user_number_followers(configuration=configuration,
                                                   tw_id=tw_id)
    query_check = f'''
                     select count(0) as count
                     from tweetcore_followers
                     where following_tw_id = '{tw_id}'
                    '''

    exists_check = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                              query=query_check)["count"].values[0]

    if exists_check == 0:
        print('--- No followers, starting from zero ---')

        followers = users_master.get_user_followers(configuration=configuration,
                                                    tw_id=tw_id,
                                                    first_time=True,
                                                    cursor=-1,
                                                    count=5000)
        print(f'--- {str(len(followers))} to be updated ---')
        export = {}
        sentinel = 0
        size_followers = len(followers)
        for j in range(1, size_followers + 1):
            follower_id = str(followers[size_followers - j])
            position = start - size_followers + sentinel + 1
            temp = {'following_tw_id': str(tw_id),
                    'following_position': position}
            export[str(follower_id)] = temp
            sentinel += 1
        export_followers(users=export)
        print(f'--- Added {str(sentinel)} followers ---')

    else:
        print('--- Updating followers ---')
        followers = users_master.get_user_followers(configuration=configuration,
                                                    tw_id=tw_id,
                                                    first_time=False,
                                                    cursor=-1,
                                                    count=5000)
        query_new = f'''
                    select tw_id
                    from tweetcore_followers
                    where following_tw_id = '{tw_id}'
                    and tw_id in {str(followers).replace('[', '(').replace(']', ')')}
        '''

        old_followers = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                                   query=query_new)
        if old_followers is None:
            new_followers = followers
        else:
            new_followers = [fi for fi in followers if fi not in old_followers["tw_id"].values]
        sentinel = 0
        size_followers = len(new_followers)
        if len(new_followers) > 0:
            print(f'--- {str(len(new_followers))} to be updated ---')
            export = {}
            for j in range(1, size_followers+1):
                follower_id = str(new_followers[size_followers - j])
                position = start - size_followers + sentinel + 1
                temp = {'following_tw_id': str(tw_id),
                        'following_position': position}
                export[str(follower_id)] = temp
                sentinel += 1
            export_followers(users=export)
        else:
            pass
        print(f'--- Added {str(sentinel)} followers ---')


if __name__ == "__main__":
    start_time = time.time()

    from tweetcore import credentials_refactor

    conf = credentials_refactor.return_credentials()
    accounts = pd.read_csv('data/main_accounts.csv')

    for i in range(accounts.shape[0]):
        screen = accounts[accounts.index == i]["tw_screen_name"].values[0]
        query = f'''
                select distinct tw_id
                from tweetcore_twusers
                where tw_screen = '{screen}' 
                '''
        main_account_id = download_data.pandas_df_from_postgre_query(configuration=conf,
                                                                     query=query)["tw_id"].values[0]
        print(f'--- {screen} ---')
        update_followers(configuration=conf,
                         tw_id=str(main_account_id))
        print(f"--- %s minutes ---" % round((time.time() - start_time) / 60, 2))
        print(f'--- {screen} updated --- \n\n')
