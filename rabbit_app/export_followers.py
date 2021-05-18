import pandas as pd
from tweetcore.tasks import users_master
from export_main_accounts import export_user
from tweetcore.lib.postgres_target import download_data
import django

django.setup()


def check_user_exists(tw_id: str = None) -> bool:
    from tweetcore.models import TwUsers
    count = TwUsers.objects.filter(tw_id=tw_id).count()
    return not count == 0


def get_user_number_followers(configuration: dict = None,
                              tw_id: str = None) -> int:
    user_followers = users_master.get_user_info(configuration=configuration,
                                                tw_id=tw_id,
                                                fields=['followers_count'])
    return user_followers["followers_count"]


def export_follower(configuration: dict = None,
                    tw_id: str = None,
                    following_tw_id: str = None,
                    position: int = None):
    from tweetcore.models import TwUsers, Followers
    if not check_user_exists(tw_id=tw_id):
        export_user(configuration=configuration,
                    tw_id=tw_id)
        pass
    else:
        pass

    user = TwUsers.objects.get(tw_id=tw_id)
    follower = Followers(tw_user=user)
    follower.following_tw_id = following_tw_id
    follower.following_position = position
    follower.save()


def update_followers(configuration: dict = None,
                     tw_id: str = None):
    query_check = f'''
                    select count(0) as count
                    from tweetcore_followers
                    where following_tw_id = '{tw_id}'
                    '''

    rows = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                      query=query_check)["count"].values[0]
    followers = users_master.get_user_followers(configuration=configuration,
                                                tw_id=tw_id)[0]
    start = get_user_number_followers(configuration=configuration,
                                      tw_id=tw_id)
    if rows == 0:
        print('--- No followers, starting from zero ---\n')
        sentinel = 0
        for j in range(len(followers)):
            follower_id = str(followers[j])
            position = start - sentinel
            export_follower(configuration=configuration,
                            tw_id=follower_id,
                            following_tw_id=tw_id,
                            position=position)
            sentinel += 1
        print(f'--- Added {str(sentinel)} followers ---\n')
        pass
    else:
        print('--- Adding new followers ---\n')
        sentinel = 0
        for j in range(len(followers)):
            follower_id = str(followers[j])
            query_f = f'''
                           select count(0) as count
                           from tweetcore_followers
                           where following_tw_id = '{tw_id}'
                           and tw_user_id = '{follower_id}'
                           '''
            rows_f = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                                query=query_f)["count"].values[0]
            if rows_f == 0:
                position = start - sentinel
                export_follower(configuration=configuration,
                                tw_id=follower_id,
                                following_tw_id=tw_id,
                                position=position)
                sentinel += 1
            else:
                pass
        print(f'--- Added {str(sentinel)} followers ---\n')


if __name__ == "__main__":
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
        update_followers(configuration=conf,
                         tw_id=str(main_account_id))
        print(f'--- {screen} updated --- \n\n')
