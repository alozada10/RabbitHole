from datetime import datetime
import pandas as pd
import time
from tweetcore.tasks import users_master
from export_main_accounts import export_user
from tweetcore.lib.postgres_target import download_data, upload_data
import django

django.setup()


def check_user_exists(tw_id: str = None) -> bool:
    from tweetcore.models import TwUsers
    count = TwUsers.objects.filter(tw_id=tw_id).count()
    return not count == 0


def export_follower(configuration: dict = None,
                    tw_id: str = None,
                    following_tw_id: str = None,
                    position: int = None,
                    date: datetime.date = datetime.today().date()):
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
    follower.date_db_added = date
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
    start = users_master.get_user_number_followers(configuration=configuration,
                                                   tw_id=tw_id)
    if rows == 0:
        print('--- No followers, starting from zero ---')
        followers = users_master.reconstruct_follower_history(configuration=configuration,
                                                              tw_id=tw_id,
                                                              cursor=-1,
                                                              count=5000)
        print(f'--- {str(len(followers))} to be updated ---')
        sentinel = 0
        for j in range(len(followers)):
            follower_id = str(followers[j])
            position = start - sentinel
            export_follower(configuration=configuration,
                            tw_id=follower_id,
                            following_tw_id=tw_id,
                            position=position)
            sentinel += 1
        print(f'--- Added {str(sentinel)} followers ---')
        pass
    else:
        print('--- Adding new followers ---')
        followers = users_master.get_user_followers(configuration=configuration,
                                                    tw_id=tw_id,
                                                    first_time=False)
        df_followers = pd.DataFrame(data={'followers': followers})
        upload_data.write_postgre_table(configuration=configuration,
                                        data=df_followers,
                                        table_name='temp_new_followers',
                                        schema='public',
                                        if_exists_then_wat='replace')
        query_new = f'''
                    select followers::varchar as tw_id
                    from temp_new_followers
                    where followers::varchar not in (select t1.tw_id::varchar as tw_id
                                                     from tweetcore_followers t0
                                                     left join tweetcore_twusers t1
                                                     on t0.tw_user_id::varchar = t1.id::varchar
                                                     where t0.following_tw_id::varchar = '{tw_id}')
        '''
        new_followers = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                                   query=query_new)
        if new_followers is not None:
            new_followers = new_followers["tw_id"].values
            sentinel = 0
            for j in range(len(new_followers)):
                follower_id = str(new_followers[j])
                position = start - sentinel
                export_follower(configuration=configuration,
                                tw_id=follower_id,
                                following_tw_id=tw_id,
                                position=position)
                sentinel += 1

            print(f'--- Added {str(sentinel)} followers ---')
        else:
            print('--- No new followers ---')


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
