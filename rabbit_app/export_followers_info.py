import tweepy as tp
import time
from tweetcore.tasks import users_master
from tweetcore.lib.postgres_target import download_data
import django

django.setup()


def export_follower_info(configuration: dict = None) -> int:
    query_info = '''
    select tw_id
    from tweetcore_twusers
    where tw_screen = '-999'
    and name = '-999'
    limit 900
    '''

    query_latency = '''
        select count(0) as count
        from tweetcore_twusers
        where tw_screen = '-999'
        and name = '-999'
        '''

    df_followers = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                              query=query_info)
    df_latency = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                            query=query_latency)
    problem_size = df_latency["count"].values[0]

    if problem_size == 0:
        print('--- No followers to update ---')
        pass
    else:
        followers_update = df_followers["tw_id"].values
        print(f'--- {str(len(followers_update))} followers to update ---')
        from tweetcore.models import TwUsers
        for follower_id in followers_update:
            user = TwUsers.objects.get(tw_id=follower_id)
            try:
                user_info = users_master.get_user_info(configuration=configuration,
                                                       tw_id=follower_id)
                user.tw_screen = user_info['screen_name']
                user.tw_account_created_at = user_info['created_at']
                user.name = user_info['name']
                user.protected = user_info['protected']
                user.verified = user_info['verified']
                user.save()
            except tp.TweepError as error:
                if error.args[0][0]['code'] == 63:
                    msg = error.args[0][0]['message']
                    print(f'--- {follower_id} ---')
                    print(f'--- {msg} ---')
                    pass
                else:
                    raise
        print(f'--- {str(df_followers.shape[0])} of {str(problem_size)} ---')
    return problem_size


if __name__ == "__main__":
    start_time = time.time()

    from tweetcore import credentials_refactor

    conf = credentials_refactor.return_credentials()

    while True:
        sentinel = export_follower_info(configuration=conf)
        print(f"--- %s minutes ---" % round((time.time() - start_time) / 60, 2))

        if sentinel == 0:
            break
        else:
            continue
