import tweepy as tp
import time
from tweetcore.tasks import users_master
from tweetcore.lib.postgres_target import download_data
import pandas as pd
import django

django.setup()


def export_follower_info(configuration: dict = None) -> int:
    df_trash = pd.read_csv('data/blocked_users.csv')
    black_list = df_trash.trash_users.values.tolist()
    if len(black_list) == 0:
        black_list_statement = ''
    elif len(black_list) > 0:
        black_list = [str(i) for i in black_list]
        black_list_statement = f'''
        and tw_id not in {str(black_list).replace('[', '(').replace(']', ')')}
        '''
    else:
        print('Something odd happened!')
        raise

    query_info = f'''
    select tw_id
    from tweetcore_twusers
    where (tw_screen = '-999'
    or name = '-999'
    or extract(year from tw_account_created_at) = 1999)
    {black_list_statement}
    limit 900
    '''

    query_latency = f'''
    select count(0) as count
    from tweetcore_twusers
    where (tw_screen = '-999'
    or name = '-999'
    or extract(year from tw_account_created_at) = 1999)
    {black_list_statement}
    '''

    df_followers = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                              query=query_info)
    df_latency = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                            query=query_latency)
    problem_size = df_latency["count"].values[0]
    balance = 0
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
                if error.args[0][0]['code'] in [63, 50, 131]:
                    msg = error.args[0][0]['message']
                    new_row = f'{str(follower_id)}, {msg}'
                    with open('data/blocked_users.csv', 'a') as file:
                        file.write('\n')
                        file.write(new_row)
                    # print(f'--- {follower_id} ---')
                    # print(f'--- {msg} ---')
                    balance += 1
                    pass
                else:
                    raise
        print(f'--- {str(df_followers.shape[0])} of {str(problem_size)} ---')
    return problem_size - balance


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
            continue  # change 2
