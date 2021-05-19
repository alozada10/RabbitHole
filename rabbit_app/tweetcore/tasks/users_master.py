import time
from tweetcore.tasks import twitter_connect as tc
from tweetcore.lib.postgres_target import download_data
from datetime import datetime


def get_user_info(configuration: dict = None,
                  tw_id: str = None,
                  screen_name: str = None,
                  fields: list = None) -> dict:
    if fields is None:
        fields = ['id_str', 'name', 'screen_name', 'protected', 'created_at', 'verified']

    api = tc.get_client(configuration=configuration,
                        wait_on_rate_limit=True)

    if tw_id is None:
        response = api.get_user(screen_name=screen_name,
                                include_entities=True)
    else:
        response = api.get_user(user_id=tw_id,
                                include_entities=True)
    values = response._json
    result = {i: values[i] for i in fields}
    if 'created_at' in fields:
        result['created_at'] = datetime.strptime(result['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    else:
        pass
    return result


def follower_exists(configuration: dict = None,
                    tw_id: str = None,
                    following_tw_id: str = None) -> bool:
    query_f = f'''
                               select count(0) as count
                               from tweetcore_followers
                               where following_tw_id = '{following_tw_id}'
                               and tw_user_id = coalesce((select id
                                                 from tweetcore_twusers
                                                 where tw_id = '{tw_id}'),-999)'''
    rows_f = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                        query=query_f)["count"].values[0]

    return not rows_f == 0


def get_user_number_followers(configuration: dict = None,
                              tw_id: str = None) -> int:
    user_followers = get_user_info(configuration=configuration,
                                   tw_id=tw_id,
                                   fields=['followers_count'])
    return user_followers["followers_count"]


def get_user_followers(configuration: dict = None,
                       tw_id: str = None,
                       first_time: bool = False,
                       cursor: int = -1) -> list:
    api = tc.get_client(configuration=configuration,
                        wait_on_rate_limit=True)

    temp_followers, next_batch = api.followers_ids(user_id=tw_id,
                                                   cursor=cursor)
    if first_time:
        return temp_followers
    else:
        batch_follower = temp_followers[len(temp_followers) - 1]

        exists = follower_exists(configuration=configuration,
                                 tw_id=batch_follower,
                                 following_tw_id=tw_id)
        if exists:
            return temp_followers
        else:
            print('--- batch ---')
            print('--- latency O.K ---')
            return temp_followers + get_user_followers(configuration=configuration,
                                                       tw_id=tw_id,
                                                       cursor=next_batch[1])
            pass


def reconstruct_follower_history(configuration: dict = None,
                                 tw_id: str = None,
                                 cursor: int = -1,
                                 count: int = 5000):
    api = tc.get_client(configuration=configuration)

    temp_followers, next_batch = api.followers_ids(user_id=tw_id,
                                                   cursor=cursor,
                                                   count=count)
    if next_batch[1] == 0:
        return temp_followers
    else:
        print('--- batch ---')
        print('--- latency O.K ---')
        return temp_followers + reconstruct_follower_history(configuration=configuration,
                                                             tw_id=tw_id,
                                                             cursor=next_batch[1],
                                                             count=count)
