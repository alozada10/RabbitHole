from tweetcore.tasks import twitter_connect as tc
from datetime import datetime


def get_user_info(configuration: dict = None,
                  tw_id: str = None,
                  screen_name: str = None,
                  fields: list = None) -> dict:
    if fields is None:
        fields = ['id_str', 'name', 'screen_name', 'protected', 'created_at', 'verified']

    api = tc.get_client(configuration=configuration)

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


def get_user_followers(configuration: dict = None,
                       tw_id: str = None,
                       cursor: int = -1):
    api = tc.get_client(configuration=configuration)

    followers_ids = api.followers_ids(user_id=tw_id,
                                      cursor=cursor)

    return followers_ids
