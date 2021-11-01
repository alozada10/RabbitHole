import twitter_connect as tc
import tweepy as tp
import pandas as pd
import numpy as np


def get_tweets_from_id(configuration: dict = None,
                       tw_id: str = None,
                       include_rt: bool = False,
                       num_tweets: int = 10,
                       stage: str = None) -> dict:
    user_tweets = {"tw_id": tw_id}
    tweets = {}
    api = tc.get_client(configuration=configuration)
    counter = 1
    for i in tp.Cursor(api.user_timeline,
                       id=tw_id,
                       include_rts=include_rt,
                       tweet_mode='extended').items(num_tweets):
        temp = {"tweet_id": i.id_str,
                "created_at": i.created_at}
        if stage == 'detect_macro':
            pass
        elif stage == 'detect_micro':
            temp["lang"] = i.lang
            user_tweets["number_followers"] = i.user.followers_count
            user_tweets["number_following"] = i.user.friends_count
            temp["tweet"] = i.full_text
        elif stage == 'analysis':
            temp["lang"] = i.lang
            user_tweets["number_followers"] = i.user.followers_count
            user_tweets["number_following"] = i.user.friends_count
            temp["tweet"] = i.full_text
            temp["hashtags"] = [j["text"] for j in i.entities["hashtags"]]
            temp["in_reply_to_tw_id"] = i.in_reply_to_user_id_str
            temp["in_reply_to_tweet_id"] = i.in_reply_to_status_id_str
            temp["mentions"] = [j["id_str"] for j in i.entities["user_mentions"]]
            temp["rt_tw_id"] = None
            temp["rt_tweet_id"] = None
            temp["rt_quoted_tw_id"] = None
            temp["rt_quoted_tweet_id"] = None
            if i.is_quote_status:
                temp["rt_quoted_tw_id"] = i.quoted_status.user.id_str
                temp["rt_quoted_tweet_id"] = i.quoted_status.id_str
            try:
                if i.retweeted_status is not None:
                    temp["rt_tw_id"] = i.retweeted_status.user.id_str
                    temp["rt_tweet_id"] = i.retweeted_status.id_str
                    temp["tweet"] = None
            except AttributeError:
                pass
        else:
            print('Must select stage: detect_macro, detect_micro or analysis')
            raise
        tweets[counter] = temp
        counter += 1
    user_tweets["tweets"] = tweets
    breakpoint()
    return user_tweets


def measure_activity(user_tweets: dict = None,
                     stage: str = None):
    report = {}
    data_tweets = pd.DataFrame(user_tweets["tweets"]).T
    freq_seconds = data_tweets["created_at"].diff(periods=-1)
    freq_seconds = freq_seconds.apply(lambda x: x/np.timedelta64(1, 's')).fillna(0).astype('int64').values[:-1]



from rabbit_app.tweetcore import credentials_refactor

conf = credentials_refactor.return_credentials()

r = get_tweets_from_id(configuration=conf,
                       tw_id="1393667839584784390",
                       include_rt=True,
                       num_tweets=6,
                       stage='analysis')
