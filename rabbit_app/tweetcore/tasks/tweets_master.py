import twitter_connect as tc
import tweepy as tp


def get_tweets_from_id(configuration: dict = None,
                       tw_id: str = None,
                       include_rt: bool = False,
                       num_tweets: int = 10) -> dict:
    tweets = {}
    api = tc.get_client(configuration=configuration)
    for i in tp.Cursor(api.user_timeline,
                       id=tw_id,
                       include_rts=include_rt,
                       tweet_mode='extended').items(num_tweets):
        breakpoint()
        hashtag_temp = [j["text"] for j in i.entities["hashtags"]]
        temp = {"hashtag": hashtag_temp,
                "text": i.full_text}
        tweets[str(i.created_at)] = temp

    return tweets


from rabbit_app.tweetcore import credentials_refactor

conf = credentials_refactor.return_credentials()

r = get_tweets_from_id(configuration=conf,
                   tw_id="1393667839584784390",
                   include_rt=True,
                   num_tweets=10)
breakpoint()