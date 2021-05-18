from tweetcore.lib.postgres_target import execute_query
import pandas as pd


def pandas_df_from_postgre_query(configuration: dict = None,
                                 query: str = None) -> pd.DataFrame:
    result, cursor = execute_query.execute_postgre_query(configuration,
                                                         query,
                                                         with_cursor=True)
    if len(result) > 0:
        headers = list(map(lambda t: t[0], cursor.description))
        df = pd.DataFrame(result)
        df.columns = headers
    else:
        df = None

    return df
