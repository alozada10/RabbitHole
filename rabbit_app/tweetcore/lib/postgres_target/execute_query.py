from tweetcore.lib.postgres_target import connect


def execute_postgre_query(configuration: dict = None,
                          query: str = None,
                          with_cursor: bool = False):
    con = connect.connect(configuration=configuration)
    cursor = con.cursor()
    try:
        if with_cursor:
            cursor.execute(query)
            res = cursor.fetchall()
            cursor.close()
            return res, cursor
        else:
            cursor.execute(query)
            cursor.close()
            con.commit()
            return True, True
    finally:
        con.close()
