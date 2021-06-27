from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from tweetcore.lib.postgres_target import connect
from tweetcore.lib.postgres_target import execute_query
from tweetcore.lib.postgres_target import download_data
import global_settings as gs


def write_postgre_table_back(configuration: dict = None,
                             data: pd.DataFrame = None,
                             table_name: str = None,
                             schema: str = None,
                             if_exists_then_wat: str = 'replace'):
    schema = schema.lower()
    table_name = table_name.lower()
    engine = create_engine(connect.url(configuration=configuration))
    connection = engine.connect()
    query_exists = gs.QUERY_EXISTS
    query_exists = query_exists.replace('{schema}', schema).replace('{table_name}', table_name)

    exists, cur = execute_query.execute_postgre_query(configuration=configuration,
                                                      query=query_exists,
                                                      with_cursor=True)

    if if_exists_then_wat == 'replace':

        if exists[0][0]:
            print('--- La tabla existe, será reemplazada ---')
            try:
                execute_query.execute_postgre_query(configuration=configuration,
                                                    query=f"DROP TABLE {schema}.{table_name}")
            except Exception as error:
                print(10 * '------------')
                print(error.__class__.__name__)
                print(error)
                print(10 * '------------')
            pass

        else:
            print('--- La tabla no existe, será creada por primera vez ---')
            pass
        try:
            data.to_sql(table_name,
                        engine,
                        schema=schema,
                        index=False,
                        chunksize=3000)
        except Exception as error:
            print(10 * '------------')
            print(error.__class__.__name__)
            print(error)
            print(10 * '------------')

        connection.execute(f"grant select on table {schema}.{table_name} to public;")
        connection.close()
        engine.dispose()

    elif if_exists_then_wat == 'append':

        if exists[0][0]:
            print('--- La tabla existe, serán insertados nuevos valores ---')
            try:

                data.to_sql(table_name + "_temp",
                            engine,
                            schema=schema,
                            index=False,
                            chunksize=3000)
                print('--- La tabla temporal se creó ---')

            except Exception as error:
                print(10 * '------------')
                print(error.__class__.__name__)
                print(error)
                print(10 * '------------')

            temp = download_data.pandas_df_from_postgre_query(configuration=configuration,
                                                              query=f"select * from {schema}.{table_name} limit 1")
            columns = str(tuple(temp.columns.to_list())).replace("'", "")

            query_insert = f'''
                INSERT INTO {schema}.{table_name} {columns}
                SELECT {columns.replace("(", "").replace(")", "")}
                FROM {schema}.{table_name}_temp;
            '''

            try:
                execute_query.execute_postgre_query(configuration=configuration,
                                                    query=query_insert)
            except Exception as error:
                print(10 * '------------')
                print(error.__class__.__name__)
                print(error)
                print(10 * '------------')

            try:
                execute_query.execute_postgre_query(configuration=configuration,
                                                    query=f"drop table {schema}.{table_name}_temp")
                print('--- La tabla temporal se borró ---')
            except Exception as error:
                print(10 * '------------')
                print(error.__class__.__name__)
                print(error)
                print(10 * '------------')
        else:
            print('--- La tabla no existe, será creada por primera vez ---')
            try:
                data.to_sql(table_name,
                            engine,
                            if_exists=if_exists_then_wat,
                            schema=schema,
                            index=False,
                            chunksize=3000)
            except Exception as error:
                print(10 * '------------')
                print(error.__class__.__name__)
                print(error)
                print(10 * '------------')

            connection.execute(f"grant select on table {schema}.{table_name} to public;")
            connection.close()
            engine.dispose()
    else:
        print("error, must specify: 'append' or 'replace'")


def write_postgre_table(configuration: dict = None,
                        data: pd.DataFrame = None,
                        table_name: str = None,
                        schema: str = None,
                        if_exists_then_wat: str = 'replace'):

    splits = round(data.shape[0] / 2000)
    if splits > 1:
        j = 0
        for d_i in np.array_split(data, splits):
            if (j == 0) & (if_exists_then_wat == 'replace'):
                write_postgre_table_back(configuration=configuration,
                                         data=d_i,
                                         table_name=table_name,
                                         schema=schema,
                                         if_exists_then_wat='replace')
            else:
                write_postgre_table_back(configuration=configuration,
                                         data=d_i,
                                         table_name=table_name,
                                         schema=schema,
                                         if_exists_then_wat='append')
            j += 1
            print(f'--- {j}/{splits} ---')
    else:
        write_postgre_table_back(configuration=configuration,
                                 data=data,
                                 table_name=table_name,
                                 schema=schema,
                                 if_exists_then_wat=if_exists_then_wat)
