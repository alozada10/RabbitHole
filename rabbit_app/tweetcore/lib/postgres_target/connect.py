import psycopg2


def url(configuration: dict = None) -> str:
    user = configuration["user"]
    password = configuration["password"]
    host = configuration["host"]
    port = configuration["port"]
    database = configuration["database"]
    url_link = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

    return url_link


def connect(configuration: dict = None) -> psycopg2.extensions.connection:
    fields = ["user", "password", "host", "port", "database"]
    conf = {i: configuration[i] for i in fields}
    conn = psycopg2.connect(**conf)

    return conn
