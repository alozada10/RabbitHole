def return_credentials() -> dict:
    from rabbit_app import credentials
    config = {
        "user": credentials.POSTGRES_USER,
        "password": credentials.POSTGRES_PASSWORD,
        "host": credentials.POSTGRES_HOST,
        "port": credentials.POSTGRES_PORT,
        "database": credentials.POSTGRES_DATABASE,
        "consumer_key": credentials.api_key,
        "consumer_secret": credentials.api_key_secret,
        "access_token": credentials.access_token,
        "access_secret": credentials.access_secret
    }
    return config
