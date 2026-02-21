from rocketstocks.core.config.environment import get_env


class secrets:

    # Discord
    discord_token = get_env('DISCORD_TOKEN')

    # News API
    news_api_token = get_env('NEWS_API_KEY')

    # Schwab
    schwab_api_key = get_env('SCHWAB_API_KEY')
    schwab_api_secret = get_env('SCHWAB_API_SECRET')

    # Postgres
    db_user = get_env('POSTGRES_USER')
    db_password = get_env('POSTGRES_PASSWORD')
    db_name = get_env('POSTGRES_DB')
    db_host = get_env('POSTGRES_HOST')
    db_port = (get_env('POSTGRES_PORT'))
