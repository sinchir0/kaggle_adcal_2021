from pydantic import BaseSettings, Field


class MyInfo(BaseSettings):
    consumer_key: str = Field(..., env="TWITTER_API_KEY")
    consumer_secret: str = Field(..., env="TWITTER_API_SECRET_KEY")
    access_token: str = Field(..., env="TWITTER_ACCESS_TOKEN")
    access_token_secret: str = Field(..., env="TWITTER_ACCESS_TOKEN_SECRET")
