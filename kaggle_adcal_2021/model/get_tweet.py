from logging import getLogger

import luigi
import pandas as pd
from gokart.config_params import inherits_config_params

from kaggle_adcal_2021.model.config import MasterConfig
from kaggle_adcal_2021.utils.template import GokartTask
from kaggle_adcal_2021.utils.tweet_getter import TweetsGetter

logger = getLogger(__name__)


@inherits_config_params(MasterConfig)
class GetTweetTask(GokartTask):

    data_path: str = luigi.Parameter()
    collect_num: int = luigi.IntParameter()

    def run(self) -> None:

        # キーワードで取得
        getter = TweetsGetter.bySearch("Kaggle")

        all_info = []
        for tweet in getter.collect(total=self.collect_num):

            all_info.append((tweet["created_at"], tweet["text"], tweet["user"]))

        # dfに変換し、CSV出力
        df_tweet = pd.DataFrame(
            all_info, columns=["created_at", "tweet_text", "tweet_user"]
        )

        last_time = df_tweet.iloc[0]["created_at"]
        fisrt_time = df_tweet.iloc[-1]["created_at"]

        df_tweet.to_csv(
            f"{self.data_path}/Kaggle_{fisrt_time}_{last_time}.csv", index=False
        )

        self.dump("get tweet task")
