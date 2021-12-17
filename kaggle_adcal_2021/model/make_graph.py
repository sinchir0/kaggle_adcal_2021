import datetime
import os
from logging import getLogger

import luigi
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from gokart.config_params import inherits_config_params

from kaggle_adcal_2021.model.classify_lang import AddClassifyLangColTask
from kaggle_adcal_2021.model.config import MasterConfig
from kaggle_adcal_2021.model.extract_text_info import PreprocessTextTask
from kaggle_adcal_2021.model.merge_tweet import MergeTweetTask
from kaggle_adcal_2021.utils.template import GokartTask

logger = getLogger(__name__)


@inherits_config_params(MasterConfig)
class MakeRankingGraphTask(GokartTask):

    figure_path: str = luigi.Parameter()

    data_path_2016: str = luigi.Parameter()
    data_path_2021: str = luigi.Parameter()

    def run(self):
        df_2016 = pd.read_csv(self.data_path_2016)
        df_2021 = pd.read_csv(self.data_path_2021)

        df_2016 = df_2016.set_index("Country")
        df_2016["Rank_16"].sort_values()[-10:].plot(kind="barh")
        plt.title("2016_top100_Kaggler_Country")
        plt.savefig(
            f"{self.figure_path}/2016_top100_Kaggler_Country.png", bbox_inches="tight"
        )
        plt.close()

        df_2021["Country_211214"].value_counts().sort_values()[-10:].plot(kind="barh")
        plt.title("202112_top100_Kaggler_Country")
        plt.savefig(
            f"{self.figure_path}/202112_top100_Kaggler_Country.png", bbox_inches="tight"
        )
        plt.close()

        self.dump("make ranking graph task")


@inherits_config_params(MasterConfig)
class MakeRankingHistTask(GokartTask):

    figure_path: str = luigi.Parameter()
    data_path_2021: str = luigi.Parameter()

    def run(self):
        df_2021 = pd.read_csv(self.data_path_2021)

        df_2021_US = df_2021[df_2021["Country_211214"] == "United States"]
        df_2021_JP = df_2021[df_2021["Country_211214"] == "Japan"]

        plt.hist(df_2021_US["Rank"], label="US", alpha=0.5)
        plt.hist(df_2021_JP["Rank"], label="JP", alpha=0.5)
        plt.title("202112_Top100_User_US_JP_Rank_Hist")
        plt.xlabel("Rank")
        plt.ylabel("Count")
        plt.legend()
        plt.savefig(
            f"{self.figure_path}/202112_Top100_User_US_JP_Rank_Hist.png",
            bbox_inches="tight",
        )
        plt.close()

        self.dump("make ranking hist task")


@inherits_config_params(MasterConfig)
class MakeLangGraphTask(GokartTask):

    figure_path: str = luigi.Parameter()

    def requires(self):
        merge_tweet_task = MergeTweetTask()
        preprocess_task = PreprocessTextTask(merge_tweet_task=merge_tweet_task)
        data_task = AddClassifyLangColTask(preprocess_task=preprocess_task)
        return data_task

    def run(self):
        today = datetime.date.today()

        df = self.load_data_frame(required_columns={"lang"}, drop_columns=True)

        sns.countplot(x="lang", data=df)
        os.makedirs(self.figure_path, exist_ok=True)
        plt.savefig(f"{self.figure_path}/{today}_lang.png", bbox_inches="tight")

        self.dump("make lang graph")


@inherits_config_params(MasterConfig)
class MakeLangGraphPerMAUTask(GokartTask):

    figure_path: str = luigi.Parameter()

    def requires(self):
        merge_tweet_task = MergeTweetTask()
        preprocess_task = PreprocessTextTask(merge_tweet_task=merge_tweet_task)
        data_task = AddClassifyLangColTask(preprocess_task=preprocess_task)
        return data_task

    def run(self):
        today = datetime.date.today()

        df = self.load_data_frame(required_columns={"lang"}, drop_columns=True)

        country_rank = (
            df["lang"].value_counts()[["en", "ja"]].astype("float").reset_index()
        )

        # column名を変更
        country_rank = country_rank.rename({"index": "lang", "lang": "count"}, axis=1)

        # MAUで割る
        country_rank.loc[0, "rate"] = country_rank.loc[0, "count"] / (77.75 * 1000000)
        country_rank.loc[1, "rate"] = country_rank.loc[1, "count"] / (58.20 * 1000000)

        ja_num = country_rank.loc[1, "rate"]
        logger.info(f"日本語のTwitterユーザーは1月当たり平均{ja_num:.6f}回Kaggleが含まれるTweetをする")

        import ipdb

        ipdb.set_trace()

        sns.barplot(x="lang", y="rate", data=country_rank, order=["ja", "en"])
        os.makedirs(self.figure_path, exist_ok=True)
        plt.savefig(f"{self.figure_path}/{today}_MAU.png", bbox_inches="tight")

        self.dump("make lang graph")
