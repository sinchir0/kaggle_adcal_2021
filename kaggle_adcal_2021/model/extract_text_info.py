from logging import getLogger

import gokart
import numpy as np

from kaggle_adcal_2021.utils.template import GokartTask
from kaggle_adcal_2021.utils.text_preprocess import clean_text

logger = getLogger(__name__)


class PreprocessTextTask(GokartTask):

    merge_tweet_task = gokart.TaskInstanceParameter()

    def requires(self):
        return self.merge_tweet_task

    def run(self) -> None:
        df = self.load_data_frame()

        # テキストの前処理
        df["clean_text"] = np.vectorize(clean_text)(df["tweet_text"])

        # @の中にKaggleを含んでいたTweetを削除する
        df = df[df["clean_text"].str.contains("Kaggle", case=False)].reset_index(
            drop=True
        )

        # RTのTweetを省く
        df = df[~df["clean_text"].str.startswith("RT")].reset_index(drop=True)

        # 適当に日付を抽出して、Tweet数が0でないか確認する
        assert (
            (df["created_at"].str.contains("Nov 26").sum() > 0)
            & (df["created_at"].str.contains("Nov 30").sum() > 0)
            & (df["created_at"].str.contains("Dec 03").sum() > 0)
            & (df["created_at"].str.contains("Dec 07").sum() > 0)
            & (df["created_at"].str.contains("Dec 11").sum() > 0)
            & (df["created_at"].str.contains("Dec 15").sum() > 0)
        )

        self.dump(df)
