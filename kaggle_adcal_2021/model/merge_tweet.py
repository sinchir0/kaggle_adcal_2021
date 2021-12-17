import glob
from logging import getLogger

import luigi
import pandas as pd

from kaggle_adcal_2021.utils.template import GokartTask

logger = getLogger(__name__)


class MergeTweetTask(GokartTask):

    get_file_path: str = luigi.Parameter()

    def run(self) -> None:
        file_path_list = sorted(glob.glob(self.get_file_path))
        dfs = [pd.read_csv(file_path) for file_path in file_path_list]

        df = (
            pd.concat(dfs)
            .drop_duplicates(subset=["created_at", "tweet_text", "tweet_user"])
            .reset_index(drop=True)
        )

        assert sum([dfs[idx].shape[0] for idx in range(len(dfs))]) >= df.shape[0]

        self.dump(df)
