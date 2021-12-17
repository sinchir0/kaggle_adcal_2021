from logging import getLogger

import gokart
import luigi
import numpy as np
from fasttext import load_model

from kaggle_adcal_2021.utils.template import GokartTask

logger = getLogger(__name__)


class AddClassifyLangColTask(GokartTask):

    preprocess_task = gokart.TaskInstanceParameter()

    fasttext_path: str = luigi.Parameter()

    def requires(self):
        return self.preprocess_task

    def classify_lang(self, model, text: str) -> str:
        return model.predict(text)[0][0][-2:]

    def run(self):

        df = self.load_data_frame()

        model = load_model(self.fasttext_path)

        df["lang"] = np.vectorize(self.classify_lang)(model, df["clean_text"])

        df["lang"] = np.where(
            ((df["lang"] != "en") & (df["lang"] != "ja")), "others", df["lang"]
        )

        self.dump(df)
