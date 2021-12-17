from logging import getLogger

from kaggle_adcal_2021.utils.template import GokartTask

logger = getLogger(__name__)


class Sample(GokartTask):
    def run(self) -> None:
        self.dump("sample output")
