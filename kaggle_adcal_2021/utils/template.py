from logging import getLogger

import gokart

logger = getLogger(__name__)


class GokartTask(gokart.TaskOnKart):
    task_namespace = "kaggle_adcal_2021"
