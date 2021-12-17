import luigi
import numpy as np
import gokart

import kaggle_adcal_2021

if __name__ == '__main__':
    gokart.add_config('./conf/param.ini')
    gokart.run()
