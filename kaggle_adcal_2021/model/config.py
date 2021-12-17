import luigi


class MasterConfig(luigi.Config):
    data_path: str = luigi.Parameter()
    figure_path: str = luigi.Parameter()
    data_path_2016: str = luigi.Parameter()
    data_path_2021: str = luigi.Parameter()
