import pyspark.sql.dataframe
from pyspark.sql.functions import regexp_extract, countDistinct
import pandas as pd
import json
import os.path
import seaborn as sns
import jinja2


class InsightJob:
    """
    A Class to to run all the transformation jobs
    """

    def __init__(self, spark_session, helper_utils, config) -> None:
        """
        The default constructor
        :param spark_session: Spark Session instance
        :param helper_utils: Utils Class instance
        :param config: Config json
        """
        self.config = config
        self.report_path = config['report_path']
        self.spark_session = spark_session
        self.helper_utils = helper_utils
        self.logger = InsightJob.__logger(spark_session)
        self.df = None
        self.df_country = None
        self.df_email = None
        self.df_user_count = None

    def run(self) -> None:
        """
        Main transform class to run all jobs and save the data into absolute sink location
        """
        self.logger.info('Running actions Job')

        def to_retrieve_data() -> None:
            self.df = self.spark_session.read \
                .format("jdbc") \
                .option("url", self.config['postgres_db'])\
                .option("dbtable", self.config['postgres_table'])\
                .option("user", self.config['postgres_user'])\
                .option("password", self.config["postgres_pwd"])\
                .load()

        def get_total_user() -> None:
            df_user_count = self.df.select(countDistinct("id"))
            pdf_user_count = df_user_count.toPandas()
            self.df_user_count = pdf_user_count['count(DISTINCT id)'][0]

        def get_country_stat() -> None:
            df_country = self.df.groupBy("country").count().withColumnRenamed("count", "freq").orderBy('freq',
                                                                                                       ascending=False)
            pdf_country = df_country.toPandas()
            self.df_country = pd.concat([pdf_country.head(1), pdf_country.tail(1)])

        def get_email_stat() -> None:
            df_email = self.df.withColumn("email_domain", regexp_extract("email", "(?<=@)[^.]+(?=\\.)", 0)).groupby(
                'email_domain').count().sort(
                'count', ascending=False)
            pdf_email = df_email.toPandas()
            self.df_email = pd.concat([pdf_email.head(1), pdf_email.tail(1)])

        to_retrieve_data()
        get_total_user()
        get_country_stat()
        get_email_stat()

        # generate a html report
        self.helper_utils.get_report(self.df_country, self.df_email, self.df_user_count, self.report_path)

        self.logger.info('End running actions')

    @staticmethod
    def __logger(spark_session):
        """
        Logger method to get the logging
        :param spark_session: Spark Session
        :return: Logmanager instance
        """
        log4j_logger = spark_session.sparkContext._jvm.org.apache.log4j  # pylint: disable=W0212
        return log4j_logger.LogManager.getLogger(__name__)


class HelperUtils:
    """
    A helper utils class to support the necessary action trigger by pyspark and generate the report
    """

    @staticmethod
    def config_loader(file_path) -> json:
        """
        A function to load config file
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
        except IOError:
            print("Error: File does not appear to exist.")
            return 0
        return config

    @staticmethod
    def get_report(df_country : pd.DataFrame, df_email : pd.DataFrame, user_count : int, report_path : str) -> None:
        """
        A report function to generate report using jinja and html
        """
        styler = df_country.style.applymap(color_negative_red)
        # Template handling
        styler2 = df_email.style.applymap(color_negative_red)

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=report_path))
        template = env.get_template('template.html')
        get_email_plot(df_email, report_path)
        get_country_plot(df_country, report_path)
        html = template.render(my_table=styler.render(), my_table1=styler2.render(), user_count=user_count)
        with open(os.path.join(report_path + 'report.html'), 'w') as f:
            f.write(html)


def get_email_plot(df_email : pd.DataFrame, report_path : str) -> None:
    ax = sns.barplot(x='email_domain', y='count', data=df_email)
    fig = ax.get_figure()
    fig.savefig(os.path.join(report_path + 'plot_email.svg'))


def get_country_plot(df_country : pd.DataFrame, report_path : str) -> None:
    ax = sns.barplot(x='country', y='freq', data=df_country)
    fig = ax.get_figure()
    fig.savefig(os.path.join(report_path + 'plot_country.svg'))


def color_negative_red(val) -> None:
    color = 'red'
    return f'color: {color}'
