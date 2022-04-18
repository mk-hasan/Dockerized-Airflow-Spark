"""
Author: Kamrul Hasan
Date: 17.04.2022
Email: hasan.alive@gmail.com
"""
"""
A simple pyspark app to do some actions 
"""

import sys
from pyspark.sql import SparkSession
import insight_job as ij


def run() -> None:
    """
    A simple function to create the spark session and triggers the jobs.
    """
    spark_session = SparkSession \
        .builder \
        .appName('oetker-insight-app') \
        .getOrCreate()

    helpers_utils = ij.HelperUtils()
    config = helpers_utils.config_loader(sys.argv[1])
    ij.InsightJob(spark_session, ij.HelperUtils, config).run()


# trigger the insight jobs

if __name__ == '__main__':
    run()
