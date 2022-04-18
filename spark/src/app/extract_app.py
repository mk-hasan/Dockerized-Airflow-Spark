"""
Author: Kamrul Hasan
Date: 17.2022
Email: hasan.alive@gmail.com
"""

import sys
from pyspark.sql import SparkSession
import extract_job as ej


def run() -> None:
    """
    A simple function to create the spark session and triggers the jobs.
    """
    spark_session = (SparkSession
                     .builder
                     .getOrCreate()
                     )

    helpers_utils = ej.HelperUtils()
    print(sys.argv[1])
    config = helpers_utils.config_loader(sys.argv[1])
    ej.ExtractJob(spark_session, ej.HelperUtils, config).run()


# trigger the trasnformations
if __name__ == '__main__':
    run()
