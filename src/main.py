from pathlib import Path
from job import Job
from job.hierarchy_join import HierarchyJoin
from job.user_retrieve import UserRetrieve
from job.validate import Validate
from pyspark import SparkConf
from configuration import AppConfiguration
import yaml
from pyspark.sql import SparkSession
from logger import logger
from configuration import TaskConfig
from typing import List
from scheduler import Scheduler
import time


class UnknownJobException(Exception):
    """
    Exception thrown when repository is not known
    """

def init_app_conf(path: Path) -> AppConfiguration:
    """
    Returns app configuration from yaml file (path).
    """
    logger.info(f"Loading app configuration from: {path}")
    yaml_dict: dict = yaml.safe_load(path.absolute().read_bytes())
    app_configuration: AppConfiguration = AppConfiguration.from_dict(yaml_dict)
    return app_configuration


def init_spark_conf(conf: AppConfiguration) -> SparkConf:
    """
    Retrieve spark conf from application conf
    """
    spark_conf: SparkConf = SparkConf()
    spark_conf.setAll([property.to_tuple() for property in app_configuration.spark_config])
    return spark_conf


def init_job(task_config: TaskConfig, spark: SparkSession, scheduler: Scheduler) -> Job:
    """
    Retrieve only the tasks needed
    """
    if task_config.name == 'validate':
        return Validate(task_config, spark, scheduler)
    if task_config.name == 'hierarchy_join':
        return HierarchyJoin(task_config, spark, scheduler)
    if task_config.name == 'user_retrieve':
        return UserRetrieve(task_config, spark, scheduler)
    raise UnknownJobException(f"Could not find job {task_config.name}")

    
if __name__ == '__main__':
    app_configuration: AppConfiguration = init_app_conf(path=Path('./config.yaml'))
    spark_conf: SparkConf = init_spark_conf(conf=app_configuration)
    spark: SparkSession = SparkSession\
        .builder\
        .config(conf=spark_conf)\
        .master('local[1]')\
        .getOrCreate()
    scheduler: Scheduler = Scheduler()    
    tasks: List[Job] = [init_job(task_config, spark, scheduler) for task_config in app_configuration.task_config]
    

    for task in tasks:
        task.schedule()
    
    while True:
        logger.info(scheduler)
        scheduler.exec_jobs()
        time.sleep(360)
