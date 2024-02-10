from src.configuration import AppConfiguration
from src.main import init_job, init_spark_conf
from scheduler import Scheduler
from pyspark.sql import SparkSession
    
    
def test_init_job(app_configuration: AppConfiguration, spark_session: SparkSession) -> None:
    scheduler: Scheduler = Scheduler()
    assert init_job(app_configuration.task_config[0], spark_session, scheduler)
