from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col
from src.configuration import TaskConfig
from typing import List, Union, Generator, Optional
import datetime as dt
from scheduler import Scheduler
from src.logger import logger


class IncompatibleFormatException(Exception):
    """
    Throws when repository is initialized with wrong format
    """
    

class Job(ABC):
    """
    Interface for a pipeline
    """
    _config: TaskConfig
    _spark: SparkSession
    _scheduler: Scheduler
    _window: Column

    def __init__(self, config: TaskConfig, spark: SparkSession, scheduler: Scheduler) -> None:
        self._config = config
        self._spark = spark
        self._scheduler = scheduler
        self._window = col(self._config.partition).between(self._config.from_date, self._config.to_date)
    
    def schedule(self) -> Optional[DataFrame]:
        if self._config.scheduler:
            if not self._config.frequency == 'hourly':
                raise
            self._scheduler.hourly(dt.time(minute=0, second=0), self._execute)
            self._execute()
            logger.info("Job %s - Scheduled hourly", self.__class__.__name__)
        else:
            return self._execute()
        
    def _execute(self) -> DataFrame:
        dfs: List[DataFrame] = self.extract()
        transformed: List[DataFrame] = self.transform(dfs)
        return self.load(transformed)
    
    @abstractmethod
    def extract(self) -> List[Union[DataFrame, Generator]]:
        "Extract the data from some source / origin"
    
    @abstractmethod
    def transform(self, dfs: List[Union[DataFrame, Generator]]) -> Union[List[DataFrame], Generator]:
        "Transform the data from one shape into another"
    
    @abstractmethod
    def load(self, dfs: List[DataFrame]) -> DataFrame:
        "Send data to some sink / destination"
