from src.job import Job, IncompatibleFormatException
from typing import List, Generator, Union
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from src.logger import logger


class UserRetrieve(Job):
    """
        Loads data from staging area based on range
    """
    def extract(self) -> List[DataFrame]:
        """
        Extract data from source
        """
        self._validate_config()        
        df_config = self._config.dataframe_config[0]
        logger.info(f"Job %s - Reading from %s", str(self.__class__.__name__), df_config.location)
        schema: str = ', '.join(str(property) for property in df_config.schema)
        df: DataFrame = self._spark.read\
            .option("header", "false")\
            .option("mode", "DROPMALFORMED")\
            .option("nullValue", "")\
            .option("dateFormat", "yyyy-MM-dd")\
            .schema(schema)\
            .format(df_config.format)\
            .load(df_config.location)        
        return [df]

    def transform(self, dfs: List[Union[DataFrame, Generator]]) -> Union[List[DataFrame], Generator]:
        df: DataFrame = dfs[0].filter(self._window)   
        df.show()   # Only here for demo purposes. This triggers the DAG. Bad idea.
        return [df]
    
    def load(self, dfs: Union[List[DataFrame], Generator]) -> List[DataFrame]:
        path: str = self._config.save_location
        df: DataFrame = dfs[0]
        logger.info(f"Job %s - Writing ds %s to %s", str(self.__class__.__name__), self._config.name, path)
        df.write.option("mode", "FAILFAST")\
            .option("nullValue", "")\
            .option("dateFormat", "yyyy-MM-dd")\
            .option("path", path)\
            .format('csv')\
            .partitionBy(self._config.partition)\
            .mode("overwrite")\
            .save()
        return [df]
    
    def _validate_config(self):
        if not len(self._config.dataframe_config) == 1:
            raise IncompatibleFormatException("1 df required")
        if not self._config.dataframe_config[0].format == 'parquet':
            raise IncompatibleFormatException("only parquet supported")
        if not self._config.dataframe_config[0].type == 'linear':
            raise IncompatibleFormatException("Only linear supported")
