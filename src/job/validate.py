from src.job import Job, IncompatibleFormatException
from typing import List, Generator, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc
from src.logger import logger
from src.configuration import Property, DataFrameConfig
from datetime import datetime


class Validate(Job):
    """
        Cleans and partitions raw input data
    """
    def extract(self) -> List[DataFrame]:
        """
        Extract data from source
        """
        self._validate_config()
        results: List[DataFrame] = []   
        for df_config in self._config.dataframe_config:
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
            results.append(df)
        return results

    def transform(self, dfs: List[Union[DataFrame, Generator]]) -> List[Union[DataFrame, Generator]]:
        results: List[DataFrame] = []
        for df, df_config in zip(dfs, self._config.dataframe_config):
            df = Validate._validate_csv_nullable(df, df_config.schema)
            df = Validate._validate_no_future_dates(df, df_config.schema)
            df = self._partition_filter(df, df_config)
            df.show()   # Only here for demo purposes. This triggers the DAG. Bad idea.
            results.append(df)
        return results
    
    def load(self, dfs: Union[List[DataFrame], Generator]) -> DataFrame:
        for df, df_config in zip(dfs, self._config.dataframe_config):
            path: str = self._config.save_location + df_config.name
            logger.info(f"Job %s - Writing ds %s to %s", str(self.__class__.__name__), self._config.name, path)
            writer = df.write
            if self._config.partition in df.columns:
                writer = df.write.partitionBy(self._config.partition)
            writer.option("mode", "FAILFAST")\
                .option("nullValue", "")\
                .option("dateFormat", "yyyy-MM-dd")\
                .option("path", path)\
                .format('parquet')\
                .mode("overwrite")\
                .save()      
        return dfs
    
    @staticmethod
    def _validate_csv_nullable(df: DataFrame, schema: List[Property]) -> DataFrame:
        """
        CSV schemas do not support nullable=false. We enforce this separately to catch all edge-cases.
        https://stackoverflow.com/questions/61180738/spark-csv-nullable-false-is-not-throwing-exception
        """
        for prop in schema:
            if "NOT NULL" in prop.value.upper():
                df: DataFrame = df.filter(col(prop.key).isNotNull())
        return df
    
    @staticmethod
    def _validate_no_future_dates(df: DataFrame, schema: List[Property]):
        for prop in schema:
            if "TIMESTAMP" in prop.value:
                df = df.filter(col(prop.key) <= datetime.now().strftime("%Y-%m-%d"))
        return df
    
    def _partition_filter(self, df: DataFrame, df_config: DataFrameConfig):
        if "EVENT_TIME" in df.columns:
            df = df\
                .withColumn(self._config.partition, date_trunc("Hour", "EVENT_TIME"))\
                .filter(self._window)
        return df

    def _validate_config(self):
        if not self._config.dataframe_config[0].format == 'csv':
            raise IncompatibleFormatException("only csv supported")
    
 