from src.job import Job, IncompatibleFormatException
from typing import List, Generator, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.logger import logger


class HierarchyJoin(Job):
    """
        Using spark.sql.sources.partitionOverwriteMode=dynamic: only update changed partitions
        Partitioning by hour: lowest cardinality required by user
    """
    def extract(self) -> List[DataFrame]:
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

    def transform(self, dfs: List[DataFrame]) -> Union[List[DataFrame], Generator]:
        left: DataFrame = dfs[0].filter(self._window)
        right: Generator = HierarchyJoin._travtree(dfs[1])
        left.show()   # Only here for demo purposes. This triggers the DAG. Bad idea.
        for depth, df in enumerate(right):
            logger.info("Job %s - joining at hierarchy depth %i", self.__class__.__name__, depth)
            yield left.join(df, ["PK"], "inner").drop("FK")
    
    def load(self, dfs: Union[List[DataFrame], Generator]) -> List[DataFrame]:
        results: List[DataFrame] = []
        path: str = self._config.save_location
        for df in dfs:  # generator trigger
            logger.info(f"Job %s - Writing ds %s to %s", str(self.__class__.__name__), self._config.name, path)
            df.show()   # Only here for demo purposes. This triggers the DAG. Bad idea.
            df.write.option("mode", "FAILFAST")\
                .option("nullValue", "")\
                .option("dateFormat", "yyyy-MM-dd")\
                .option("path", path)\
                .format('parquet')\
                .mode("overwrite")\
                .partitionBy(self._config.partition)\
                .save()
            results.append(df)
        return results
    
    @staticmethod
    def _travtree(df: DataFrame) -> Generator[DataFrame, None, None]:
        """
        BFS through tree. Yield to transform per layer.
        """
        root: DataFrame = df.where(col('FK').isNull())
        yield root
        prev = root

        while True:
            pk: DataFrame = prev.select('PK')
            level: DataFrame = df.join(pk, col('FK') == pk['PK'], "left_semi")

            if level.isEmpty():
                break
            
            yield level
            prev = level
            
    def _validate_config(self):
        if not len(self._config.dataframe_config) == 2:
            raise IncompatibleFormatException("2 dfs required")
        if not self._config.dataframe_config[0].type == 'linear':
            raise IncompatibleFormatException("First df must be linear")
        if not self._config.dataframe_config[1].type == 'hierarchy':
            raise IncompatibleFormatException("Second df must be hierarchy")
        if not all(df_config.format == 'parquet' for df_config in self._config.dataframe_config):
            raise IncompatibleFormatException("only parquet supported")
