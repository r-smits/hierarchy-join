from pytest import fixture
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from src.configuration import Property, AppConfiguration
from typing import List
from datetime import datetime as dt
from src.job.hierarchy_join import HierarchyJoin
from src.job.user_retrieve import UserRetrieve
from src.job.validate import Validate
from scheduler import Scheduler


@fixture
def config_dict() -> dict:
    config: dict = {
        'spark_config': [
            {
                'key': 'spark.app.name', 
                'value': 'pipeline'
            }, 
            {
                'key': 'spark.sql.sources.partitionOverwriteMode', 
                'value': 'dynamic'
            }
        ], 
        'task_config': [
            {
                'name': 'validate', 
                'from_date': '2010-01-01', 
                'to_date': '2024-02-10', 
                'scheduler': True, 
                'frequency': 'hourly', 
                'partition': 'PARTITION_TIME', 
                'target_format': 'parquet', 
                'dataframe_config': [
                    {
                        'name': 'linear', 
                        'format': 'csv', 
                        'type': 'linear', 
                        'location': 'data/0_db/linear', 
                        'schema': [
                            {
                                'key': 'PK', 
                                'value': 'INTEGER NOT NULL'
                            }, 
                            {
                                'key': 'NOTIONAL', 
                                 'value': 'FLOAT'
                            }, 
                            {
                                'key': 'EVENT_TIME', 
                                'value': 'TIMESTAMP NOT NULL'
                            }
                        ]
                    },
                    {
                        'name': 'hierarchy', 
                        'format': 'csv', 
                        'type': 'hierarchy', 
                        'location': 'data/0_db/hierarchy', 
                        'schema': [
                            {
                                'key': 'PK', 
                                'value': 'INTEGER NOT NULL'
                            }, 
                            {
                                'key': 'FK', 
                                'value': 'INTEGER'
                            },
                            {
                                'key': 'LOCATION', 
                                'value': 'STRING'
                            }
                        ]
                    }
                ], 
                'save_location': 'data/1_validated/'
            }, 
            {
                'name': 'hierarchy_join', 
                'from_date': '2010-01-01', 
                'to_date': '2024-02-10', 
                'scheduler': True, 
                'frequency': 'hourly', 
                'partition': 'PARTITION_TIME', 
                'target_format': 'parquet', 
                'dataframe_config': [
                    {
                        'name': 'ds1', 
                        'format': 'parquet', 
                        'type': 'linear', 
                        'location': 'data/1_validated/linear', 
                        'schema': [
                            {
                                'key': 'PK', 
                                'value': 'INTEGER NOT NULL'
                            }, 
                            {
                                'key': 'NOTIONAL', 
                                'value': 'FLOAT'
                            }, 
                            {
                                'key': 'EVENT_TIME', 
                                'value': 'TIMESTAMP NOT NULL'
                            }, 
                            {
                                'key': 'PARTITION_TIME', 
                                'value': 'TIMESTAMP NOT NULL'
                            }
                        ]
                    }, 
                    {
                        'name': 'ds2', 
                        'format': 'parquet', 
                        'type': 'hierarchy', 
                        'location': 'data/1_validated/hierarchy', 
                        'schema': [
                            {
                                'key': 'PK', 
                                'value': 'INTEGER NOT NULL'
                            }, 
                            {
                                'key': 'FK', 
                                'value': 'INTEGER'
                            }, 
                            {
                                'key': 'LOCATION', 
                                'value': 'STRING'
                            }
                        ]
                    }
                ], 
                'save_location': 'data/2_staging/dataset'
            }, 
            {
                'name': 'user_retrieve', 
                'from_date': '2010-01-07', 
                'to_date': '2010-01-08', 
                'scheduler': False, 
                'frequency': '', 
                'partition': 'PARTITION_TIME', 
                'target_format': 'parquet', 
                'dataframe_config': [
                    {
                        'name': 'ds1', 
                        'format': 'parquet', 
                        'type': 'linear', 
                        'location': 'data/2_staging/dataset', 
                        'schema': [
                            {
                                'key': 'PK', 
                                'value': 'INTEGER NOT NULL'
                            }, 
                            {
                                'key': 'NOTIONAL', 
                                'value': 'FLOAT'
                            }, 
                            {
                                'key': 'EVENT_TIME', 
                                'value': 'TIMESTAMP NOT NULL'
                            }, 
                            {
                                'key': 'PARTITION_TIME', 
                                'value': 'TIMESTAMP NOT NULL'
                            }, 
                            {
                                'key': 'LOCATION', 
                                'value': 'STRING'
                            }
                        ]
                    }
                ], 
                'save_location': 'data/3_user/0/'
            }
        ]
    }
    return config


@fixture
def app_configuration(config_dict: dict) -> AppConfiguration:
    return AppConfiguration.from_dict(config_dict)

@fixture
def spark_config_properties() -> List[Property]:
    properties_dict: List[dict] = [
        {"key": "spark.app.name", "value": "pipeline"},
        {"key": "spark.sql.sources.partitionOverwriteMode", "value": "dynamic"}
    ]
    return [Property.from_dict(kv_pair) for kv_pair in properties_dict]


@fixture
def spark_session(spark_config_properties: List[Property]) -> SparkSession:
    spark_conf: SparkConf = SparkConf()
    spark_conf = spark_conf.setAll([property.to_tuple() for property in spark_config_properties])
    spark: SparkSession = SparkSession\
            .builder\
            .config(conf=spark_conf)\
            .master('local[1]')\
            .getOrCreate()
    return spark


@fixture
def csv_linear_schema() -> str:
     schema_dict: List[dict] = [
           {"key": "PK", "value": "INTEGER NOT NULL"},
           {"key": "NOTIONAL", "value": "FLOAT"},
           {"key": "EVENT_TIME", "value": "TIMESTAMP NOT NULL"},
           {"key": "PARTITION_TIME", "value": "TIMESTAMP NOT NULL"}
     ]
     schema_properties: List[Property] = [Property.from_dict(kv_pair) for kv_pair in schema_dict]
     return ', '.join(str(property) for property in schema_properties)


@fixture
def csv_hierarchy_schema() -> str:
     schema_dict: List[dict] = [
         {'key': 'PK', 'value': 'INTEGER NOT NULL'}, 
         {'key': 'FK', 'value': 'INTEGER'},
         {'key': 'LOCATION', 'value': 'STRING'}
     ]
     schema_properties: List[Property] = [Property.from_dict(kv_pair) for kv_pair in schema_dict]
     return ', '.join(str(property) for property in schema_properties)


@fixture
def csv_joined_schema() -> str:
     schema_dict: List[dict] = [
         {'key': 'PK',  'value': 'INTEGER NOT NULL'}, 
         {'key': 'NOTIONAL', 'value': 'FLOAT'}, 
         {'key': 'EVENT_TIME', 'value': 'TIMESTAMP NOT NULL'}, 
         {"key": "PARTITION_TIME", "value": "TIMESTAMP NOT NULL"},
         {'key': 'LOCATION', 'value': 'STRING'}
     ]
     schema_properties: List[Property] = [Property.from_dict(kv_pair) for kv_pair in schema_dict]
     return ', '.join(str(property) for property in schema_properties)


@fixture
def csv_linear_data() -> List[List[str]]:
    data: List[List[str]] = [
        [1,10.0,dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [2,20.0,dt.strptime("2010-01-07T05:50:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [3,50.0,dt.strptime("2010-03-10T06:40:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [4,900.0,dt.strptime("2024-01-01T04:40:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [5,1250.0,dt.strptime("2024-01-02T02:20:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [6,1333.0,dt.strptime("2024-01-03T01:10:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
        [7,2750.0,dt.strptime("2024-02-09T03:30:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S")],
    ]
    return data


@fixture
def hierarchy_first_level_data() -> List[List[str]]:
    data: List[List[str]] = [
        [1,10.0,dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "EMEA"],
        [2,20.0,dt.strptime("2010-01-07T05:50:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "USA"]
    ]
    return data


@fixture
def hierarchy_second_level_data() -> List[List[str]]:
    data: List[List[str]] = [
        [3,50.0,dt.strptime("2010-03-10T06:40:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "NETHERLANDS"],
        [4,900.0,dt.strptime("2024-01-01T04:40:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "TURKEY"],
        [5,1250.0,dt.strptime("2024-01-02T02:20:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "FRANCE"],
        [6,1333.0,dt.strptime("2024-01-03T01:10:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "CALIFORNIA"],
    ]
    return data


@fixture
def hierarchy_third_level_data() -> List[List[str]]:
    data: List[List[str]] = [
        [7,2750.0,dt.strptime("2024-02-09T03:30:00", "%Y-%m-%dT%H:%M:%S"), dt.strptime("2010-01-01T07:30:00", "%Y-%m-%dT%H:%M:%S"), "LOS_ANGELES"]
    ]
    return data


@fixture
def csv_hierarchy_data() -> List[List[str]]:
    data: List[List[str]] = [
            [1,None,"EMEA"],
            [2,None,"USA"],
            [3,1,"NETHERLANDS"],
            [4,1,"TURKEY"],
            [5,1,"FRANCE"],
            [6,2,"CALIFORNIA"],
            [7,6,"LOS_ANGELES"]
    ]
    return data
     

@fixture
def csv_linear_df(
          spark_session: SparkSession, 
          csv_linear_schema: str, 
          csv_linear_data: List[List[str]]
          ) -> DataFrame:
    return spark_session.createDataFrame(csv_linear_data, csv_linear_schema)


@fixture
def csv_hierarchy_df(
          spark_session: SparkSession, 
          csv_hierarchy_schema: str, 
          csv_hierarchy_data: List[List[str]]
          ) -> DataFrame:
    return spark_session.createDataFrame(csv_hierarchy_data, csv_hierarchy_schema)


@fixture
def csv_hierarchy_first_level_df(
          spark_session: SparkSession, 
          csv_joined_schema: str, 
          hierarchy_first_level_data: List[List[str]]
          ) -> DataFrame:
    return spark_session.createDataFrame(hierarchy_first_level_data, csv_joined_schema)


@fixture
def csv_hierarchy_second_level_df(
          spark_session: SparkSession, 
          csv_joined_schema: str, 
          hierarchy_second_level_data: List[List[str]]
          ) -> DataFrame:
    return spark_session.createDataFrame(hierarchy_second_level_data, csv_joined_schema)


@fixture
def csv_hierarchy_third_level_df(
          spark_session: SparkSession, 
          csv_joined_schema: str, 
          hierarchy_third_level_data: List[List[str]]
          ) -> DataFrame:
    return spark_session.createDataFrame(hierarchy_third_level_data, csv_joined_schema)


@fixture
def hierarchy_join(
        app_configuration: AppConfiguration,
        spark_session: SparkSession
    ) -> HierarchyJoin:
    scheduler: Scheduler = Scheduler()
    return HierarchyJoin(app_configuration.task_config[1], spark_session, scheduler)
