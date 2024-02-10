from pyspark.sql import DataFrame
from src.job.hierarchy_join import HierarchyJoin
from typing import List, Generator


def test_hierarchy_validate_config(hierarchy_join: HierarchyJoin) -> None:
    hierarchy_join._validate_config()


def test_hierarchy_tree_traversal(
        hierarchy_join: HierarchyJoin,
        csv_linear_df: DataFrame, 
        csv_hierarchy_df: DataFrame,
        csv_hierarchy_first_level_df: DataFrame,
        csv_hierarchy_second_level_df: DataFrame,
        csv_hierarchy_third_level_df: DataFrame
    ) -> None:
    dfs: List[DataFrame] = [csv_linear_df, csv_hierarchy_df]
    levels: List[DataFrame] = [
        csv_hierarchy_first_level_df, 
        csv_hierarchy_second_level_df, 
        csv_hierarchy_third_level_df
    ]
    gen: Generator = hierarchy_join.transform(dfs)
    for index, df in enumerate(gen):
        assert df.subtract(levels[index]).isEmpty()
