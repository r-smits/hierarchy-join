from typing import List, Tuple
from dataclasses import dataclass
from dataclasses_json import LetterCase, Undefined, dataclass_json


@dataclass_json(letter_case=LetterCase.SNAKE, undefined=Undefined.EXCLUDE)
@dataclass(frozen=True)
class Property:
    key: str
    value: str

    def __str__(self) -> str:
        return f"{self.key} {self.value}"

    def to_tuple(self) -> Tuple[str, str]:
        return (self.key, self.value)


@dataclass_json(letter_case=LetterCase.SNAKE, undefined=Undefined.EXCLUDE)
@dataclass(frozen=False)
class DataFrameConfig:
    """
    Configuration that applies to a given spark dataframe
    """
    name: str
    format: str
    type: str
    location: str
    schema: List[Property]


@dataclass_json(letter_case=LetterCase.SNAKE, undefined=Undefined.EXCLUDE)
@dataclass(frozen=False)
class TaskConfig:
    """
    Task to be executed
    """
    name: str
    dataframe_config: List[DataFrameConfig]
    save_location: str
    from_date: str
    to_date: str
    partition: str
    scheduler: bool
    frequency: str


@dataclass_json(letter_case=LetterCase.SNAKE, undefined=Undefined.EXCLUDE)
@dataclass(frozen=False)
class AppConfiguration:
    """
    General configuration for pipeline app
    """
    spark_config: List[Property]
    task_config: List[TaskConfig]
    