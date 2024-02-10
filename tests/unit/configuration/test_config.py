from src.configuration import AppConfiguration
import pytest


def test_init_app_configuration_sucess(config_dict: dict) -> None:
    assert AppConfiguration.from_dict(config_dict)


def test_init_app_configuration_fail(config_dict: dict) -> None:
    config_dict['task_config'][0].pop("name")
    with pytest.raises(KeyError):
        AppConfiguration.from_dict(config_dict)
