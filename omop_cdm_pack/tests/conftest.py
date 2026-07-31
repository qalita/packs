import pytest

from omop_dqd.context import CdmContext
from tests.fixtures import write_mini_cdm


@pytest.fixture(scope="session")
def mini_cdm(tmp_path_factory):
    directory = tmp_path_factory.mktemp("mini_cdm")
    return CdmContext.from_paths(write_mini_cdm(str(directory)))


@pytest.fixture(scope="session")
def mini_cdm_no_vocabulary(tmp_path_factory):
    directory = tmp_path_factory.mktemp("mini_cdm_no_vocab")
    return CdmContext.from_paths(
        write_mini_cdm(str(directory), include_vocabulary=False)
    )
