from pytest import fixture
from pathlib import Path
from mtg import Collection, ScryfallDB
import os


BASE_TESTDATA_FOLDER = Path(os.path.abspath(os.path.dirname(__file__))) / "test_data"


@fixture(scope="session")
def old_format_datafile() -> Path:
    return BASE_TESTDATA_FOLDER / "old_format.csv"


@fixture(scope="session")
def new_format_datafile() -> Path:
    return BASE_TESTDATA_FOLDER / "new_format.csv"


@fixture(scope="function")  # re-newed every test
def test_collection(old_format_datafile) -> Collection:
    return Collection(old_format_datafile)


@fixture(scope="session")
def scryfall_oracle() -> ScryfallDB:
    return ScryfallDB()
