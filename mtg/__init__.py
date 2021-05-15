import os
from pathlib import Path

BASE_FOLDER = Path(os.path.abspath(os.path.dirname(__file__)))
DATA_FOLDER = BASE_FOLDER.parent / "data"

from .collection import Collection
from .scryfall import ScryfallDB


__all__ = ["ScryfallDB", "Collection", "DATA_FOLDER"]
