import os
from pathlib import Path
import pandas as pd
from typing import Any, Sequence, Tuple, Union, Optional, List

# The Key to the Get Access to the DataFrame, i.e. string | Series
GetKey = Union[str, pd.Series]

# The Key to the Set Access to the DataFrame, i.e. input to .loc
# ==> a tuple of "GetKey" for (subset) selection,
#     and the (optional) [list of] column[s] to change
LocKey = Tuple[GetKey, Optional[Union[str, Sequence[str]]]]


class Collection:
    """
    Abstraction to represent a generalised collection of cards.

    Attributes
    ----------
    label: str
        A reference lable assigned to the collection.
        If not provided, this will correspond to the name of the input data file.
    source: str
        A reference to the original source.
        If not provided, this will correspond to the name of the folder
        in which data file is contained.
    filepath: Path
        Full filepath to reference data file
    data: pandas.DataFrame
        The actual collection data
    """

    EIGHT_COLS_LAYOUT = [
        "Quantity",
        "Name",
        "ExpansionCode",
        "PurchasePrice",
        "Foil",
        "Condition",
        "Language",
        "PurchaseDate",
    ]

    NINE_COLS_LAYOUT = [
        "Quantity",
        "Name",
        "ExpansionCode",
        "ExpansionName",
        "PurchasePrice",
        "Foil",
        "Condition",
        "Language",
        "PurchaseDate",
    ]

    def __init__(
        self, filepath: Union[Path, str], label: str = None, source: str = None
    ):
        self.filepath = Path(filepath)
        if not label:
            label = self._get_label(self.filepath)
        if not source:
            source = self._get_label(self.filepath.parent)
        self._data = None
        self.label = label
        self.source = source

    @staticmethod
    def _get_label(filepath: Path) -> str:
        l = filepath.stem
        l = l.strip().replace("_", " ").replace("-", " ")
        return l

    def _read_mtg_data(self) -> pd.DataFrame:
        """Function to read data from CSV files into Pandas DataFrame.
        The function will include several operation to 'normalise' the
        dataframe format between the old and the new data."""
        df = pd.read_csv(
            self.filepath,
            header=0,
            parse_dates=[
                "PurchaseDate",
            ],
            quotechar='"',
        )
        # first-off check whether data is still in old_format
        # This is to be verified before any remapping to columns
        is_old_format = "Code" in df.columns
        if is_old_format:  # OLD MTGManager format
            self._fix_oldformat(df)

        # Map Foil as Boolean
        df["Foil"] = df.Foil.map(bool)
        # Conditional: Ordinal Type
        # Poor < Played < LightPlayed < Good, < Excellent < Near Mint < Mint Mint
        df["Condition"] = pd.Categorical(
            df.Condition.values,
            ordered=True,
            categories=[
                "Poor",
                "Played",
                "LightPlayed",
                "Good",
                "Excellent",
                "NearMint",
                "Mint",
            ],
        )
        # Language: Nominal Type
        df["Language"] = pd.Categorical(df.Language.values)
        # re-map columns
        if len(df.columns) == 8:  # No ExpansionName or "Expansion Name" in cols
            cols_layout = self.EIGHT_COLS_LAYOUT
        else:
            cols_layout = self.NINE_COLS_LAYOUT
        df.columns = cols_layout

        return df

    def _fix_oldformat(self, df: pd.DataFrame):
        """Quick fix to Old-format data"""

        # 1. transform all expansion code in lower-case
        df["Code"] = df.Code.apply(lambda c: c.lower())
        # 2. Re-map Condition to the new categorical scale
        df["Condition"] = df.Condition.map(
            {0: "NearMint", 1: "Excellent", 2: "Good", 3: "Played", 4: "Poor"}
        )
        # 3. remap languages to match the new labels
        df["Language"] = df.Language.map(
            {
                0: "English",
                1: "German",
                2: "Portuguese",
                3: "French",
                4: "Italian",
                5: "Spanish",
                6: "Japanese",
                7: "Simplified Chinese",
                8: "Russian",
                9: "Traditional Chinese",
                10: "Korean",
            }
        )

    @property
    def data(self) -> pd.DataFrame:
        if self._data is None:
            self._data = self._read_mtg_data()
        return self._data

    @property
    def name(self) -> str:
        return f"{self.source}/{self.label}"

    def __getattr__(self, name):
        try:
            attr = self.data.__getattr__(name)
        except AttributeError:
            raise AttributeError(f"Collection object has no attribute {name}")
        else:
            return attr

    def __hash__(self) -> int:
        return hash(self.source + self.label)

    def __getitem__(self, key: GetKey) -> Union[pd.Series, pd.DataFrame]:
        """Shortcut proxy to internal (DataFrame) data
        for a quick access to data in the collection"""
        return self.data[key]

    def __setitem__(self, key: LocKey, value: Any) -> None:
        """Shortcut proxy to change values in the internal DB
        (DataFrame)."""
        # New Column
        if isinstance(key, str) and isinstance(value, pd.Series):
            self.data[key] = value
        # Loc-based Change
        if isinstance(key, tuple):
            self.data.loc[key] = value

    def __len__(self) -> int:
        return self.data.Quantity.values.sum()

    def diff(self, collection: "Collection") -> pd.DataFrame:

        """Calculates the difference, i.e. the DIFF between two collections
        having the same Label."""

        if collection.label != self.label:
            raise ValueError("The compared collection has different name!")

        if set(self.data.columns) != set(collection.data.columns):
            raise ValueError("The compared collection has different columns!")

        left, right = self.data.copy(), collection.data.copy()
        # Exclude PurchaseDate from final cols layout
        if len(self.data.columns) == 8:
            cols_layout = self.EIGHT_COLS_LAYOUT[:-1]
        else:
            cols_layout = self.NINE_COLS_LAYOUT[:-1]
        # get rid of Purchase Date due to BUG in date parsing from old to new app
        left, right = left[cols_layout], right[cols_layout]

        # lowercase all card names
        left["Name"] = left.Name.apply(lambda n: n.lower())
        right["Name"] = right.Name.apply(lambda n: n.lower())
        return left[~left.apply(tuple, 1).isin(right.apply(tuple, 1))]

    def __sub__(self, collection: "Collection") -> pd.DataFrame:
        return self.diff(collection)

    def save(
        self, target_folder: Optional[Union[str, Path]] = None, verbose: bool = True
    ) -> None:
        """Saves current collection as a CSV file.

        Parameters
        ----------
        target_folder: Path or str (default None)
            If provided, it represents the path to the folder where the CSV file will be saved.
            If a relative path is provided, it will be interpreted as the name of a
            (new?) folder relative to current Collection path.
            If NOT provided, the same file will be overwritten.
        verbose: bool (default True)
            If True, the path of the new saved files will be displayed.
        """
        if not target_folder:
            # overwrite current file
            filepath = self.filepath
            relative = ""
        else:
            target_folder = Path(target_folder)
            if not target_folder.is_absolute():
                relative = self.filepath.parent
                target_folder = relative / target_folder
            else:
                relative = ""

            os.makedirs(target_folder, exist_ok=True)
            filepath = Path(target_folder) / self.filepath.name
        self.data.to_csv(filepath, index=False, quotechar='"')
        if verbose:
            fp = filepath.relative_to(relative) if relative else self.name
            print(f"{self.name} saved in {fp}")

    def resort_colunns(self, cols_list: List[str]) -> None:
        """
        Resort the columns of internal data according to the list
        of columns passed in input.

        Parameters
        ----------
        cols_list : List[str]
            List of columns to reassign.

        Raises
        ------
        ValueError if any column in input is not present in data.
        """

        if any(map(lambda c: c not in self.data.columns, cols_list)):
            raise ValueError(
                "Input columns to reorder must belong to the original DataFrame"
            )
        self._data = self._data[cols_list]
