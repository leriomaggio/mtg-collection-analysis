import json
import requests
import sys
from typing import Tuple, Iterable, Sequence, Dict, Optional
from functools import reduce, partial
from operator import and_, is_
from collections import defaultdict, namedtuple
from . import DATA_FOLDER
from tqdm import tqdm

SCRYFALL_DEFAULT_CARDS_URL = "https://c2.scryfall.com/file/scryfall-bulk/default-cards/default-cards-20210508090304.json"
SCRYFALL_DATA_FOLDER = DATA_FOLDER / "scryfall"
SCRYFALL_DB = SCRYFALL_DATA_FOLDER / "default_cards.json"


def download_and_save(dbfile=SCRYFALL_DB):
    print("Downloading DB file (Default Cards) from Scryfall")
    # Streaming, so we can iterate over the response.
    response = requests.get(SCRYFALL_DEFAULT_CARDS_URL, stream=True)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 4096  # 4 Kibibyte
    progress_bar = tqdm(
        total=total_size_in_bytes, unit="iB", unit_scale=True, file=sys.stdout
    )
    with open(dbfile, "wb") as scryfall_db:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            scryfall_db.write(data)
    progress_bar.close()
    print(f"DB file saved in {dbfile}!")


def load_scryfall_dbjson(dbfile=SCRYFALL_DB):
    def json_hook(obj, pbar):
        pbar.update(1)
        return obj

    if not dbfile.exists():
        print("DB file does not exist!")
        download_and_save(dbfile=dbfile)
    # For Performance Issues, load (BIG ~= 1.20GB) JSON DB from Scryfall just once
    with open(dbfile) as scryfalldb_file:
        print("Loading Full Database file")
        pbar = tqdm(ascii=True, desc="Cards", unit="", file=sys.stdout)
        hook = partial(json_hook, pbar=pbar)
        scryfall_db = json.load(scryfalldb_file, object_hook=hook)
        pbar.close()
    return scryfall_db


Card = namedtuple("Card", ["name", "lang", "set_code", "set_name", "set_type"])


class ScryfallDB:
    """Cards ScryfallDB"""

    def __init__(self, dbfile=SCRYFALL_DB, db_preloaded=None):
        if not db_preloaded:
            self._db_file = dbfile
            self._db = load_scryfall_dbjson(dbfile=dbfile)
        else:
            self._db_file = None
            self._db = db_preloaded

        self._cards_map = defaultdict(list)
        print("\nLoading Cards from Database into Oracle")
        self._load_cards_from_db()
        self._expansion_codename_map = self._load_codename_map()

    def _load_cards_from_db(self):
        for entry in self._db:
            if entry["lang"] != "en":
                continue
            if (
                entry["games"]
                and len(entry["games"]) == 1
                and entry["games"][0] == "mtgo"
            ):
                continue  # skip Online-only Expansion Promo Sets
            c = Card(
                name=entry["name"],
                lang=entry["lang"],
                set_code=entry["set"],
                set_name=entry["set_name"],
                set_type=entry["set_type"],
            )
            dbentry = self.make_dbentry(c.name)
            self._cards_map[dbentry].append(c)

    @staticmethod
    def make_dbentry(name: str) -> str:
        return name.lower().replace(" ", "-")

    def lookup(
        self,
        card_name: Optional[str] = None,
        expand_search: bool = False,
        doubles_only: bool = False,
        unique: bool = False,
        set_code: str = None,
        set_type: str = None,
    ) -> Iterable[Card]:
        """Lookup for Cards in the DB with the specified name.

        Parameters
        ----------
        card_name: str (default None)
            Name of the card to look up (case-insensitive)
            If No Name is provided, a lookup by set_code and/or set_type will be enabled (if any).
        expand_search: bool (default False)
            If True, the search for card_name will be extended also to non-exact matches.
            Some options are available:
            1. SUFFIX-search (e.g. *ice)
            2. PREFIX-search (e.g. ice*)
            3. SUFFIX and PREFIX search (joint results, e.g. *ice*)
            4. WHOLE-WORD search: only cards in the db containing the whole words in `card_name` are returned
                (e.g. "spring")
            Note: Passing in just "*" as card_name will be treated as emptry string (i.e. NO card name)
            Full DB lookup is pointless, just iterating over the Oracle would be good for that.
        doubles_only: bool (default False)
            If True, expanded search will be only limited to double cards (e.g. Fire // Ice)
            NOTE: This option will be ignored, if expanded_search is False!
        unique: bool (default False)
            If True, only one entry per Card will be returned (usually used with expanded search).
            NOTE: If no set_code nor set_type is specified, the first entry for each retrieved card (if any)
            is returned.
        set_code: str (default None)
            If speficied, cards' version from the speficied `set_code` will be returned.
            If the search won't produce any result, the set_code will be automatically tried
            as a prefix, before giving up.
        set_type: str, allowed: ("expansion", "promo", "memorabilia", "premium_deck, "funny") (default None)
            If speficied, the list of results will be further filtered by returning only sets of the
            specified type.
        Return
        ------
            (Lazy) Iterable sequence of retrieved Card instances matching the speficied criteria.
            Empty result set will be returned if no match is found in the DB.
        """
        allowed_set_types = (
            "promo",
            "expansion",
            "memorabilia",
            "premium_deck",
            "funny",
        )
        if set_type and not set_type in allowed_set_types:
            return ValueError(
                f"Input set type {set_type} Not Recognised. Values allowed are {allowed_set_types}"
            )

        is_card = (card_name is not None) and len(card_name.replace("*", ""))
        is_set = set_code is not None
        is_set_type = set_type is not None
        if not any((is_card, is_set, is_set_type)):
            return self._result_set([])

        if is_card:
            card_dbentry = self.make_dbentry(card_name)
            entries = self._cards_map.get(card_dbentry, [])

            if not entries and not expand_search:
                return self._result_set([])  # Empty result

            if expand_search:
                # set cards pool of reference
                if doubles_only:
                    cards_pool = filter(lambda c: "//" in c.name, self.all_cards)
                else:
                    cards_pool = self.all_cards
                # set filter function
                if card_name.startswith("*") and card_name.endswith("*"):
                    card_name = card_name[1:-1].lower()
                    f = lambda c: c.name.lower().startswith(
                        card_name
                    ) or c.name.lower().endswith(card_name)
                elif card_name.startswith("*"):  # SUFFIX
                    card_name = card_name[1:].lower()
                    f = lambda c: c.name.lower().endswith(card_name)
                elif card_name.endswith("*"):  # PREFIX
                    card_name = card_name[:-1].lower()
                    f = lambda c: c.name.lower().startswith(card_name)
                else:  # WHOLE-WORD lookup
                    f = lambda c: self._isin(card_name, c.name)
                expanded_search = filter(f, cards_pool)
                entries.extend(expanded_search)
            entries = tuple(entries)  # make an immutable sequence
        else:
            entries = self.all_cards

        # Lookup by set_code
        if is_set:
            if set_code in self._expansion_codename_map:
                filter_func = lambda c: c.set_code == set_code
            else:
                filter_func = lambda c: c.set_code.startswith(set_code)
            entries_set = tuple(filter(filter_func, entries))
            if not entries_set:
                return self._result_set(())  # Empty result
            entries = entries_set
        # Lookup by set_type
        if is_set_type:
            entries = tuple(filter(lambda c: c.set_type == set_type, entries))
        # filter unique values
        if unique:
            entries_map = dict()
            for e in entries:
                _ = entries_map.setdefault(e.name, e)
            entries = tuple(c for c in entries_map.values())
        return self._result_set(entries)

    @staticmethod
    def _result_set(entries: Sequence[Card]) -> Iterable[Card]:
        if not entries:
            return iter(tuple())  # empty set
        else:
            for e in entries:
                yield e

    @staticmethod
    def _isin(card_name: str, db_entry: str) -> bool:
        db_entry_words = db_entry.lower().split()
        return reduce(
            and_, map(lambda n: n in db_entry_words, card_name.lower().split())
        )

    def __len__(self) -> int:
        return sum(map(len, self._cards_map.values()))

    def __contains__(self, card_name: str) -> bool:
        return len(tuple(self.lookup(card_name))) > 0

    def __getitem__(self, card_name: str) -> Tuple[Card]:
        """proxy for lookup with just the card name specified"""
        return tuple(self.lookup(card_name))

    def __iter__(self) -> Iterable[Card]:
        return self.all_cards

    def _load_codename_map(self) -> Dict[str, str]:
        codename_seq = set()
        for entry in self:
            set_code, set_name = entry.set_code, entry.set_name
            codename_seq.add((set_code, set_name))
        return {code: name for code, name in codename_seq}

    @property
    def expansion_codename_map(self) -> Dict[str, str]:
        """returns a dictionary mapping expansion codes to their corresponding full names"""
        if not self._expansion_codename_map:
            self._expansion_codename_map = self._load_codename_map()
        return self._expansion_codename_map

    @property
    def all_cards(self) -> Iterable[Card]:
        for cards in self._cards_map.values():
            for card in cards:
                yield card

    def add_expansion_code(self, codename: Tuple[str, str]) -> None:
        """Method to add any Code-Name expansion set found in the MTG-Manager data
        that is  "missing" from Scryfall"""
        set_code, set_name = codename
        self.expansion_codename_map[set_code] = set_name
