from mtg import ScryfallDB
from pytest import raises


def test_lookup_case_insensitive(scryfall_oracle: ScryfallDB):
    hammer_res = scryfall_oracle.lookup("HAMMEr of BOGardAn")
    hammer_res = tuple(hammer_res)
    assert len(hammer_res) > 0
    assert hammer_res == tuple(scryfall_oracle.lookup("Hammer of bogardan"))


def test_result_with_set_code(scryfall_oracle: ScryfallDB):
    hammer_res = scryfall_oracle.lookup("HAMMEr of BOGardAn", set_code="mir")
    hammer_res = tuple(hammer_res)
    assert len(hammer_res) > 0
    assert len(hammer_res) == 1


def test_empty_result_with_no_name(scryfall_oracle: ScryfallDB):
    empty_set = scryfall_oracle.lookup("")
    assert len(tuple(empty_set)) == 0

    empty_set = scryfall_oracle.lookup("*")
    assert len(tuple(empty_set)) == 0


def test_result_set_code_as_prefix(scryfall_oracle: ScryfallDB):
    hammer_res = scryfall_oracle.lookup("HAMMEr of BOGardAn", set_code="w")
    hammer_res = tuple(hammer_res)
    assert len(hammer_res) > 0
    assert len(hammer_res) == 3


def test_set_type_restriction(scryfall_oracle: ScryfallDB):
    hammer_res = scryfall_oracle.lookup("HAMMEr of BOGardAn", set_type="memorabilia")
    hammer_res = tuple(hammer_res)
    assert len(hammer_res) == 3
    assert len(list(filter(lambda c: c.set_type == "memorabilia", hammer_res))) == len(
        hammer_res
    )


def test_lookup_from_a_single_set_code(scryfall_oracle: ScryfallDB):
    all_mirage_cards = tuple(scryfall_oracle.lookup(set_code="mir"))
    assert len(all_mirage_cards) > 0
    assert all(map(lambda c: c.set_code == "mir", all_mirage_cards))


def test_lookup_for_a_single_set_type(scryfall_oracle: ScryfallDB):
    all_promo_cards = tuple(scryfall_oracle.lookup(set_type="promo"))
    assert len(all_promo_cards) > 0
    assert all(map(lambda c: c.set_type == "promo", all_promo_cards))


def test_set_type_restriction_and_unique(scryfall_oracle: ScryfallDB):
    hammer_res = scryfall_oracle.lookup(
        "HAMMEr of BOGardAn", set_type="memorabilia", unique=True
    )
    hammer_res = tuple(hammer_res)
    assert len(hammer_res) == 1


def test_expanded_search_prefix(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup("spring*", expand_search=True)
    res = tuple(res)
    assert len(res) == 21
    assert len(list(filter(lambda c: c.name.lower().startswith("spring"), res))) == len(
        res
    )


def test_expanded_search_suffix(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup("*spring", expand_search=True)
    res = tuple(res)
    assert len(res) == 43
    assert len(list(filter(lambda c: c.name.lower().endswith("spring"), res))) == len(
        res
    )


def test_expanded_search_prefix_suffix(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup("*spring*", expand_search=True)
    res = tuple(res)
    assert len(res) == 64, f"Not 64 items, {len(res)}"
    assert (
        len(
            list(
                filter(
                    lambda c: c.name.lower().endswith("spring")
                    or c.name.lower().startswith("spring"),
                    res,
                )
            )
        )
        == len(res)
    )


def test_expanded_search_case_insensitive(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup("spring", expand_search=True)
    res = tuple(res)
    assert len(res) > 0
    assert len(list(filter(lambda c: "spring" in c.name.lower(), res))) == len(res)


def test_search_for_doubles(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup(
        "ice", expand_search=True, doubles_only=True, unique=True
    )
    res = tuple(res)
    assert len(res) == 4
    assert len(list(filter(lambda c: "ice" in c.name.lower(), res))) == len(res)

    res = scryfall_oracle.lookup(
        "Akki Lavarunner", expand_search=True, doubles_only=True, unique=True
    )
    res = tuple(res)
    assert len(res) == 1
    assert res[0].name == "Akki Lavarunner // Tok-Tok, Volcano Born"


def test_search_for_doubles_with_prefix(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup(
        "ice*", expand_search=True, doubles_only=True, unique=True
    )
    res = tuple(res)
    assert len(res) == 2
    assert len(list(filter(lambda c: c.name.lower().startswith("ice"), res))) == len(
        res
    )


def test_search_for_doubles_with_suffix(scryfall_oracle: ScryfallDB):
    res = scryfall_oracle.lookup(
        "*ice", expand_search=True, doubles_only=True, unique=True
    )
    res = tuple(res)
    assert len(res) == 4
    assert len(list(filter(lambda c: c.name.lower().endswith("ice"), res))) == len(res)


def test_getitem_from_oracle(scryfall_oracle: ScryfallDB):
    cards = scryfall_oracle["Fireball"]
    assert isinstance(cards, tuple)
    assert len(cards) > 0
    assert all(map(lambda c: c.name == "Fireball", cards))


def test_contains_lookup_in_oracle(scryfall_oracle: ScryfallDB):
    assert "Fireball" in scryfall_oracle
    assert "non-existing-card" not in scryfall_oracle
