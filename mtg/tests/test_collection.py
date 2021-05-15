import numpy as np
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal, assert_series_equal

from operator import and_
from functools import reduce

from mtg.collection import Collection
from pytest import raises


def test_collection_metadata(old_format_datafile):
    c = Collection(old_format_datafile, label="Islands", source="MtgManager")
    assert c.label == "Islands", f"Error in label: {c.label}"
    assert c.source == "MtgManager", f"Error in Source: {c.source}"


def test_old_format_layout(old_format_datafile):
    c = Collection(old_format_datafile)

    assert "Code" not in c.data.columns
    assert "ExpansionCode" in c.data.columns
    assert "ExpansionName" not in c.data.columns

    # Foil is Bool type
    assert c.data.Foil.dtype == bool
    # Condition is Categorical
    assert c.data.Condition.dtype.name == "category"
    # Language is Categorical
    assert c.data.Language.dtype.name == "category"

    # assert all ExpansionCode are lowercase
    lowercase_codes = c.data.ExpansionCode.apply(lambda c: c.lower()).values
    assert_array_equal(c.data.ExpansionCode.values, lowercase_codes)


def test_new_format_layout(new_format_datafile):
    c = Collection(new_format_datafile)

    assert "ExpansionCode" in c.data.columns
    assert "ExpansionName" in c.data.columns

    # Foil is Bool type
    assert c.data.Foil.dtype == bool
    # Condition is Categorical
    assert c.data.Condition.dtype.name == "category"
    # Language is Categorical
    assert c.data.Language.dtype.name == "category"

    # assert all ExpansionCode are lowercase
    lowercase_codes = c.data.ExpansionCode.apply(lambda c: c.lower()).values
    assert_array_equal(c.data.ExpansionCode.values, lowercase_codes)


def test_lazy_data_loading(test_collection):

    # No data property access yet
    assert test_collection._data is None
    # access data property - should not be None
    assert test_collection.data is not None
    assert_frame_equal(test_collection.data, test_collection._data)
    # they should be indeed the same object
    assert test_collection.data is test_collection._data


def test_len_collection_is_sum_of_quantities(test_collection):
    assert len(test_collection) > 0
    assert len(test_collection) != len(test_collection.data)
    assert len(test_collection) == test_collection.data.Quantity.sum()


def test_collection_is_hashable(test_collection):
    assert hash(test_collection) is not None
    assert hash(test_collection) == hash(test_collection.source + test_collection.label)


def test_proxy_dataframe(test_collection):
    """This tests verify that dataframe access is proxied via Collection"""

    try:
        # try to call the head method of the dataframe underneath
        _ = test_collection.head()
    except AttributeError:
        assert False, "Attribute head() not found in Collection"
    else:
        assert True

    try:
        # try access single column
        _ = test_collection.Name
    except AttributeError:
        assert False, "Attribute head() not found in Collection"
    else:
        assert True


def test_non_existing_name_raises_attribute_error(test_collection):
    with raises(AttributeError):
        test_collection.headhead()  # this does not exist
        test_collection.NonExistingColumn


def test_collection_is_subscriptable(test_collection):
    try:
        result = test_collection[test_collection.Name == "Island"]
    except TypeError:
        assert False, "Collection is Not Subscriptable"
    else:
        assert len(result) > 0
        res_via_data = test_collection.data[test_collection.data.Name == "Island"]
        assert_frame_equal(result, res_via_data)


def test_collection_is_writable(test_collection):
    try:
        test_collection[test_collection.Name == "Island", ["Name", "Quantity"]] = (
            "Forest",
            1,
        )
    except RuntimeError:
        assert False, "Assignment not permitted"
    else:
        # check that all values have been actually changed
        assert len(test_collection[test_collection.Quantity > 1]) == 0
        assert len(test_collection) == len(
            test_collection.data
        )  # every entry should count as for 1
        # check no island exists anymore
        assert len(test_collection[test_collection.Name == "Island"]) == 0


def test_add_new_column_to_collection(test_collection):

    n_cols_before_change = len(test_collection.columns)
    test_collection["NewColumn"] = test_collection.Quantity.apply(lambda v: v + 1)
    assert len(test_collection.columns) == n_cols_before_change + 1
    assert "NewColumn" in test_collection.columns

    qs = test_collection.Quantity.values.tolist()
    nqs = test_collection.NewColumn.values.tolist()
    assert reduce(and_, map(lambda p: p[0] > p[1] and p[0] == p[1] + 1, zip(nqs, qs)))


def test_diff_collections_nodiffs(new_format_datafile):
    c1 = Collection(new_format_datafile, label="Islands", source="Other NEW")
    c2 = Collection(new_format_datafile, label="Islands", source="NEW")

    assert len(c1 - c1) == 0
    assert len(c2 - c2) == 0

    assert len(c1 - c2) == 0
    assert len(c2 - c1) == 0

    assert "PurchaseDate" not in (c2 - c1).columns


def test_diff_collections_with_different_columns(
    old_format_datafile, new_format_datafile
):
    c1 = Collection(old_format_datafile, label="Islands")
    c2 = Collection(new_format_datafile, label="Islands")

    with raises(ValueError):
        c2 - c1


def test_diff_two_collections_different_label(old_format_datafile, new_format_datafile):
    c1 = Collection(old_format_datafile, label="Islands_old")
    c2 = Collection(new_format_datafile, label="Islands_new")

    with raises(ValueError):
        c2 - c1


def test_reorder_columns(new_format_datafile):
    c = Collection(new_format_datafile, label="Islands", source="Test New Format")
    cols = [
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
    assert all(map(lambda col: col in c.columns, cols))
    assert_array_equal(c.columns, np.asarray(cols))
    cols_reversed = cols[::-1]
    c.resort_colunns(cols_reversed)
    assert_array_equal(c.columns, np.asarray(cols_reversed))


def test_reorder_non_existing_columns(new_format_datafile):
    c = Collection(new_format_datafile, label="Islands", source="Test New Format")
    cols = [
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
    assert all(map(lambda col: col in c.columns, cols))
    assert_array_equal(c.columns, np.asarray(cols))
    with raises(ValueError):
        non_existing_cols = ["NonExistingCols"]
        c.resort_colunns(non_existing_cols)
