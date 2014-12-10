"""Unit tests for ckanserviceprovider/db.py."""
import nose.tools

import ckanserviceprovider.db as db


def test_validate_error_with_none():
    """_validate_error() should return None if given None."""
    assert db._validate_error(None) is None


def test_validate_error_with_string():
    """If given a string _validate_error() should return it wrapped in a dict.

    """
    assert db._validate_error("Something went wrong") == {
        "message": "Something went wrong"}


def test_validate_error_with_valid_dict():
    """If given a valid dict _validate_error() should return the same dict."""
    job_dict = {"message": "Something went wrong"}
    assert db._validate_error(job_dict) == job_dict


def test_validate_error_with_dict_with_invalid_error():
    """_validate_error() should raise if given a dict with an invalid message.

    """
    job_dict = {"message": 42}  # Error message is invalid: it's not a string.
    nose.tools.assert_raises(
        db.InvalidErrorObjectError, db._validate_error, job_dict)


def test_validate_error_with_dict_with_no_error_key():
    """_validate_error() should raise if given a dict with no "message" key."""
    job_dict = {"foo": "bar"}
    nose.tools.assert_raises(
        db.InvalidErrorObjectError, db._validate_error, job_dict)


def test_validate_error_with_random_object():
    """_validate_error() should raise if given an object of the wrong type."""

    class Foo(object):
        pass

    # An error object that is not None and is not string- or dict-like at all.
    error_obj = Foo()

    nose.tools.assert_raises(
        db.InvalidErrorObjectError, db._validate_error, error_obj)
