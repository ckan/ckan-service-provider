# -*- coding: utf-8 -*-

from ckanserviceprovider.util import JobError


def test_joberror():
    err = JobError("oh no ❌")
    assert err.message == "oh no ❌"
    assert str(err) == "oh no ❌"
