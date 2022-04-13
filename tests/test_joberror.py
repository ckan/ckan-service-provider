# -*- coding: utf-8 -*-

import sys

from ckanserviceprovider.util import JobError


def test_joberror():
    err = JobError("oh no ❌")
    assert err.message == "oh no ❌"
    if sys.version_info[0] == 2:
        assert unicode(err) == "oh no ❌"
    else:
        assert str(err) == "oh no ❌"
