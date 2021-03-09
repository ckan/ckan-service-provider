# -*- coding: utf-8 -*-

from ckanserviceprovider.util import JobError

def test_joberror():
    err = JobError(u'oh no ❌')
    assert err.message == u'oh no ❌'
    assert str(err) == 'oh no ❌'
