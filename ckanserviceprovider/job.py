from . import web


def asynchronous(func):
    web.async_types[func.__name__] = func
    return func


def synchronous(func):
    web.sync_types[func.__name__] = func
    return func
