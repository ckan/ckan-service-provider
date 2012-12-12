import web


def async(func):
    web.async_types[func.__name__] = func
    return func


def sync(func):
    web.sync_types[func.__name__] = func
    return func
