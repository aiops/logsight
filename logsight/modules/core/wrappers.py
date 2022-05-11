import threading


def synchronized(func):
    """Makes functions thread-safe"""
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)
    return synced_func


def synchronized_lock(fun, lock):
    def synced_func(*args, **kwargs):
        with lock:
            return fun(*args, **kwargs)
    return synced_func


class Synchronized:
    def __init_subclass__(cls, **kwargs):
        __lock__ = threading.Lock()
        for name in cls.__dict__:
            attr = getattr(cls, name)
            if callable(attr):
                setattr(cls, name, synchronized_lock(attr, __lock__))
