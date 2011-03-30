VERSION = (0, 1, 2)
__version__ = '.'.join(map(str, VERSION))

def background(*arg, **kw):
    from tasks import tasks
    return tasks.background(*arg, **kw)
