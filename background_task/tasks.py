class Tasks(object):
    def __init__(self):
        self._tasks = {}
    
    def background(self, name=None):
        '''
        decorator to turn a regular function into
        something that gets run asynchronously in
        the background, at a later time
        '''
        def _decorator(fn):
            _name = name
            if not _name:
                _name = '%s.%s' % (fn.__module__, fn.__name__)
            proxy = TaskProxy(_name, fn)
            self._tasks[_name] = proxy
            return proxy
            
        return _decorator
    
    def run_task(self, task_name, args, kwargs):
        task = self._tasks[task_name]
        task.task_function(*args, **kwargs)

class TaskProxy(object):
    def __init__(self, name, task_function):
        self.name = name
        self.task_function = task_function
    
    def __call__(self, schedule=None, *arg, **kw):
        pass
    
    def __unicode__(self):
        return u'TaskProxy(%s)' % self.name

tasks = Tasks()