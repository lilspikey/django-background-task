from models import Task

import os
from datetime import datetime, timedelta
from django.db import transaction


class Tasks(object):
    def __init__(self):
        self._tasks = {}
        self._runner = DBTaskRunner()
    
    def background(self, name=None, schedule=None):
        '''
        decorator to turn a regular function into
        something that gets run asynchronously in
        the background, at a later time
        '''
        def _decorator(fn):
            _name = name
            if not _name:
                _name = '%s.%s' % (fn.__module__, fn.__name__)
            proxy = TaskProxy(_name, fn, schedule, self._runner)
            self._tasks[_name] = proxy
            return proxy
            
        return _decorator
    
    def run_task(self, task_name, args, kwargs):
        task = self._tasks[task_name]
        task.task_function(*args, **kwargs)
    
    def run_next_task(self):
        return self._runner.run_next_task(self)

class DBTaskRunner(object):
    
    def __init__(self):
        self.worker_name = str(os.getpid())
    
    '''
    Encapsulate the model related logic in here, in case
    we want to support different queues in the future
    '''
    def schedule(self, task_name, args, kwargs, run_at=None, priority=0):
        '''Simply create a task object in the database'''
        task = Task.objects.create_task(task_name, args, kwargs, run_at, priority)
    
    @transaction.autocommit
    def get_task_to_run(self):
        tasks = Task.objects.find_available()[:5]
        for task in tasks:
            # try to lock task
            locked_task = task.lock(self.worker_name)
            if locked_task:
                return locked_task
        return None
    
    @transaction.autocommit
    def run_task(self, tasks, task):
        try:
            args, kwargs = task.params()
            tasks.run_task(task.task_name, args, kwargs)
            # task done, so can delete it
            task.delete()
        except Exception, e:
            task.reschedule(e)
    
    def run_next_task(self, tasks):
        task = self.get_task_to_run()
        if task:
            self.run_task(tasks, task)
            return True
        else:
            return False

class TaskProxy(object):
    def __init__(self, name, task_function, schedule, runner):
        self.name = name
        self.task_function = task_function
        self.runner = runner
        self.schedule = schedule
    
    def _run_at_from_schedule(self, schedule):
        # schedule may either be dictionary possinly containing a run_at
        # value, or if not then it will be the value of run_at itself.
        # the run at value can either be a datetime object of when to run
        # the task or a timedelta which will be added to datetime.now()
        # to create a datetime or else an int, which will be treated as
        # seconds and added to datetime.now() to also create a datetime
        run_at = None
        if schedule:
            if isinstance(schedule, (int, timedelta, datetime)):
                run_at = schedule
            else:
                run_at = schedule.get('run_at', None)
            
            if run_at:
                if isinstance(run_at, int):
                    run_at = datetime.now() + timedelta(seconds=run_at)
                if isinstance(run_at, timedelta):
                    run_at = datetime.now() + run_at
            else:
                run_at = None
        return run_at
    
    def _priority_from_schedule(self, schedule):
        default = 0
        if schedule:
            try:
                return int(schedule.get('priority', default))
            except AttributeError:
                pass
        return default
    
    def __call__(self, *args, **kwargs):
        schedule = kwargs.pop('schedule', self.schedule)
        run_at = self._run_at_from_schedule(schedule)
        priority = self._priority_from_schedule(schedule)
        self.runner.schedule(self.name, args, kwargs, run_at, priority)
    
    def __unicode__(self):
        return u'TaskProxy(%s)' % self.name

tasks = Tasks()